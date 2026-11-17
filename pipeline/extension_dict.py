"""提出者拡張タクソノミの表示名の要素辞書 (P2-T10、仕様 #15 §10)。

日次解析 (pipeline.parse) が月別に出力する
parse/month=YYYY-MM/extension_labels.jsonl (書類ごとの拡張要素と
表示名の対応) を入力として、表示名の要素辞書を独立した成果物として
構築する。Silver の作り直しはしない (仕様の定め)。辞書は v2
データセット (Yoshi-Dai/financial-lakehouse-v2) の dictionary/ 配下に
置く。

辞書の形:
- 1行 = 1観測 (doc_id, element, element_ns)。書類ごとの対応を
  そのまま保持し、集約で出所を失わない。
- element は Silver の element 列と同じ文字列 (接頭辞:局所名) で、
  Silver とは (doc_id, element) で結合できる。
- 出所として、書類目録 (catalog/month=YYYY-MM/ledger.jsonl) から
  提出者 (EDINET コード・提出者名・証券コード・ファンドコード)、
  書類種別、提出日時、源泉の種別・識別子・取得日時・再配布可否を
  併記する。目録に無い書類は出所の欄を空にし、サマリに数える
  (行自体は落とさない。表示名の観測は目録の有無に依存しない)。
- 同一キー (doc_id, element, element_ns) の重複は除く。同じキーで
  表示名が食い違う場合は決定的に (表示名の辞書順で最小の組を) 採り、
  食い違いをサマリの key_conflicts に記録する。要素名が同じで
  書類をまたいで表示名が食い違う場合も、行は書類ごとに残したうえで
  サマリの label_conflicts に記録する (どちらが正かは辞書側では
  決めない)。

過去分 (バックフィル済み Silver の期間) には extension_labels が
無い。その期間の表示名は tar / zip から別途抽出した
extension_labels 形式の JSONL を同じ build に渡すことで取り込める
(抽出自体は pipeline.parse の extract_facts が担う既存経路であり、
本モジュールの範囲は「JSONL から辞書を構築する」まで)。

冪等性: 辞書は入力 JSONL 全部からの再構築であり、同じ入力からは
同じ行集合 (ソート済み) が得られる。sync は対象月の入力をすべて
落としてから再構築して置き換えるため、再実行で重複しない。

認証は環境変数 HF_TOKEN (HF への書き込み、huggingface_hub が自動で
読む) を使う。読み取りは公開データセットなのでトークンなしでも動く。
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .hf_sync import _with_retry

DEFAULT_REPO = "Yoshi-Dai/financial-lakehouse-v2"
DICT_PATH_IN_REPO = "dictionary/extension_elements.parquet"
SUMMARY_PATH_IN_REPO = "dictionary/extension_elements_summary.json"

DICT_SCHEMA = pa.schema([
    ("element", pa.string()),
    ("element_ns", pa.string()),
    ("element_local", pa.string()),
    ("label_ja", pa.string()),
    ("label_en", pa.string()),
    ("doc_id", pa.string()),
    ("edinet_code", pa.string()),
    ("filer_name", pa.string()),
    ("sec_code", pa.string()),
    ("fund_code", pa.string()),
    ("doc_type", pa.string()),
    ("submit_at", pa.string()),
    ("source", pa.string()),
    ("source_id", pa.string()),
    ("fetched_at", pa.string()),
    ("redistribution", pa.string()),
])

_LABEL_FIELDS = ("doc_id", "element", "element_ns")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_jsonl(path):
    """JSONL を読む。戻り値は (行のリスト, 壊れた行数)。

    壊れた行は数えて返す (黙って捨てない)。呼び出し側がサマリに
    残すことで、入力の欠けが照合で見えるようにする。
    """
    rows = []
    corrupt = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except ValueError:
                corrupt += 1
    return rows, corrupt


def catalog_index(catalog_rows):
    """書類目録の行から docID -> 行 の索引を作る。

    目録は同じ書類が取得のたびに重複して載る (2026-11 の実物では
    1書類あたり3行、fetched_at 以外は同一)。batch.Ledger と同じく
    最新の行をその書類の状態とし、重複行数を返す (呼び出し側が
    サマリに残す)。
    """
    index = {}
    duplicates = 0
    for row in catalog_rows:
        doc_id = row.get("docID")
        if not doc_id:
            continue
        if doc_id in index:
            duplicates += 1
        index[doc_id] = row
    return index, duplicates


def _provenance(catalog_row):
    if catalog_row is None:
        return {name: None for name in (
            "edinet_code", "filer_name", "sec_code", "fund_code",
            "doc_type", "submit_at", "source", "source_id",
            "fetched_at", "redistribution")}
    return {
        "edinet_code": catalog_row.get("edinetCode"),
        "filer_name": catalog_row.get("filerName"),
        "sec_code": catalog_row.get("secCode"),
        "fund_code": catalog_row.get("fundCode"),
        "doc_type": catalog_row.get("docTypeCode"),
        "submit_at": catalog_row.get("submitDateTime"),
        "source": catalog_row.get("source"),
        "source_id": catalog_row.get("source_id"),
        "fetched_at": catalog_row.get("fetched_at"),
        "redistribution": catalog_row.get("redistribution"),
    }


def build_dictionary(label_rows, catalog_by_id):
    """表示名の行と書類目録から辞書の行と照合サマリを作る。

    戻り値: (rows, summary)。rows は (doc_id, element, element_ns)
    でソート済み。summary は入力側との照合に必要な件数をすべて持つ。
    """
    invalid = []
    groups = {}
    for row in label_rows:
        if not isinstance(row, dict) or any(
                not isinstance(row.get(k), str) or not row.get(k)
                for k in _LABEL_FIELDS):
            invalid.append(row if isinstance(row, dict) else {"raw": row})
            continue
        key = (row["doc_id"], row["element"], row["element_ns"])
        groups.setdefault(key, []).append(
            (row.get("label_ja"), row.get("label_en")))

    rows = []
    key_conflicts = []
    duplicates = 0
    for (doc_id, element, element_ns), variants in sorted(groups.items()):
        distinct = sorted(set(variants),
                          key=lambda v: (v[0] or "", v[1] or ""))
        duplicates += len(variants) - 1
        if len(distinct) > 1:
            key_conflicts.append({
                "doc_id": doc_id, "element": element,
                "element_ns": element_ns,
                "variants": [{"label_ja": ja, "label_en": en}
                             for ja, en in distinct],
            })
        label_ja, label_en = distinct[0]
        record = {
            "element": element,
            "element_ns": element_ns,
            "element_local": (element.split(":", 1)[1]
                              if ":" in element else element),
            "label_ja": label_ja,
            "label_en": label_en,
            "doc_id": doc_id,
        }
        record.update(_provenance(catalog_by_id.get(doc_id)))
        rows.append(record)

    by_element = defaultdict(set)
    docs_by_element = defaultdict(set)
    for r in rows:
        by_element[r["element"]].add((r["label_ja"], r["label_en"]))
        docs_by_element[r["element"]].add(r["doc_id"])
    label_conflicts = [
        {"element": element,
         "variants": [{"label_ja": ja, "label_en": en}
                      for ja, en in sorted(
                          variants, key=lambda v: (v[0] or "", v[1] or ""))],
         "doc_ids": sorted(docs_by_element[element])}
        for element, variants in sorted(by_element.items())
        if len(variants) > 1
    ]

    docs = sorted({r["doc_id"] for r in rows})
    summary = {
        "input_rows": len(label_rows),
        "invalid_rows": len(invalid),
        "invalid_samples": invalid[:10],
        "duplicate_rows": duplicates,
        "output_rows": len(rows),
        "distinct_docs": len(docs),
        "distinct_elements": len({r["element"] for r in rows}),
        "distinct_element_ns": len({r["element_ns"] for r in rows}),
        "distinct_filers": len({r["edinet_code"] for r in rows
                                 if r["edinet_code"]}),
        "label_ja_missing": sum(1 for r in rows if not r["label_ja"]),
        "label_en_missing": sum(1 for r in rows if not r["label_en"]),
        "docs_without_catalog": [d for d in docs
                                 if d not in catalog_by_id],
        "key_conflicts": key_conflicts,
        "label_conflicts": label_conflicts,
    }
    return rows, summary


def write_dictionary(rows, out_path):
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=DICT_SCHEMA)
    pq.write_table(table, out_path)


def read_dictionary(path):
    return pq.read_table(path).to_pylist()


def build_from_files(label_paths, catalog_paths, out_path, summary_path):
    """ローカル JSONL から辞書を構築し、parquet とサマリを書く。"""
    label_rows = []
    corrupt = Counter()
    for path in label_paths:
        rows, bad = load_jsonl(path)
        label_rows.extend(rows)
        corrupt[f"labels:{path}"] = bad
    catalog_rows = []
    for path in catalog_paths:
        rows, bad = load_jsonl(path)
        catalog_rows.extend(rows)
        corrupt[f"catalog:{path}"] = bad
    catalog_by_id, catalog_duplicates = catalog_index(catalog_rows)
    rows, summary = build_dictionary(label_rows, catalog_by_id)
    summary["corrupt_lines"] = {k: v for k, v in corrupt.items() if v}
    summary["catalog_rows"] = len(catalog_rows)
    summary["catalog_distinct_docs"] = len(catalog_by_id)
    summary["catalog_duplicate_rows"] = catalog_duplicates
    summary["built_at"] = _now()
    write_dictionary(rows, out_path)
    if summary_path:
        os.makedirs(os.path.dirname(os.path.abspath(summary_path)),
                    exist_ok=True)
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def _download(api, repo, path_in_repo, local_path):
    try:
        got = _with_retry("hf_hub_download", lambda: hf_hub_download(
            repo_id=repo, filename=path_in_repo, repo_type="dataset",
            local_dir=None))
    except EntryNotFoundError:
        return False
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    import shutil
    shutil.copyfile(got, local_path)
    return True


def _upload(api, repo, local_path, path_in_repo, message):
    return _with_retry("upload_file", lambda: api.upload_file(
        path_or_fileobj=local_path, path_in_repo=path_in_repo,
        repo_id=repo, repo_type="dataset", commit_message=message))


def sync(repo, months, work_dir, upload=False):
    """v2 データセットの extension_labels から辞書を再構築する。

    months が None の場合は、parse/month=*/extension_labels.jsonl が
    実在する月をすべて対象にする。upload=True のときだけ dictionary/
    へ上げる (parquet が先、サマリが後。途中で落ちても次回の再構築が
    同じ内容を置き直す)。
    """
    api = HfApi()
    remote = _with_retry(
        "list_repo_files",
        lambda: api.list_repo_files(repo, repo_type="dataset"))
    label_paths = sorted(
        p for p in remote
        if p.startswith("parse/month=")
        and p.endswith("/extension_labels.jsonl"))
    if months is not None:
        wanted = set(months)
        label_paths = [p for p in label_paths
                       if p.split("/")[1].split("=", 1)[1] in wanted]
    if not label_paths:
        raise SystemExit("対象の extension_labels.jsonl がありません")

    local_labels = []
    local_catalogs = []
    for path in label_paths:
        month = path.split("/")[1].split("=", 1)[1]
        local = os.path.join(work_dir, "inputs", path.replace("/", "_"))
        if not _download(api, repo, path, local):
            raise SystemExit(f"取得できませんでした: {path}")
        local_labels.append(local)
        catalog_path = f"catalog/month={month}/ledger.jsonl"
        if catalog_path in remote:
            local_cat = os.path.join(
                work_dir, "inputs", catalog_path.replace("/", "_"))
            if _download(api, repo, catalog_path, local_cat):
                local_catalogs.append(local_cat)

    out_path = os.path.join(work_dir, "extension_elements.parquet")
    summary_path = os.path.join(work_dir, "extension_elements_summary.json")
    summary = build_from_files(local_labels, local_catalogs,
                               out_path, summary_path)
    summary["repo"] = repo
    summary["months"] = sorted(
        p.split("/")[1].split("=", 1)[1] for p in label_paths)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    if upload:
        _upload(api, repo, out_path, DICT_PATH_IN_REPO,
                "P2-T10: extension element dictionary")
        _upload(api, repo, summary_path, SUMMARY_PATH_IN_REPO,
                "P2-T10: extension element dictionary summary")
        summary["uploaded"] = [DICT_PATH_IN_REPO, SUMMARY_PATH_IN_REPO]
    return summary


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="拡張タクソノミ表示名の要素辞書 (P2-T10)")
    sub = ap.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build", help="ローカル JSONL から構築する")
    b.add_argument("--labels", nargs="+", required=True,
                   help="extension_labels.jsonl のパス (複数可)")
    b.add_argument("--catalog", nargs="*", default=[],
                   help="書類目録 ledger.jsonl のパス (複数可)")
    b.add_argument("--out", required=True, help="出力 parquet のパス")
    b.add_argument("--summary-out", help="照合サマリ JSON の出力先")

    s = sub.add_parser("sync", help="v2 データセットから再構築する")
    s.add_argument("--repo", default=DEFAULT_REPO)
    s.add_argument("--months", help="対象月 YYYY-MM のカンマ区切り (既定は全月)")
    s.add_argument("--work-dir", required=True)
    s.add_argument("--upload", action="store_true",
                   help="構築結果を dictionary/ へ上げる")

    args = ap.parse_args(argv)
    if args.command == "build":
        summary = build_from_files(args.labels, args.catalog,
                                   args.out, args.summary_out)
    else:
        months = (args.months.split(",") if args.months else None)
        summary = sync(args.repo, months, args.work_dir,
                       upload=args.upload)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
