"""過去分 (バックフィル期間) の拡張要素の表示名を原形 zip から抽出する。

Issue #25 (P2-T10: 拡張タクソノミの表示名の要素辞書) の残件。
P2-T10 本体は S2-T5 の日次経路が出す parse/month=<月>/extension_labels.jsonl
を入力に辞書を構築するが、バックフィル期間 (第1期・第2期) の書類は
日次経路を通っていないため表示名が辞書に入らない。本モジュールは
過去分の原形 (提出 zip) から表示名を別途抽出し、同じ
extension_labels.jsonl へ流し込むことで辞書へ取り込む。

経路の決定 (根拠の詳細は作業記録
docs/records/2026-11-19-issue25-dict-backfill-work-record.md):
- 母数は v2 の Silver に現れる (書類, 要素) の組のうち、標準タクソノミ
  (金融庁の一覧ページから取得できる全版の要素キー) に無く、既存の
  extension_labels にも無い組。2026-11-19 の実測で 737,224 組 /
  130,285 書類 (全129か月)。計画段階の切り分けは接頭辞では行わない
  (拡張かどうかは名前空間 URI が決めるため、抽出時の arelle の判定が真値)。
- 原形は v1 データセット (Yoshi-Dai/financial-lakehouse) の日次 tar 内の
  提出 zip を、バックフィルと同じ固定リビジョンから取る。v2 の raw/ は
  S2-T5 の取得分 (2026-11 以降) しかなく過去分を賄えない。v1 へは
  読み取りのみで書き込まない。
- 抽出は pipeline.parse.extract_facts をそのまま使う (新しい抽出器は
  作らない)。arelle の固定事項 (終端は endDate、FormulaOptions の設定、
  開示制度による遮断の無効化) は parse._new_cntlr / extract_facts 側に
  閉じている。

出力と冪等性:
- ラベル行は parse/month=<月>/extension_labels.jsonl へ、既存行を優先する
  重複除去 (parse.run_merge と同じ規則) でマージして上げる。再実行で
  同じ入力からは同じファイルになる。
- 出所は v1 索引の行から合成し、dictionary/backfill/month=<月>/catalog.jsonl
  に置く。catalog/ 本体は discovery の観測台帳であり、合成行を混ぜると
  会社マスタや監視の件数信号が変わるため混ぜない。extension_dict の
  catalog_index は fetched_at の大きい行を優先し、合成行の fetched_at は
  v1 索引の ope_date_time (多くは空) なので、実観測の行がある書類では
  合成行は使われない。
- 進捗は dictionary/backfill/month=<月>/ledger.jsonl (書類単位の台帳)。
  ok / quarantined は完了として再実行で飛ばし、failed は再試行する。
  tar は日単位で取得して処理後に消すため、中断しても日単位で再開できる。
- 書類ごとの書き込みは、ラベル行 (shard) を先に書いてから台帳へ記録
  する順序にする。台帳が先行すると、中断時に台帳だけが進んでラベルが
  欠けたまま再開で飛ばされる。ラベルだけが書かれた状態は、再抽出と
  統合時の重複除去で通常実行と同じ結果に収束する。

対象要素の結果の分類 (台帳の各エントリに理由つきで残る):
- resolved_text: 表示名 (日英どちらか) が取れた。
- no_label_text: ラベル行はあるが日英とも空 (辞書には空の表示名で入る)。
- in_facts_standard: 事実としては現れたが拡張の表示名が無い
  (標準名前空間の要素。例: ifrs-full)。
- not_in_facts: 抽出結果の事実にも表示名にも現れなかった
  (Silver は自前変換器由来のため抽出器の違いで起こりうる)。
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
from datetime import datetime, timezone

import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from . import batch, parse, taxonomy
from .hf_sync import _with_retry
from .ledger import JsonlLedger

V2_REPO = "Yoshi-Dai/financial-lakehouse-v2"
V1_REPO = "Yoshi-Dai/financial-lakehouse"
# バックフィル (backfill.yml) と同じ固定リビジョン・同じ索引を使う。
V1_REVISION = "43540407ea82f792551e145598b1cf50cde6cb78"
V1_INDEX_SHA256 = \
    "68a8c01ea8cc1c952eeeb4b3b1eebf2dd52a3b56b511c45a8f9154b601512997"

LABELS_PATH = "parse/month={month}/extension_labels.jsonl"
LEDGER_PATH = "dictionary/backfill/month={month}/ledger.jsonl"
PROVENANCE_PATH = "dictionary/backfill/month={month}/catalog.jsonl"
SUMMARY_PATH = "dictionary/backfill/month={month}/summary.json"

INDEX_COLUMNS = ["doc_id", "edinet_code", "code", "company_name",
                 "submit_at", "doc_type", "fund_code", "parent_doc_id",
                 "raw_zip_path", "ope_date_time"]

_DONE_STATUSES = ("ok", "quarantined")


def _now():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- 索引と出所

def verify_index_hash(path):
    """v1 索引ファイルの SHA-256 が固定リビジョンのものと一致するか。"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest() == V1_INDEX_SHA256


def load_index(path):
    """v1 索引 (parquet) を doc_id -> 行 dict の辞書で読む。"""
    table = pq.read_table(path, columns=INDEX_COLUMNS)
    index = {}
    for row in table.to_pylist():
        index[row["doc_id"]] = row
    return index


def catalog_row_from_index(row):
    """v1 索引の行から、書類目録 (catalog) 形式の出所行を合成する。

    列の意味は pipeline.extension_dict の docstring (出所の併記) と
    discovery の目録に合わせる。fetched_at は索引の ope_date_time
    (実測ではほぼ空) を入れ、実観測の行があればそちらが優先される
    ようにする。source は索引由来であることが分かる値にする。
    """
    code = row.get("code") or ""
    sec_code = code[len("JP:"):] if code.startswith("JP:") else (code or None)
    return {
        "docID": row["doc_id"],
        "edinetCode": row.get("edinet_code"),
        "filerName": row.get("company_name"),
        "secCode": sec_code or None,
        "fundCode": row.get("fund_code"),
        "docTypeCode": row.get("doc_type"),
        "submitDateTime": row.get("submit_at"),
        "parentDocID": row.get("parent_doc_id"),
        "source": "v1_index",
        "source_id": row["doc_id"],
        "fetched_at": row.get("ope_date_time"),
        "redistribution": "allowed",
    }


# ---------------------------------------------------------------- 標準要素

def load_standard_keys(cache_path=None, fetcher=None):
    """標準タクソノミの要素キー (接頭辞:要素名) の全版ぶんの和集合。

    取得元は金融庁の一覧ページ (pipeline.taxonomy の経路)。版をまたいで
    要素キーは削除されない (2018〜2026 年版の和集合が最新版と一致する
    ことを 2026-11-19 に実測) が、将来の版で削除があっても過去分の
    要素を標準と誤判定しないよう、和集合を定義として使う。
    """
    if cache_path and os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            return set(json.load(f))
    fetcher = fetcher or taxonomy.fetch_url
    html = fetcher(taxonomy.LIST_PAGE_URL).decode("utf-8", errors="replace")
    editions = taxonomy.fetch_editions(fetcher, html)
    keys = set()
    for edition in editions:
        if not edition.get("zip_url"):
            continue
        parsed = taxonomy.parse_taxonomy_zip(fetcher(edition["zip_url"]))
        keys |= set(parsed["elements"].keys())
    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(sorted(keys), f)
    return keys


# ---------------------------------------------------------------- 対象の算出

def _download_or_none(repo, filename, revision=None):
    try:
        return _with_retry("hf_hub_download", lambda: hf_hub_download(
            repo_id=repo, filename=filename, repo_type="dataset",
            revision=revision, token=None))
    except EntryNotFoundError:
        return None


def _read_jsonl(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _jsonl_bytes(rows):
    return "".join(
        json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n"
        for r in rows).encode("utf-8")


def compute_targets(month, standard_keys):
    """その月の対象 (doc_id -> 対象要素のソート済みリスト) と既存ラベル行。

    対象は Silver の (書類, 要素) のうち標準要素でなく、既存の
    extension_labels にも無い組。既存ラベル行はマージの入力として
    そのまま返す。
    """
    silver_path = _download_or_none(
        V2_REPO, f"silver/month={month}/silver.parquet")
    if silver_path is None:
        raise FileNotFoundError(
            f"Silver がありません: silver/month={month}/silver.parquet")
    table = pq.read_table(silver_path, columns=["doc_id", "element"])
    pairs = set(zip(table.column("doc_id").to_pylist(),
                    table.column("element").to_pylist()))
    labels_path = _download_or_none(V2_REPO, LABELS_PATH.format(month=month))
    existing_rows = _read_jsonl(labels_path) if labels_path else []
    covered = {(r["doc_id"], r["element"]) for r in existing_rows}
    targets = {}
    for doc_id, element in pairs:
        if element in standard_keys or (doc_id, element) in covered:
            continue
        targets.setdefault(doc_id, []).append(element)
    for elements in targets.values():
        elements.sort()
    return targets, existing_rows


# ---------------------------------------------------------------- 結果の分類

def classify_targets(target_elements, fact_elements, label_rows):
    """対象要素を結果に分類する。戻り値: (件数 dict, 未解決リスト)。"""
    labels_by_element = {}
    for row in label_rows:
        labels_by_element.setdefault(row["element"], row)
    counts = {"resolved_text": 0, "resolved_empty": 0,
              "in_facts_standard": 0, "not_in_facts": 0}
    unresolved = []
    for element in target_elements:
        row = labels_by_element.get(element)
        if row is not None:
            if row.get("label_ja") or row.get("label_en"):
                counts["resolved_text"] += 1
            else:
                counts["resolved_empty"] += 1
                unresolved.append({"element": element,
                                   "reason": "no_label_text"})
        elif element in fact_elements:
            counts["in_facts_standard"] += 1
            unresolved.append({"element": element,
                               "reason": "in_facts_standard"})
        else:
            counts["not_in_facts"] += 1
            unresolved.append({"element": element,
                               "reason": "not_in_facts"})
    return counts, unresolved


def merge_label_rows(existing_rows, new_rows):
    """既存行を優先して (doc_id, element) で重複除去する。

    parse.run_merge と同じ規則 (既存が勝つ)。戻り値: (統合行, 追加行数)。
    追加行は (doc_id, element, element_ns) の順に並べ、同じ入力からは
    同じ出力になるようにする。
    """
    seen = {(r["doc_id"], r["element"]) for r in existing_rows}
    fresh = []
    for row in new_rows:
        key = (row["doc_id"], row["element"])
        if key in seen:
            continue
        seen.add(key)
        fresh.append(row)
    fresh.sort(key=lambda r: (r["doc_id"], r["element"],
                              r.get("element_ns") or ""))
    return list(existing_rows) + fresh, len(fresh)


# ---------------------------------------------------------------- 実行

def tar_path_of(raw_zip_path):
    """v1 索引の論理パスから、その書類を含む日次 tar のリポジトリパス。"""
    name = batch.day_tar_name(raw_zip_path)
    if not name:
        return None
    return f"raw/edinet/{name[0:4]}/{name[5:7]}/{name[8:10]}.tar"


def _tar_members(tf):
    """日次 tar のメンバー索引 (doc_id -> TarInfo)。batch と同じ規則。"""
    members = {}
    for m in tf.getmembers():
        base = os.path.basename(m.name)
        if base.endswith(".zip") and m.isfile():
            members[base[:-4]] = m
    return members


def _download_tar(tar_path, dest_dir):
    """日次 tar を固定リビジョンから dest_dir へ落とす。無ければ None。"""
    try:
        return _with_retry("hf_hub_download", lambda: hf_hub_download(
            repo_id=V1_REPO, filename=tar_path, repo_type="dataset",
            revision=V1_REVISION, local_dir=dest_dir, token=None))
    except EntryNotFoundError:
        return None


def _extract_one(cntlr, doc_id, target_elements, index_row, tf, members,
                 tmp_dir):
    """1書類を抽出して (台帳エントリ, ラベル行) を返す。

    予期しない例外は呼び出し側で failed として扱うため、ここでは
    parse.Quarantine だけを隔離に振り分ける。
    """
    base = {"doc_id": doc_id, "targets": len(target_elements)}
    if index_row is None:
        return {**base, "status": "failed", "reason": "index_missing",
                "detail": "v1 索引に対象書類がありません"}, []
    if not index_row.get("raw_zip_path"):
        return {**base, "status": "failed", "reason": "no_raw_zip_path",
                "detail": "v1 索引に原形 zip のパスがありません"}, []
    member = members.get(doc_id)
    if member is None:
        return {**base, "status": "failed", "reason": "zip_missing",
                "detail": "日次 tar 内に対象書類の zip がありません"}, []
    zip_path = os.path.join(tmp_dir, doc_id + ".zip")
    with tf.extractfile(member) as src, open(zip_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    try:
        extracted = parse.extract_facts(cntlr, zip_path)
    except parse.Quarantine as q:
        return {**base, "status": "quarantined", "reason": q.reason,
                "detail": q.detail}, []
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)
    fact_elements = {r["element"] for r in extracted["facts"]}
    label_rows = [dict(v, doc_id=doc_id)
                  for v in extracted["labels"].values()]
    counts, unresolved = classify_targets(
        target_elements, fact_elements, label_rows)
    return {**base, "status": "ok", **counts,
            "labels": len(label_rows), "unresolved": unresolved}, label_rows


def _append(ledger, entry):
    entry.setdefault("processed_at", _now())
    ledger.append(entry)


def _summarize(month, targets, state, added, total_label_rows):
    done = {d: e for d, e in state.items()
            if e.get("status") in _DONE_STATUSES}
    agg = {"resolved_text": 0, "resolved_empty": 0,
           "in_facts_standard": 0, "not_in_facts": 0}
    for entry in done.values():
        for key in agg:
            agg[key] += entry.get(key, 0) or 0
    return {
        "month": month,
        "target_docs": len(targets),
        "target_pairs": sum(len(v) for v in targets.values()),
        "done_docs": len(done),
        "pending_docs": len([d for d in targets if d not in done]),
        "ok_docs": len([e for e in state.values()
                        if e.get("status") == "ok"]),
        "quarantined_docs": len([e for e in state.values()
                                 if e.get("status") == "quarantined"]),
        "failed_docs": len([e for e in state.values()
                             if e.get("status") == "failed"]),
        **agg,
        "unresolved_pairs": (agg["resolved_empty"]
                             + agg["in_facts_standard"]
                             + agg["not_in_facts"]),
        "label_rows_total": total_label_rows,
        "label_rows_added": added,
        "generated_at": _now(),
    }


def _publish(api, month, *, work_dir, index, existing_rows, targets, ledger,
             upload, log):
    """ラベルの統合・出所・台帳・要約を、アップロードまたはローカルへ出す。"""
    shard_path = os.path.join(work_dir, "labels", f"month={month}.jsonl")
    shard_rows = _read_jsonl(shard_path) if os.path.exists(shard_path) else []
    merged, added = merge_label_rows(existing_rows, shard_rows)
    doc_ids = {r["doc_id"] for r in merged}
    provenance = [catalog_row_from_index(index[d]) for d in sorted(doc_ids)
                  if d in index]
    summary = _summarize(month, targets, ledger.latest, added, len(merged))
    labels_bytes = _jsonl_bytes(merged)
    provenance_bytes = _jsonl_bytes(provenance)
    summary_bytes = (json.dumps(summary, ensure_ascii=False, indent=1,
                                sort_keys=True) + "\n").encode("utf-8")
    if upload:
        if added or (not existing_rows and merged):
            _upload_bytes(api, labels_bytes, LABELS_PATH.format(month=month),
                          f"extension_backfill: labels {month}")
        remote_prov = _download_or_none(
            V2_REPO, PROVENANCE_PATH.format(month=month))
        remote_prov_bytes = None
        if remote_prov:
            with open(remote_prov, "rb") as f:
                remote_prov_bytes = f.read()
        if remote_prov_bytes != provenance_bytes:
            _upload_bytes(api, provenance_bytes,
                          PROVENANCE_PATH.format(month=month),
                          f"extension_backfill: provenance {month}")
        with open(ledger.path, "rb") as f:
            _upload_bytes(api, f.read(), LEDGER_PATH.format(month=month),
                          f"extension_backfill: ledger {month}")
        _upload_bytes(api, summary_bytes, SUMMARY_PATH.format(month=month),
                      f"extension_backfill: summary {month}")
    else:
        out_dir = os.path.join(work_dir, "out")
        for path_fmt, data in ((LABELS_PATH, labels_bytes),
                               (PROVENANCE_PATH, provenance_bytes),
                               (SUMMARY_PATH, summary_bytes)):
            dest = os.path.join(out_dir, path_fmt.format(month=month))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
        log(f"[backfill-labels] {month}: ローカル出力 "
            f"(追加 {added} 行 / 全 {len(merged)} 行)")
    return summary


def _upload_bytes(api, data, path_in_repo, message):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as f:
        f.write(data)
        tmp = f.name
    try:
        return _with_retry("upload_file", lambda: api.upload_file(
            path_or_fileobj=tmp, path_in_repo=path_in_repo,
            repo_id=V2_REPO, repo_type="dataset", commit_message=message))
    finally:
        os.remove(tmp)


def run_month(api, month, *, index, standard_keys, work_dir, upload=False,
              max_docs=None, keep_tars=False, log=print):
    """1か月分の対象書類を抽出し、ラベル・出所・台帳・要約を出す。"""
    targets, existing_rows = compute_targets(month, standard_keys)
    ledger_dir = os.path.join(work_dir, "ledger")
    os.makedirs(ledger_dir, exist_ok=True)
    ledger_path = os.path.join(ledger_dir, f"month={month}.jsonl")
    if upload and not os.path.exists(ledger_path):
        remote = _download_or_none(V2_REPO, LEDGER_PATH.format(month=month))
        if remote:
            shutil.copyfile(remote, ledger_path)
    ledger = JsonlLedger(ledger_path, key_of=lambda e: e.get("doc_id"))
    state = ledger.latest
    done = {d for d, e in state.items() if e.get("status") in _DONE_STATUSES}
    pending = [d for d in targets if d not in done]
    pending.sort(key=lambda d: ((index.get(d) or {}).get("submit_at") or
                                "9999", d))
    if max_docs is not None:
        pending = pending[:max_docs]
    log(f"[backfill-labels] {month}: 対象 {len(targets)} 書類 / "
        f"完了済み {len(done)} / 今回 {len(pending)} 書類")
    if not pending:
        return _publish(api, month, work_dir=work_dir, index=index,
                        existing_rows=existing_rows, targets=targets,
                        ledger=ledger, upload=upload, log=log)
    by_day = {}
    for doc_id in pending:
        row = index.get(doc_id)
        tar_path = tar_path_of((row or {}).get("raw_zip_path"))
        by_day.setdefault(tar_path, []).append(doc_id)
    shard_dir = os.path.join(work_dir, "labels")
    os.makedirs(shard_dir, exist_ok=True)
    shard_path = os.path.join(shard_dir, f"month={month}.jsonl")
    tmp_dir = os.path.join(work_dir, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    tar_dir = os.path.join(work_dir, "tars")
    os.makedirs(tar_dir, exist_ok=True)
    webcache = os.path.join(work_dir, "webcache")
    cntlr = None
    for tar_path in sorted(by_day, key=lambda p: (p is None, p or "")):
        docs = by_day[tar_path]
        tf = None
        members = {}
        tar_local = None
        if tar_path is not None:
            tar_local = _download_tar(tar_path, tar_dir)
            if tar_local is not None:
                tf = tarfile.open(tar_local)
                members = _tar_members(tf)
        try:
            for doc_id in docs:
                if tf is None and tar_path is not None:
                    _append(ledger, {
                        "doc_id": doc_id, "status": "failed",
                        "reason": "tar_missing",
                        "detail": f"日次 tar を取得できません: {tar_path}",
                        "targets": len(targets[doc_id])})
                    continue
                if cntlr is None:
                    cntlr = parse._new_cntlr(webcache)
                try:
                    entry, label_rows = _extract_one(
                        cntlr, doc_id, targets[doc_id], index.get(doc_id),
                        tf, members, tmp_dir)
                except Exception as e:  # noqa: BLE001 - 書類単位で隔離する
                    entry = {"doc_id": doc_id, "status": "failed",
                             "reason": "unexpected",
                             "detail": f"{type(e).__name__}: {e}",
                             "targets": len(targets[doc_id])}
                    label_rows = []
                    cntlr = None  # 次の書類で Cntlr を作り直す
                # ラベル行は台帳への記録より先に shard へ書く (台帳の
                # 追記と同じく fsync まで行う)。台帳が先だと、その間で
                # 落ちたときに「台帳は解決済み・ラベルは欠落」のまま
                # 再開で飛ばされ、要約の解決数とラベルファイルが黙って
                # 食い違う (SF-1)。ラベルだけが残った場合は再抽出で
                # 重複行が出るが、統合 (merge_label_rows) の重複除去が
                # 受けるため冪等に収束する。
                if label_rows:
                    with open(shard_path, "a", encoding="utf-8") as f:
                        for row in label_rows:
                            f.write(json.dumps(row, ensure_ascii=False)
                                    + "\n")
                        f.flush()
                        os.fsync(f.fileno())
                _append(ledger, entry)
        finally:
            if tf is not None:
                tf.close()
            if tar_local and not keep_tars and os.path.exists(tar_local):
                os.remove(tar_local)
        if upload:
            _publish(api, month, work_dir=work_dir, index=index,
                     existing_rows=existing_rows, targets=targets,
                     ledger=ledger, upload=upload, log=log)
    return _publish(api, month, work_dir=work_dir, index=index,
                    existing_rows=existing_rows, targets=targets,
                    ledger=ledger, upload=upload, log=log)


# ---------------------------------------------------------------- CLI

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="過去分の拡張要素の表示名を原形 zip から抽出する")
    parser.add_argument("--index", required=True, help="v1 索引 parquet")
    parser.add_argument("--work-dir", default="work/extension-backfill")
    parser.add_argument("--skip-index-hash-check", action="store_true",
                        help="索引の SHA-256 照合を省略する (意図的な別版用)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_targets = sub.add_parser("targets", help="対象の件数だけを数える")
    p_targets.add_argument("--month", required=True)
    p_run = sub.add_parser("run", help="抽出を実行する")
    p_run.add_argument("--months", required=True,
                       help="対象月 (YYYY-MM のカンマ区切り)")
    p_run.add_argument("--max-docs", type=int, default=None,
                       help="1か月あたりの処理書類数の上限 (再開前提の分割用)")
    p_run.add_argument("--upload", action="store_true",
                       help="結果を v2 データセットへ上げる")
    p_run.add_argument("--keep-tars", action="store_true")
    args = parser.parse_args(argv)

    if not args.skip_index_hash_check and not verify_index_hash(args.index):
        print(f"索引の SHA-256 が固定リビジョンのものと一致しません: "
              f"{args.index}", file=sys.stderr)
        return 1
    os.makedirs(args.work_dir, exist_ok=True)
    standard_keys = load_standard_keys(
        cache_path=os.path.join(args.work_dir, "standard_keys.json"))
    if args.cmd == "targets":
        targets, existing = compute_targets(args.month, standard_keys)
        print(json.dumps({
            "month": args.month,
            "target_docs": len(targets),
            "target_pairs": sum(len(v) for v in targets.values()),
            "existing_label_rows": len(existing),
        }, ensure_ascii=False))
        return 0
    index = load_index(args.index)
    api = HfApi()
    summaries = []
    for month in [m.strip() for m in args.months.split(",") if m.strip()]:
        summaries.append(run_month(
            api, month, index=index, standard_keys=standard_keys,
            work_dir=args.work_dir, upload=args.upload,
            max_docs=args.max_docs, keep_tars=args.keep_tars))
    print(json.dumps(summaries, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
