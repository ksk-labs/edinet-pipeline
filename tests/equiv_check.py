"""等価性テスト: arelle 解析経路 (pipeline.parse) と既存 Silver の標本照合。

仕様 §2 は、arelle の版更新時に自前変換器 (既存 Silver) との標本照合を
必ず行うと定める。本スクリプトはその再実行手段であり、標本8書類について
抽出事実の多重集合 (要素・期間開始・期間終了・次元・単位・値。S2-T3 の
照合規則) を期待値と突き合わせる。

期待値 (tests/equiv_samples.json) は、既存 Silver (v2 データセットの
silver/month=<月>/silver.parquet。バックフィルの成果物) から 2026-11-17
に計算し、実施役の記録済み抽出出力とも全件一致を確認したもの。件数と、
正規化した多重集合の SHA-256 で固定する (実データ自体は Silver 側に
あり、doc_id と月から再取得できるため、リポジトリには置かない)。

標本 zip の取得手順: EDINET API の書類取得 (type=1) で doc_id を指定して
取得する (API キーが必要)。取得した zip を <doc_id>.zip の名前で
--zips-dir に置く。

使い方 (arelle と requirements を入れた解釈系で実行する):

    python tests/equiv_check.py --zips-dir <zip の置き場所> \
        --webcache-dir <arelle のキャッシュ>

--silver-dir に <月>-silver.parquet の置き場所を渡すと、期待値との照合に
加えて、その Silver 実物との多重集合の直接照合も行う。
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANIFEST = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "equiv_samples.json")


def canon_lines(facts):
    """事実の多重集合を正規化した行のリストにする (照合規則は S2-T3)。"""
    lines = []
    for f in facts:
        lines.append(json.dumps(
            [f["element"], f["context_start"], f["context_end"],
             f["dimension"], f["unit"], repr(float(f["value"]))],
            ensure_ascii=False))
    return lines


def canon_hash(facts):
    lines = sorted(canon_lines(facts))
    digest = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
    return digest, len(lines)


def extract_facts(zip_path, webcache_dir, out_path):
    env = dict(os.environ, PYTHONPATH=REPO_ROOT)
    proc = subprocess.run(
        [sys.executable, "-m", "pipeline.parse", "extract-one",
         "--zip", zip_path, "--out", out_path,
         "--webcache-dir", webcache_dir],
        capture_output=True, text=True, env=env, cwd=REPO_ROOT)
    if proc.returncode != 0:
        return None, f"extract-one が失敗 (rc={proc.returncode}): " \
            + (proc.stderr or proc.stdout)[-300:]
    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)
    if "quarantined" in data:
        return None, f"隔離された: {data['quarantined']}: {data['error']}"
    return data["facts"], None


def silver_facts(silver_dir, month, doc_id):
    import pyarrow.parquet as pq
    path = os.path.join(silver_dir, f"{month}-silver.parquet")
    table = pq.read_table(
        path, columns=["doc_id", "element", "value", "unit",
                       "context_start", "context_end", "dimension"])
    return [r for r in table.to_pylist() if r["doc_id"] == doc_id]


def main(argv=None):
    ap = argparse.ArgumentParser(description="arelle 抽出と既存 Silver の標本照合")
    ap.add_argument("--zips-dir", required=True,
                    help="<doc_id>.zip の置き場所")
    ap.add_argument("--webcache-dir", required=True,
                    help="arelle のタクソノミキャッシュ")
    ap.add_argument("--silver-dir", default=None,
                    help="<月>-silver.parquet の置き場所 (任意。直接照合も行う)")
    ap.add_argument("--doc-ids", default=None,
                    help="対象を絞る場合の doc_id (カンマ区切り)")
    args = ap.parse_args(argv)

    with open(MANIFEST, encoding="utf-8") as f:
        manifest = json.load(f)
    samples = manifest["samples"]
    if args.doc_ids:
        wanted = {d.strip() for d in args.doc_ids.split(",") if d.strip()}
        samples = [s for s in samples if s["doc_id"] in wanted]

    mismatches = 0
    with tempfile.TemporaryDirectory() as tmp:
        for s in samples:
            doc_id = s["doc_id"]
            zip_path = os.path.join(args.zips_dir, doc_id + ".zip")
            if not os.path.exists(zip_path):
                print(f"{doc_id}: MISMATCH zip が無い: {zip_path}")
                mismatches += 1
                continue
            facts, error = extract_facts(
                zip_path, args.webcache_dir,
                os.path.join(tmp, doc_id + ".json"))
            if error:
                print(f"{doc_id}: MISMATCH {error}")
                mismatches += 1
                continue
            digest, count = canon_hash(facts)
            if count != s["facts"] or digest != s["facts_sha256"]:
                print(f"{doc_id}: MISMATCH extracted={count} "
                      f"expected={s['facts']} hash "
                      f"{'一致' if digest == s['facts_sha256'] else '不一致'}")
                mismatches += 1
                continue
            note = ""
            if args.silver_dir:
                silver = silver_facts(args.silver_dir, s["month"], doc_id)
                extra = Counter(canon_lines(facts)) \
                    - Counter(canon_lines(silver))
                lacking = Counter(canon_lines(silver)) \
                    - Counter(canon_lines(facts))
                if extra or lacking:
                    print(f"{doc_id}: MISMATCH Silver 実物と不一致 "
                          f"extra={sum(extra.values())} "
                          f"missing={sum(lacking.values())}")
                    mismatches += 1
                    continue
                note = " (Silver 実物とも一致)"
            print(f"{doc_id}: OK facts={count}{note}")
    print(f"TOTAL MISMATCH DOCS: {mismatches}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
