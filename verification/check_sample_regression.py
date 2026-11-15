#!/usr/bin/env python3
"""標本回帰チェック: 本番変換の出力が試作の確定値と一致するかを確認する。

試作の確定値 (prototype/output/silver_sample.parquet、21,079行) と
本番バッチの結合結果 (silver.parquet) を、行の多重集合として比べる。

比較は2段階:
1. 受け入れ条件のキー (書類・要素・期間・次元・単位・値) での完全一致。
2. 共通する全列 (試作側の cell 列を除く) での完全一致。

使い方:
    python verification/check_sample_regression.py \
        --expected prototype/output/silver_sample.parquet \
        --actual /path/to/out/silver.parquet

終了コード: 両段階とも一致すれば 0、しなければ 1。
"""
import argparse
import hashlib
import sys
from collections import Counter

import pyarrow.parquet as pq

KEY_COLUMNS = ["doc_id", "element", "context_start", "context_end",
               "dimension", "unit", "value"]


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def as_multiset(table, columns):
    rows = table.select(columns).to_pylist()
    return Counter(tuple(r[c] for c in columns) for r in rows)


def compare(name, expected, actual, columns):
    e = as_multiset(expected, columns)
    a = as_multiset(actual, columns)
    only_e = e - a
    only_a = a - e
    print(f"[{name}] 期待 {sum(e.values())} 行 / 実績 {sum(a.values())} 行 / "
          f"期待にのみある行 {sum(only_e.values())} / 実績にのみある行 {sum(only_a.values())}")
    for label, diff in (("期待にのみ", only_e), ("実績にのみ", only_a)):
        for row, n in list(diff.items())[:10]:
            print(f"  {label} x{n}: {row}")
    return not only_e and not only_a


def main():
    ap = argparse.ArgumentParser(description="標本回帰チェック")
    ap.add_argument("--expected", required=True, help="試作の確定値 parquet")
    ap.add_argument("--actual", required=True, help="本番バッチの結合結果 parquet")
    args = ap.parse_args()

    print(f"expected sha256: {sha256_file(args.expected)}")
    print(f"actual   sha256: {sha256_file(args.actual)}")
    expected = pq.read_table(args.expected)
    actual = pq.read_table(args.actual)
    print(f"expected rows: {expected.num_rows}, actual rows: {actual.num_rows}")

    ok_key = compare("受け入れキー", expected, actual, KEY_COLUMNS)
    shared = [c for c in expected.column_names if c != "cell"
              and c in actual.column_names]
    ok_full = compare("共通全列", expected, actual, shared)
    print("結果:", "一致" if (ok_key and ok_full) else "不一致")
    return 0 if (ok_key and ok_full) else 1


if __name__ == "__main__":
    sys.exit(main())
