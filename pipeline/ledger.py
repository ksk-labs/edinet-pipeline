"""追記型 JSONL 目録の共通土台。

目録は JSONL の追記ファイルで、キーごとの最新行が現在の状態を表す。
読み込みでは壊れた行 (中断で最終行が途中で切れた場合など) を無視する
(追記は末尾にしか起きないため、壊れた行は「記録なし」として扱える)。
追記は flush と fsync まで行ってから最新行と試行回数を更新する。

キーは呼び出し側が key_of (行 -> キー) で決める。キーが取れない行は
状態を持たないため、最新行にも試行回数にも数えない。
pipeline.batch.Ledger (Silver 変換の処理済み目録) と
pipeline.discovery.JsonlLedger (日次取得の目録) は、この土台の
特殊形である。
"""
import json
import os
from collections import Counter


class JsonlLedger:
    """追記型の JSONL 目録。キーの最新行が状態。"""

    def __init__(self, path, key_of):
        self.path = path
        self.key_of = key_of
        self.latest = {}
        self.attempts = Counter()
        self.corrupt_lines = 0
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        self.corrupt_lines += 1
                        continue
                    key = key_of(entry)
                    if not key:
                        continue
                    self.latest[key] = entry
                    self.attempts[key] += 1

    def append(self, entry):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        key = self.key_of(entry)
        if key:
            self.latest[key] = entry
            self.attempts[key] += 1
