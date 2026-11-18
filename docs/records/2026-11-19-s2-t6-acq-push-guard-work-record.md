# S2-T6 取得目録の月末 push の欠陥修正の作業記録

- 日付: 2026-11-19
- 対象: Issue #26（S2-T6、v1 未収穫 250件の補完取得）
- ブランチ: `s2t6-acq-push-guard`（main 23d6c9c9 から分岐）

## 背景

小規模確認（backfill_missing ワークフロー、max_docs=5、run 44372301295）
が acquire ジョブで失敗した。ログの実物：

```
ValueError: Provided path: 'work/acquire/state/acquisition-2026-03.jsonl'
is not a file on the local file system
```

発生箇所は `pipeline/backfill_missing.py` の `run_acquire` 内、
月ループ末尾の無条件の `push_acquisition_ledger()` 呼び出しである。

## 原因

対象 250件の提出月は 2024-11 が4件、2024-12 が1件、2026-03 が222件、
2026-04 が1件、2026-11 が22件である。max_docs=5 では 2024-11 で4件、
2024-12 で1件を試行して上限に達し、次の月 2026-03 の行ループは最初の行で
`stopped_early` となり、取得目録への追記が0件のまま月末の push に到達した。
`acquisition/month=2026-03/ledger.jsonl` は HF 上に存在しないため
ローカルの state ファイルも作られず（`JsonlLedger.append` は初回追記時に
だけファイルを作る）、アップロードが存在しないパスを指して落ちた。

5件の取得自体は成功しており、HF 上で raw zip 5件と取得目録
2024-11（4行・全件 ok）・2024-12（1行・ok）が揃っている。
失敗は月末 push の欠陥だけで、取得物は無事である。

## 修正

`pipeline/backfill_missing.py` の月末の `push_acquisition_ledger()` の
呼び出しを、ローカルの取得目録ファイルが存在する場合だけに行う
（`os.path.exists(acq_local)` でのガード）。存在しないのは、遠隔目録が
無く追記も0件の場合だけであり、その月には上げるものが無い。

- 変更はこの1箇所だけである。catalog 側の push は行ループで必ず追記が
  あるため対象外。ループ内の `if confirmed:` で呼ばれる push は追記が
  ある場合だけなので対象外（触っていない）。
- 遠隔目録が存在して追記が0件の月は、ローカルファイルがダウンロード済みで
  存在するため push は従来どおり行われ、内容が同一なら HF 側が空コミットを
  省く（既存の挙動のまま）。

## 検証

- 回帰テスト `test_max_docs_exhausted_before_next_month_does_not_crash`
  を `tests/test_backfill_missing.py` に追加した。max_docs=1 で 2024-11 の
  1件だけを試行し、次の月 2026-03 に追記0件で到達しても落ちず、
  `stopped_early=True` でサマリが返ること、2026-03 分の取得目録が
  遠隔に作られないこと、実行サマリが書き出されることを縛る。
  HF・EDINET API は叩かず、既存の FakeClient・FakeHfApi を使う。
- 修正前コードでの再現確認：`git stash` で修正だけを外して回帰テストを
  実行し、`FileNotFoundError: .../work_next/state/acquisition-2026-03.jsonl`
  で失敗することを確認した（再試行のバックオフのため約230秒かかった）。
  その後 stash を戻し、修正が復旧していることを差分で確認した。
- 全テスト：`python -m unittest discover -s tests` で **212件全件合格**
  （新規回帰テスト1件を含む。実行環境はこの作業用に新規作成した
  `s2t6-fix/venv` で、requirements.txt から依存関係を入れたもの）。

## 残手順（メイン側）

- PR の code-review と独立検証を経てマージする。
- マージ後、backfill_missing ワークフローを max_docs=5 で再 dispatch し、
  小規模確認を行う（再実行では取得済みの5件は skipped_done となり、
  次の5件（2026-03 分）が試行される）。
