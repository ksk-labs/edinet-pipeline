# S2-T6 作業記録: v1 未収穫 250 件の補完取得と Silver への追加 (Issue #26)

## 目的と範囲

第1期バックフィル (S2-T2) の全数照合で確定した、xbrl_flag=true なのに
v1 に実ファイル (raw_zip_path) が無く未収穫だった 250 件を、EDINET API
から補完取得して Silver へ追加する。本記録の変更はコードとワークフロー
と照合の仕組みまでであり、実際の取得・解析の実行 (ワークフローの
dispatch) は着地後に別途行う。

## 実測した前提 (2026-11-17、固定リビジョンの v1 索引で再現)

- 母数 305,662 件、未収穫 250 件。doc_id の集合は S2-T2 の照合結果
  (reconcile-result.json) と完全一致した。
- 提出日は 12 日に集中する (2026-03-13 が 134 件、2026-11-13 が 22 件、
  2024-11-13〜15 が 4 件など)。提出月は 2024-11 (4)、2024-12 (1)、
  2026-03 (222)、2026-04 (1)、2026-11 (22)。
- 対象に doc_type=130 (訂正報告書) は 0 件。対象を親に持つ書類は
  17 件あるが、doc_type=130 の子は 0 件のため、Silver へ追加しても
  既存書類の superseded_by は変わらない (系譜の再計算は不要)。

## 変更内容

### pipeline/backfill_missing.py (新規)

補完取得の実施役。日次取得 (pipeline.discovery) と同じ目録の仕組みに
載せる。

- 対象の確定は固定リビジョンの v1 索引から行う (select_targets)。
  対象リストを別に保持せず、母数の定義から毎回再現する。
- 書類目録は、提出日の一覧を EDINET API で取得して観測として追記
  する。一覧が取れなかった日は v1 索引由来の行で代用し、
  catalog_source=v1_index_fallback と明記する。
- 原形は書類取得 API の type=1 (zip) のみを取得し、discovery と
  同じ保存先 (raw/edinet/YYYY/MM/DD/zip/) へ置く。取得目録の ok は
  アップロード後に遠隔で実在を確認できた分だけ書く (冪等)。
- 取得できない書類は取得目録へ failed として理由つきで記録する。
  1 書類の失敗で全体を止めない。再実行で再試行される。
- リクエスト間隔と再試行は discovery.EdinetClient の作法 (1.1 秒、
  429 は指数バックオフ) をそのまま使う。

### .github/workflows/backfill_missing.yml (新規)

acquire → plan → parse (マトリクス) → merge の 4 ジョブ。
plan 以降は daily_parse.yml と同じ構成で、acquire が出力した書類 ID
一覧を parse plan の --doc-ids に渡す。EDINET API キーはリポジトリの
Secrets から渡す (取得はこのワークフロー経由でのみ行う)。

### verification/reconcile_backfill.py (拡張)

照合項目 6 を追加した。未収穫だった書類はすべて、recovered
(Silver に追加済み。解析目録 ok + 結合 parquet の行数一致)、
unobtainable (取得目録 failed で理由つき)、quarantined (取得できたが
解析で隔離)、unexplained のどれかに分類される。quarantined と
unexplained が残れば終了コード 1 になる。

### tests/test_backfill_missing.py (新規)

対象選定、目録の源泉の明記、取得の成否の記録、再実行での
スキップ (冪等)、一覧失敗時の代用、照合の分類の 7 件。

## 検証

- 新規テスト 7 件合格。既存を含む全 68 件も合格
  (`python -m unittest discover -s tests`)。
- 拡張した照合を実データ (v2 の実物) で実走した: 既存の照合項目は
  従来どおりすべて一致 (母数 305,662、ok 305,412、行数 50,560,027 で
  目録と parquet が一致、failed・欠落・余分・行数不一致すべて 0) し、
  分類は recovered 0、unobtainable 0、quarantined 0、unexplained 250
  で終了コード 1 になった。dispatch 前の正しい状態であることを確認
  した (取得と解析が済めば unexplained は 0 になる)。

## 残る手順 (着地後)

1. backfill_missing ワークフローを dispatch する (まず max_docs で
   小規模に動作確認し、問題が無ければ全件)。
2. 実行サマリで取得できた件数と取得できなかった件数 (理由) を確認
   する。取得できなかった書類は取得目録の failed が正本になる。
3. verification/reconcile_backfill.py を再実行し、missing_classification
   で recovered と unobtainable の合計が 250 件になり、unexplained と
   quarantined が 0 であることを確認する。Silver の増加行数は
   recovered 書類の解析目録の rows の合計として数える。
