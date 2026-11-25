# Issue #48 作業記録（監視の遅延時誤警報の是正）

- Issue: ksk-labs/edinet-pipeline#48「S2-T5 後続: 監視の実行が遅れた日の誤警報（codelist_missing・parse_backlog）の扱い」
- 仕様の正本: `~/workspace/goals/edinet/hidden_files/issue48/issue48-body.md`（Issue #48 本文の実物＋2026-11-20/21 の精緻化した形＋ユーザー決定 2026-11-25 04:41）
- 作業場所: `~/workspace/goals/edinet/hidden_files/issue48/repo`（ブランチは exec 回復後に作成する）
- 実施役: implementer（レビュー・マージ・Issue クローズはメインが行う）

## 1. 設計の決定

### 1.1 締め時刻の統一規則

締め時刻は「その日の最終実行の定刻 + GitHub スケジューラ遅延の実測最大 10時間」とする。

- 遅延の実測（Issue #48 本文と S2-T5 観測より）: codelist 15:15 JST 枠が翌 01:04 JST 実行（9時間49分遅れ）、monitor 21:30 JST 枠が翌 06:15 JST 実行（8時間45分遅れ）、daily_acquire 約6.5時間、daily_parse 約6時間。最大 9時間49分を切り上げて 10時間とした。
- 取得（対象日 T、実行 03:30/15:30 JST on T+1）: 締め = 15:30 + 10時間 = T+2 の 01:30 JST。
- コードリスト（日付 D、実行 09:15/15:15/19:00/23:00 JST on D）: 締め = 23:00 + 10時間 = D+1 の 09:00 JST。
- 解析の滞留（実行 05:30 JST on D）: 締め = 05:30 + 10時間 = 当日 15:30 JST。
- Gold（生成 20:30 JST on D）: 締め = 20:30 + 10時間 = D+1 の 06:30 JST。
- 会社マスタは日付に加えて sha でも判定する（後述 §1.4。code-review の S-1 による修正）。

### 1.2 点検日の決め方（判定の保留に代わる「締めを過ぎた最新の日を点検する」方式）

監視は各項目について、実行時刻（JST now）時点で締め時刻を過ぎている最新の日を点検する。点検日は実行日（run_date）を上限とする（過去日を指定した手動再実行が未来の日を点検しないため）。締めを過ぎていない当日分は点検せず、警報も出さない。

- 定常（21:30 JST の定刻実行）での点検日: 取得は run_date - 2、コードリストは run_date - 1、解析は run_date、Gold は run_date - 1。
- 監視が深夜をまたいで遅れた実行の場合、点検日は各項目の締め時刻との関係で決まる（「さらに1日ずれる」のは、締めがまだ過ぎていない項目だけである）。例として翌 06:15 JST の実行（run_date は実行日。schedule 実行では `--date` を渡さないため run_date は実行当日の日付になる）: 取得は run_date - 2（変わらず。取得の締めは 01:30 であり、06:15 時点では既に過ぎているため）、コードリストは run_date - 2、解析は run_date - 1、Gold は run_date - 2。取得が run_date - 3 になるのは、実行が 01:30 より前の場合だけである。
- 帰結: 各日の各項目は、締めを過ぎた最初の監視実行でちょうど1回点検される（重複点検は無害で、同じ結果になる）。代償として取得・コードリスト・Gold の判定は定常で1日遅れる。信頼性を速度より優先する方針（ユーザー明示）による。
- 取得の連続失敗の数え方: 点検対象日が前回の点検対象日より新しい場合にだけ加算する。同じ対象日の再実行や、過去日への手動再実行では加算しない（後者は従来の欠陥の修正でもある）。

### 1.3 codelist_conflict の降格と「正」の変更

- conflict は警報から記録（情報）へ落とす。台帳への競合の記録は残す（status は "replaced" に変わる）。
- コードリストの「正」は「その日の最後に成功した取得」とする。従来の「先行分を保持」は廃止する。
- 差し替え時の挙動（codelist.py）: 同日・異内容の取得があった場合、スナップショット一式（CSV・snapshot.json・archive zip）を新しい版で上書きし、差分（diff.json・changes.jsonl）を新しい版で再生成する。台帳行の status は "replaced" とし、新旧の sha256 を両方残す。run_summary の status も "replaced"。
- 旧版の復元: 上書きされた旧版は HF データセットのコミット履歴に残る（台帳行の existing_sha256_csv で特定できる）。

### 1.4 会社マスタへの取り込み（再適用の配線）

- 現状の問題: `apply_new_snapshots` は `date <= last_applied_snapshot_date` を読み飛ばすため、適用済みの日付のスナップショットが差し替えられても再適用されない。
- 配線: スナップショットを (日付, 行, sha256) で扱い、state の meta に `applied_snapshot_sha256`（日付→sha）を保持する。最終適用日と同じ日付で sha が変わっていれば、その日付を再適用する（`apply_snapshot` の再実行）。再適用は `applied_snapshots` に `reapplied: true` として記録し、summary に `last_applied_snapshot_sha256` を載せる。
- 再適用の安全性: `_observe` は同値なら最終観測日と観測回数を伸ばすだけであり、値が変わった場合は新しい行を開く（旧行は閉じたまま残る）。同じ日付の再適用は、観測回数が1増える（再観測として数える）ほかは既存の履歴を壊さない。分類の `snapshot_count` も同じく1増える。再適用は差し替えられた日付にだけ起きる。
- 最終適用日より前の日付の差し替えは再適用しない（`apply_snapshot` は最終状態を前提としており、古い日付の再適用は `in_latest_snapshot` などを壊す）。実測上あり得ない：差し替えは当日のファイルにしか起きず、実行遅延の実測最大は約10時間で、24時間を超える遅延は観測されていない。残余リスクとしてここに記録する。
- 移行時の1回だけの再適用: 既存の state には `applied_snapshot_sha256` が無いため、初回の更新では最終適用日を（現在の sha で）再適用する。これは1回だけのもので、観測回数が1増えるほか影響はない。
- 監視の会社マスタ判定は、日付に加えて sha も比べる（code-review の S-1 による修正。2026-11-25 07:4x）。同日差し替え後に master update が失敗すると適用済み日付と最新日付は同じままになるため、日付だけでは気づけない。比較は master/summary.json の `last_applied_snapshot_sha256` と、最新スナップショットの snapshot.json の `sha256_csv` で行う。どちらかの sha が無ければ（移行前の summary など）日付だけの比較に落とし、誤った stale は出さない。監視の早朝実行で「適用済み < 最新」が誤って出る構造ではない点は従来どおり（マスタ更新は codelist ジョブの後段で直列化されているため）。

### 1.5 取得スケジュールの追加（夕方・深夜の回）

daily_codelist.yml に 19:00 JST と 23:00 JST の実行を追加する。昼の 09:15/15:15 はフォールバックとして残す。

測定（HF v2 の codelist/ledger.jsonl 全17行、2026-11-17〜2026-11-25 取得）：

| 日付（宣言日） | 初回取得（JST） | 差し替え版の取得（JST） | 行数 |
|---|---|---|---|
| 2026-11-17 | 11:56（11468行） | 翌 01:04（11469行） | 変化 |
| 2026-11-18 | 16:18（11471行） | 翌 00:42（11471行、同数・内容違い） | 内容変化 |
| 2026-11-19 | 14:55（11468行） | 16:16（11468行、同数・内容違い）、翌 00:48（同版） | 内容変化 |
| 2026-11-20 | 16:05（11469行） | —（翌 00:30 は同版） | — |
| 2026-11-21 | 15:58（11469行） | —（23:41 は同版） | — |
| 2026-11-22 | 15:39（11469行） | —（23:38 は同版） | — |
| 2026-11-23 | 16:28（11469行） | 翌 03:14（11469行、同数・内容違い） | 内容変化 |
| 2026-11-24 | 18:47（11470行） | —（翌 02:49 は同版） | — |

- 同日差し替えは 8日中 4日（11-17・11-18・11-19・11-23）で発生した。
- 差し替えが起きた時刻の挟み込み：唯一の厳密な挟み込みは 11-19 の 14:55→16:16（14:55 の取得は旧版、16:16 の取得は新版）。他の差し替えは初回取得（11:56〜16:28）と次回取得（翌 00:42〜03:14）の間に発生した。すなわち差し替えは午後以降に起きる。
- 15:15 枠の実行は実測で 15:30〜16:30 JST ごろに着地しており、16:00 以降の差し替えを取りこぼす（11-19 の 16:16 の例）。
- 19:00 枠は 16:00〜19:00 の差し替えを、23:00 枠は 19:00〜23:00 の差し替えを、それぞれ定刻どおり走れば拾う。
- 23:00 をこれ以上遅くしない理由：日付を跨いで遅延実行されると翌日分のファイルを取得してしまい、当日分の最終版が失われる。台帳の実測では、翌 03:14 の取得はまだ前日分を宣言し、09:47 の取得は当日分を宣言している（ファイルの切り替わりは 03:14〜09:47 JST の間）。23:00 枠が 10時間遅れると翌 09:00 JST 実行となり、切り替わり後かもしれないが、その場合は翌日分の初回取得として扱われ、当日分を壊さない。
- 残余リスク（精緻化した形のとおり）: 夜間の取得がその日すべて失敗・遅延した場合は、その日は日中の版が残り、その日の遅い変更は翌日のスナップショットで検知される（検知日が1日ずれる）。

## 2. 変更範囲

- `pipeline/monitor.py`: 締め時刻による点検日の決定、codelist_conflict 警報の廃止（replaced を情報として報告）、連続失敗の加算条件の修正、報告への点検日の記録。**S-1 の修正（2026-11-25 07:4x）**: `_judge_company_master` で日付に加えて sha でも stale を判定する。`run_monitor` で最新スナップショットの sha を読んで渡す。報告と警報に sha を載せる。
- `pipeline/codelist.py`: 同日・異内容を "replaced" とし、スナップショット一式の上書きと差分の再生成。`_complete_diff` に強制再生成を追加。**M-1 の修正（2026-11-25 07:4x）**: 既存の差分が古い版の場合に stale として再生成して上書きする。既存の差分が読めない・取得できない場合も同じ扱いとする。
- `pipeline/company_master.py`: スナップショットへの sha256 の追加、最終適用日の再適用、summary への適用 sha の追加。`apply_new_snapshots` は (日付, 行, sha256) の3要素に加え、sha の無い (日付, 行) の2要素（テストやローカル検証用）も受け付け、その場合は従来どおり最終適用日以前を読み飛ばす。
- `.github/workflows/daily_codelist.yml`: 19:00・23:00 JST の実行を追加。
- `.github/workflows/daily_monitor.yml`: コメントの更新（点検対象の日の決め方）。
- `tests/test_monitor.py`、`tests/test_codelist.py`、`tests/test_company_master.py`: 新しい挙動のテストと、変わった意味への既存テストの更新。**M-1 のテスト3件、S-1 のテスト3件を追加（2026-11-25 07:4x）**。詳細は §3.3 を参照。
- `docs/records/2026-11-25-issue48-work-record.md`: 本記録。

## 3. テスト結果

### 3.1 全スイートの実行結果（2026-11-25 07:1x、exec 回復窓で実施）

実行コマンド（作業場所 `~/workspace/goals/edinet/hidden_files/issue48/repo` から、t3 venv の python で）:

```
~/workspace/goals/edinet/hidden_files/t3/venv/bin/python -m unittest discover -s tests -v
```

- **結果：381件中 381件合格（Ran 381 tests、OK）。失敗 0件。**
- 初回実行では 381件中 28件がエラーで失敗した。28件すべて `tests/test_monitor.py` のテストで、原因はすべて同一：`NameError: name '_load_json' is not defined`（`pipeline/monitor.py` の 416行目、`run_monitor` 内の呼び出し）。

### 3.2 28件失敗の原因と修正

- **原因**：実装中に `pipeline/monitor.py` のヘルパー `_load_json` の `def` 行だけが失われていた。Issue #48 で新設した `_latest_due_date` の直後に、本来の定義行 `def _load_json(api, repo, remote, path_in_repo, work_dir, name):` が無く、その本体（docstring と実装）が `_latest_due_date` の `return due` の直後にぶら下がった到達不能なコードになっていた。呼び出し箇所は 6か所（`_acquisition_summaries`、`_judge_codelist`、`run_monitor` 内の state・master・gold の読み込み）すべて残っていたため、呼び出しに至るテストがすべて `NameError` で失敗した。`_upload_json` は別関数（書き込み用）であり無関係。
- **特定の根拠**：`git show HEAD:pipeline/monitor.py` で main の定義行（70行目）`def _load_json(api, repo, remote, path_in_repo, work_dir, name):` を確認し、作業ツリーの本体の引数名・使用変数（`path_in_repo`、`remote`、`work_dir`、`name`、`api`、`repo`）と一致することを照合した。改名ではなく定義行の欠落である。
- **修正**：`pipeline/monitor.py` の `_latest_due_date` の `return due` の直後に、欠落した定義行を復元した（最小の修正。1か所、定義行の追加のみ。本体と呼び出し側は変更なし）。修正後、全スイートを再実行し 381件すべて合格を確認した（§3.1）。

追加・更新したテスト:

- `tests/test_monitor.py`: `DeadlineTest` を新設（06:15 の早朝実行で各項目が前日・前々日分を点検し誤警報が出ないこと、取得・コードリスト・解析・Gold の締め時刻の境界 01:30/09:00/15:30/06:30、00:30 のごく早い実行で締めを過ぎた古い日だけを点検すること）。`HealthyDayTest`・`AcquisitionFailureTest`・`LatestSummarySelectionTest`・`MonitorGapTest`・`CodelistMissingTest`・`ParseIssuesTest`・`CompanyMasterMonitorTest`・`GoldMonitorTest` の fixture を新しい点検日（定刻実行では取得 run_date - 2、コードリスト run_date - 1、解析 run_date、Gold run_date - 1）へ更新。`CodelistMissingTest` では conflict の警報の代わりに、replaced・旧 conflict 行が警報にならず情報として報告されることと、diff_pending が引き続き警報になることを確認するテストへ置き換えた。`AcquisitionFailureTest` に過去日への手動再実行で連続失敗と点検対象日が後ろへ戻らないテストを追加した。
- `tests/test_codelist.py`: `test_conflict_keeps_first` を `test_replaced_overwrites_with_last` へ置き換え（同日・異内容では新しい版で上書きされ、status は "replaced"、台帳に新旧の sha256 が残り、差分が新しい版で再生成されること）。`test_replaced_then_same_run_is_idempotent` を追加（差し替え後に同じ版を再実行すると already_present になること）。
- `tests/test_company_master.py`: `ReapplySnapshotTest` を新設（最終適用日と同じ日付で sha が変われば再適用され `reapplied: true` が記録されること、同じ sha では何もしないこと、最終適用日より前の日付の差し替えは再適用されないこと、applied_snapshot_sha256 の記録が無い既存の state では最終適用日を現在の sha で1回だけ再適用すること、summary に `last_applied_snapshot_sha256` が載ること）。

突然変異の確認（exec 回復後に実施）: 締め時刻の定数（`SCHEDULER_DELAY_ALLOWANCE` の 10時間、各工程の最終実行時刻）をずらすと `DeadlineTest` の境界テストが落ちること、"replaced" の判定を外すと `CodelistMissingTest` の新テストが落ちること、`apply_new_snapshots` の再適用条件を外すと `ReapplySnapshotTest` が落ちること、をそれぞれ確認する。

### 3.3 code-review の指摘 M-1・S-1 の修正（2026-11-25 07:4x）

レビュー報告書は `~/workspace/goals/edinet/hidden_files/issue48-review/issue48-code-review.md`。判定は「修正が必要」（must-fix 1件・should-fix 1件・note 8件）。note（N-1〜N-8）のうち対応が要るものはない。

**M-1（`pipeline/codelist.py` の `_complete_diff`）**: 既存 diff.json の sha が現在のスナップショットと異なる場合、再生成へ進むものの products ループの `if repo_path in remote and not force: continue` により両ファイルともアップロードがスキップされ、戻り値は status="generated" になるがリモートの diff.json は古い版のまま残っていた。修正は、不一致を検出した時点でローカルに `stale` フラグを立て、ガードを `not (force or stale)` とした。あわせて、既存の差分が読めない場合（JSON の破損）と取得できない場合（ダウンロード失敗）も、sha を検証できないため同じ扱い（stale として再生成して上書き）とした。内容が同じなら再送は冪等である。なお、既存の差分が無く changes.jsonl だけが残っている場合など、従来の補完経路（不足した成果物だけを上げる）の挙動は変えていない（sha が一致する差分は再送しない）。

**S-1（`pipeline/monitor.py` の `_judge_company_master`）**: 会社マスタの stale 判定を、日付だけでなく sha でも比べるようにした。比較は master/summary.json の `last_applied_snapshot_sha256` と、最新スナップショットの snapshot.json の `sha256_csv` で行う。`run_monitor` で最新スナップショットの snapshot.json を読んで sha を渡す。どちらかの sha が無ければ（移行前の summary など）日付だけの比較に落とし、誤った stale は出さない。報告の company_master と stale の警報に、両方の sha を載せた。

**追加したテスト**:
- `tests/test_codelist.py`（M-1 の経路3件）: `test_stale_diff_regenerated_and_uploaded_on_rerun`（差し替え時に差分の再生成が失敗して古い差分が残り、次回の同一内容の実行で新しい版へ更新されること）、`test_corrupt_existing_diff_regenerated_and_uploaded`（既存の差分が壊れた JSON の場合も再生成して上書きすること）、`test_diff_download_failure_regenerates_and_uploads`（既存の差分を取得できない場合も再生成して上書きすること）。
- `tests/test_monitor.py`（S-1 の経路3件）: `test_same_date_different_sha_is_stale`（同日・異 sha で stale になり `company_master_stale` の警報が出ること）、`test_same_date_same_sha_is_ok`（同日・同 sha で ok になること）、`test_same_date_missing_sha_falls_back_to_date`（移行前の summary（sha 無し）では日付だけの比較に落とし、誤った stale を出さないこと）。fixture の `put_codelist_ok` と `put_master` に sha を渡せる引数を追加した（既存の呼び出しは変更なし）。

**検証の状況（未了）**: exec 基盤障害（`failed to prepare tool cgroup: no pure cgroup-v2 base resolved`）のため、本タスク中は exec が一度も起動せず、再現スクリプトの修正前後の実行と全スイートの再実行ができていない。修正を先に適用したため、修正前の再現確認は検証スクリプトで修正前の状態へ一時的に戻して行う。exec 回復後の再開手順は次のとおり（1コマンドで修正前後の確認と全スイートを実行する）:

```
~/workspace/goals/edinet/hidden_files/t3/venv/bin/python ~/workspace/goals/edinet/hidden_files/issue48-review/verify_m1_before_after.py
```

検証スクリプト（`~/workspace/goals/edinet/hidden_files/issue48-review/verify_m1_before_after.py`）は次の順で確認する。(1) codelist.py を修正前の状態へ一時的に戻し、再現スクリプトで BUG REPRODUCED が出ることと、M-1 テスト3件が失敗すること。(2) 修正後の状態へ戻し、再現スクリプトで diff.json UPDATED が出ることと、M-1・S-1 テスト6件が合格すること。(3) 全スイート（381件＋追加6件＝387件）の実行。いずれかが期待どおりでなければ終了コード 1 で止まる。ファイルは常に修正後の状態へ戻す（finally で復元）。途中で止まった場合は `issue48-review/codelist.py.after-m1-fix` が修正後の実物であり、repo の `pipeline/codelist.py` へ上書きすれば復旧できる。

## 4. 残件・未決

- **M-1・S-1 の修正は完了、検証は exec 障害のため未了（2026-11-25 07:4x）**: §3.3 のとおり。検証スクリプトの実行結果を §3.3 へ追記すること。**突然変異の確認は本タスクの範囲外**とし、後続の独立検証が担う。実施役（implementer）では行わない。
- **ブランチ作成・コミット・push・PR 作成（exec 回復後に実施）**: ブランチ名は `issue48-monitor-fix` とする。コミットメッセージは `fix: Issue #48 の監視の誤警報を是正する`（型プレフィックス＋要約72文字以内・末尾句点なし）に Executor トレーラ `Executor: implementer` を付ける。push は gh_api.py 経由で blob→tree→commit→ref 更新の手順で行い、プッシュ直前にリモート head（main `14c9bdc33a39a688ababac6deed32a062422b7f4`）を取得し直して想定値と一致することを確認する。PR 本文は必須3節「目的」「変更範囲」「完了条件」＋「- フロー分岐: コード・1セッション級」＋「チケット: #48」。**マージと Issue #48 のクローズは実施役では行わず、メインが行う**。
- `_latest_due_date` の None 返却（pending 扱い）は、締めの計算上は実用上起きない（`_latest_due_date` の docstring に明記）。`run_monitor` の pending 分岐と各 `_judge_*` の None 対応は防御的な経路として残してある。
- monitor の `last_checked_target` が文字列でない値の場合に TypeError で落ちる件（塊4 の独立検証の観察）は、本チケットの範囲外の未決として残る。
- 夜間の取得がその日すべて失敗・遅延した場合の残余リスク（§1.5 参照）は仕様の精緻化した形のとおり残る。
- 会社マスタの最終適用日より前の日付の差し替えは再適用しない（§1.4 参照）。実測上あり得ないが、24時間超の遅延が起きた場合の穴として記録する。
