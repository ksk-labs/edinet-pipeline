# Issue #41 作業記録: 会社マスタ update と last_submissions の日次配線

対象: Issue #41（P2-T3 後続）。仕様は Issue #15（工程2仕様）。

## 方式の決定と根拠

### 日次実行の方式: daily_codelist への相乗りを採用

daily_codelist.yml に `company-master-update` ジョブを追加し、
`needs: codelist` で同一 run の後段に置いた。company_master.yml へ
schedule を追加する案は採らなかった。根拠は次のとおり。

1. **順序の保証**: needs で同一 run の後段に置くため、スケジューラ
   遅延の影響を受けず、その run で保存されたスナップショットが
   そのまま update に適用される。完了条件は「スナップショットが
   増えた翌日までに update が自動で適用されること」であり、これに
   対して最も強い形である。
2. **schedule 案の弱さ**: GitHub Actions のスケジューラ遅延は
   ワークフローごとにばらつく（2026-11-17〜18 の実測で 6〜10時間、
   ワークフローごとに異なる）。company_master.yml に独立した
   schedule を置くと、遅延の組み合わせによっては codelist より先に
   走り、当日分を取りこぼす可能性がある。update は未適用分を
   まとめて適用するため翌日には追いつくが、順序の保証は弱い。
3. **既存の配線との整合**: company_master.yml 自身のヘッダコメントが
   「日次の定期実行への配線 (daily_codelist への相乗り)」を後続作業
   として明記していた。
4. **2回/日の実行について**: codelist は 09:15 と 15:15 JST の2回
   run するため、update も2回走る。update は未適用のスナップ
   ショットだけを適用する冪等な処理であり、2回目は実質的に何も
   しない。コストよりも順序の保証を優先した。

### 手動 dispatch との競合防止

concurrency グループはリポジトリ全体で有効である（GitHub 公式
ドキュメント "Control the concurrency of workflows and jobs"。
グループ名がワークフローをまたいで作用する）。このため、両方の
ワークフローのジョブに同名グループ `company-master-write` を付け、
次の設定で直列化した。

- `cancel-in-progress: false`: 実行中のジョブは止めない。
- `queue: max`: pending のジョブが後続の到着で取り消されない
  ようにする。既定の `queue: single` では pending が新しい到着に
  取り消されるため、手動 dispatch が静かに消える恐れがある。

### last_submissions の日次更新

- 入力は v2 の `catalog/`（S2-T5 の取得経路が保存した書類目録）のみ。
  EDINET への再取得はしない（Issue の「二重取得にしない」に従う）。
- 集計は書類単位（docID）で一意化してから（EDINET コード、書類
  種別）ごとに数える。目録は観測の追記であり、同じ書類が複数回
  現れるためである。
- 最終提出日は最大値で単調に更新する。件数は、現在の最終提出日
  より後の提出日の書類だけを加算する。初期値（v1 索引の集計）に
  は基準日以前の書類が既に含まれており、それ以前を加算すると
  二重数えになるためである。代わりに、基準日と同日の別書類は
  数え落とすことがある（派生属性の近似として許容し、コード内の
  説明にも明記した）。同じ集計での再実行は冪等である。
- マスタに無いコードの書類は取り込まず、件数を summary の統計
  （unknown_codes / unknown_documents）に残す。

### monitor の点検追加

- 日次報告に `company_master` 節を追加し、会社数と最終提出日の
  行数を件数信号（master_companies、master_last_submissions_rows）
  に載せた。
- 適用済みスナップショットが v2 上の最新スナップショットに
  追いついていない場合は警報 `company_master_stale` にする。
- master/summary.json が無い場合は missing として節と信号に
  載せるが、警報にはしない。警報の対象は仕様 §5（取得・コード
  リスト・解析）の範囲であり、会社マスタの有無は節と信号で
  可視化する、という線引きである。

## 変更内容

- `.github/workflows/daily_codelist.yml`: `company-master-update`
  ジョブ（needs: codelist、concurrency 共有）を追加。ヘッダ
  コメントに配線の説明を追加。
- `.github/workflows/company_master.yml`: ジョブへ concurrency
  （同名グループ）を追加。ヘッダコメントを配線済みの記述へ更新。
- `pipeline/company_master.py`:
  - `aggregate_catalog_submissions`（目録の集計）と
    `refresh_last_submissions`（冪等な更新）を追加。
  - `download_catalog_rows` / `load_catalog_rows` を追加し、
    `run_update` がスナップショット適用に続けて最終提出日を
    更新するようにした。CLI に `--catalog-dir` を追加。
  - summary に `last_submissions_rows` と
    `last_submissions_refresh`（更新の統計）を追加。
- `pipeline/monitor.py`: `company_master` 節、件数信号2件、警報
  `company_master_stale` を追加。
- `tests/test_company_master.py`: 集計・更新・run_update の統合
  テスト 8件を追加。
- `tests/test_monitor.py`: 会社マスタの点検テスト 3件を追加。

## 検証

- 全テスト 223件合格（変更前は 212件、新規 11件）。
- HF を介さないローカル実行で CLI 経路を確認した: 状態と
  スナップショットと目録をローカルに用意し、`update` で
  スナップショットの適用と、重複観測を一意化したうえでの
  last_submissions 2行の生成を確認した。
- ワークフロー YAML 2本の構文と concurrency の設定値を読み戻して
  確認した。

## レビュー指摘への修正（2026-11-19、code-review の should-fix 2件）

対象レビュー: `2026-11-19-issue41-code-review.md`（最終見解は
マージ可、must-fix 0件、should-fix 2件）。

### S-1: run_update の結合の向き（目録側の失敗の分離）

- `run_update` で書類目録の読み取り・集計・反映
  （`download_catalog_rows` → `aggregate_catalog_submissions` →
  `refresh_last_submissions`）を失敗分離した。目録側が失敗しても
  例外は上に伝わらず、スナップショットの適用と状態の保存は
  そのまま完結する。
- 失敗時は `last_submissions` を反映前の状態へ戻し、失敗の内容を
  summary の `last_submissions_refresh` に `error_kind` と
  `error` として記録する（統計の各件数は 0）。S2-T5 塊3レビューの
  S-1 と同型の直し方である。
- 回帰テスト 2件を追加した（`RunUpdateCatalogTest`）: 目録の
  読み取りが例外を投げる場合と、目録の行が壊れていて集計が
  例外を投げる場合のどちらでも、スナップショットが適用され、
  summary に失敗が記録され、保存された `last_submissions` が
  変更されていないことを固定する。修正前のコードではこの
  2件が失敗することを確認した。

### S-2: 一意化のタイブレーク規則のテスト固定

- `aggregate_catalog_submissions` の「同一 docID で提出日が
  食い違う場合は新しい方を採る」規則を固定するテストを 1件
  追加した（`AggregateCatalogSubmissionsTest`）。新しい日付が
  先に現れる順序と後に現れる順序の両方を含み、どちらの順序
  でも新しい方が残ることを確認する。
- 突然変異（一意化を常に後勝ちの上書きへ変える）でこのテスト
  が失敗することを確認した。変異の下では他の集計テスト 2件は
  合格のままであり、タイブレークを縛っているのはこのテスト
  である。

### 検証（修正分）

- 修正前（head b8700b5c）: 全テスト 223件合格。新規テスト 3件を
  先に加えた状態では 226件中 S-1 の回帰テスト 2件が失敗した。
- 修正後: 全テスト 226件合格（223件 + 新規 3件）。

## 残手順（マージ後、メイン側）

- daily_codelist を dispatch し、company-master-update ジョブが
  codelist の後段で success になることを確認する。
- HF 実物で master/summary.json の last_applied_snapshot_date が
  当日のスナップショットに一致すること、monitor の日次報告に
  company_master 節と件数信号が載ることを確認する。
