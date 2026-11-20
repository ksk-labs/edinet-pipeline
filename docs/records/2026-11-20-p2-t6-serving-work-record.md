# 提供層（D1＋Workers API）の実装（P2-T6）作業記録

作成: 2026-11-20。対象: Issue #21（P2-T6、仕様 #15 §9）。
成果物: pipeline/serving_sync.py、pipeline/serving_deploy.py、
workers/serving_api.js、workers/serving_api.test.mjs、
tests/test_serving_sync.py、tests/test_serving_deploy.py、本記録。

## 目的

Gold と会社マスタの現行値を Cloudflare D1 に載せ、Workers API で
提供する提供層を実装する。設計の正本は P2-T6 設計書（§1〜§6）。
本 PR の完了条件はコードとテストと本記録までとし、D1 への実際の
投入と API の応答確認（実地確認）はマージ後に行う。

## 前提の確認（実データでの検証）

検証は 2026-11-20 時点の v2 データセット現行ファイルで行った
（gold/summary.json: snapshot_date 2026-11-20、generated_at
2026-11-20T02:02:08+00:00。行数は annual 85,247、quarterly
122,702、dividends 81,081、会社 11,827、銘柄 4,493、イベント
113,980、関係 17,443）。

### 主キーの一意性

- companies の internal_id: 一意（11,827件）。
- securities の (internal_id, securities_code): 一意（4,493件）。
  is_primary が真の行は会社あたり最大1件（4,491社に存在）。
- events の event_id: 一意（113,980件）。
- relationships の設計候補キー (parent_internal_id,
  subsidiary_internal_id, relation_kind) には16件の重複（12組）が
  あった。原因は subsidiary_internal_id が NULL の行（Issue #43
  で記録した、索引に提出者として現れない発行会社のみのコード。
  実データでは440行）で、相手先の EDINET コードが異なる行が
  同じキーに潰れるためである。subsidiary_edinet_code を足しても
  親側のコード違いで3件残り、(parent_edinet_code,
  subsidiary_edinet_code, relation_kind) では一意（17,443件）に
  なることを確認したため、この3列を主キーに採用した（方式の
  決定を参照）。
- gold_annual は (internal_id, period_end) では4,865件の重複が
  ある（同一の期間末に複数の期間開始がある。変則決算や期間の
  分割による）。(internal_id, period_start, period_end) で一意
  （85,247件）。
- gold_quarterly は (internal_id, period_start, period_end,
  doc_type) で一意（122,702件）。
- gold_dividends は (internal_id, period_start, period_end,
  securities_code, term_kind) で一意（81,081件）。ただし
  securities_code が NULL の行が4,374件ある。SQLite の複合主
  キーは NULL を含む行の一意性を強制しないため、D1 側の強制は
  データの性質に依存する（残件に明記）。

### 現行ビューの導出

- 名称の kind の実値は company_name（identifier_history の kind
  の内訳: company_name 23,990、corporate_number 14,682、
  company_name_kana 11,474、securities_code 8,295、
  company_name_en 4,357）。
- name の現行値: 全会社に company_name の行があり、設計の規則
  （last_observed_date 最新、同点なら observation_count 最大）
  で未解決の同点は0件だった。
- edinet_code の現行値: 最新スナップショットに複数のコードが
  残る会社が5社あり（内部 ID 3781、4216、4301、8194、9174）、
  いずれも last_seen_date が同点で設計の規則だけでは決まらない。
  決定的な追加規則として「コードリスト由来の接続
  （assignment_basis が initial_v1 以外）を v1 索引由来
  （initial_v1）より優先し、それでも同点ならコードの辞書順で
  最小」を採用し、5社とも一意に決まることを確認した。最新
  スナップショットにコードが残らない会社は364社で、その
  edinet_code は NULL とする。
- gold_annual_latest の最新行: period_end が同点の会社が21社
  あり、period_start が最大の行を最新とする決定的規則を採用
  した（より後に始まった期間を直近の期間として扱う）。

## 方式の決定と根拠

- relationships の主キーは、設計 §4.1 の候補キーで重複が出た
  ため、同節の定め（判別に必要な列を足した複合キーにする）に
  従い (parent_edinet_code, subsidiary_edinet_code,
  relation_kind) に変更した。コードは両側とも全行に存在し、
  内部 ID は導出列として併せて載せる。
- attribute_history.parquet は設計 §4.1 の入力一覧にあるが、
  同節のどのテーブルの導出にも使われていないため、取得対象に
  含めなかった。属性の提供が必要になったら serving_sync の
  INPUT_FILES への追加とテーブルの新設で追随する。
- 書き込み行数の予算管理は、推定と実測を分けた。バッチ寸法と
  停止判定には保守側の推定（主キーと明示インデックスの行数を
  行ごとに加算。events と relationships は4倍、gold 系は2倍、
  securities は3倍、companies は2倍）を使い、予算の消化は
  各応答の meta.rows_written の実測値で累計する。停止判定は
  推定で決まるため、推定が実測より大きい場合は保守側（安全
  側）に倒れ、推定が小さい場合は予算超過側に倒れる。超過幅は
  1バッチ分に限られ、上限 100,000 行に対する1割の余裕で吸収
  する設計の範囲内である。消化の累計自体は実測値で行う（N-1
  による訂正。旧記述「1日の適用総量は予算に対して保守側に
  倒れない」は不正確だったため直した）。初期投入の推定総量は最大で
  1,151,413 行（予算 90,000 行/日で約13日分）。設計書の見込み
  （約50万行・約3日）との差は、D1 が主キーのインデックス行を
  rows_written に数えるかどうかに依存し、初日の実測で確定
  する（未決点）。
- 同期とデプロイの実行は Muse の VM から Cloudflare コネクタ
  （custom.cloudflare）経由で行う（設計 §3 の決定どおり）。
  serving_sync / serving_deploy は実行時に cf_api.py を import
  して資格情報を使い、アカウント ID とデータベース UUID は
  引数で受ける。HTTP 層は差し替え可能にし、単体テストは
  Cloudflare へ接続しない。
- Worker のページングは limit の既定を50、上限を500とした
  （設計に数値の指定が無いための実装判断）。/v1/screen の並び
  は指標の降順（スクリーニングの用途に合わせた）。

## 変更内容

- pipeline/serving_sync.py: 入力 parquet から目標状態を構築し
  （Gold 系は parquet スキーマから列と型を導出、companies は
  現行ビューを導出、gold_annual_latest を派生）、ローカル
  SQLite ミラーとの差分（主キー＋行ハッシュ）を D1 REST の
  query エンドポイントへ予算管理つきで適用する。適用できた分
  だけミラーへ反映し、再開は差分の続きから始まる。適用後に
  D1 側の行数を数えてミラーと照合し、serving_meta（snapshot
  の版情報は全量適用が済んだ場合のみ）を更新する。目標と
  適用済みが一致し差分も無い場合は何もしない（冪等）。
  --dry-run では計画と推定書き込み行数だけを出す。
- pipeline/serving_deploy.py: Workers Scripts API へ multipart
  で Worker を投入し、metadata に main_module と D1 バイン
  ディング（DB）を含める。workers.dev サブドメインが未設定
  なら API で設定し（既定の名前は edinet-serving）、公開 URL
  を返す。
- workers/serving_api.js: 設計 §5 の契約どおりの API
  （/v1/meta、/v1/companies、/v1/companies/{id} と配下の
  annual / quarterly / dividends / events、/v1/screen）。
  すべて GET のみ、応答は JSON で snapshot_date を含める。
  SQL はバインド変数で組み立て、指標名はホワイトリストから
  のみ使う。
- tests/test_serving_sync.py（16件）: 型対応、現行ビューの
  導出（コードと名称の同点規則、主銘柄、latest の同点規則）、
  差分の切り分け、SQL の組み立て（引用符のエスケープ、NULL
  を含むキーの WHERE、100 KB 上限のバッチ分割）、予算台帳の
  累計と永続化、フェイク D1 に対する同期の通し（全量適用と
  冪等な再実行、予算切れでの停止と翌日の再開、更新と削除の
  伝播）。
- tests/test_serving_deploy.py（7件）: metadata の構成、
  multipart の組み立て、サブドメインの取得と設定、デプロイの
  通しと失敗時の異常系。
- workers/serving_api.test.mjs（10件）: meta の応答、companies
  の検索条件の組み立て、404、screen の指標検証と範囲条件、
  不正な limit の 400、GET 以外の 405、events の並び、q の
  LIKE エスケープ（修正で1件追加）。

## 検証結果

- テストスイート全体: 変更前 331件全件合格 → 変更後 **357件
  全件合格**（`python -m unittest discover -s tests`、CI と
  同じコマンド。2026-11-20 の修正後に実測）。新規は Python 23件（test_serving_sync 16件、
  test_serving_deploy 7件）に加え、修正で3件（M-1・S-1・N-3
  の回帰テスト）。
- Worker の JS テスト: `node workers/serving_api.test.mjs` で
  10件全件合格（2026-11-20 の修正後に実測）。`node --check workers/serving_api.js` で構文も
  確認した。tests.yml の CI は Python のみのため、JS テストは
  CI では回らない（残件に明記）。
- 実データでの目標構築: 現行ファイルから目標状態を構築し、
  行数が前提の確認の値と一致すること、主キーで重複が無い
  ことを確認した。dry-run の計画は 442,037 演算、推定書き
  込み行数は最大 1,151,413 行だった。

## 完了条件への対応

- 設計 §6 の定めどおり、本 PR の完了条件はコードとテストと
  本記録まで。D1 への実際の投入（初期投入キャンペーン）と
  API の応答確認は、マージ後にメインとキャンペーン側で
  行う。
- 日次の駆動（Muse 側の cron）は設計 §3 の定めどおり本
  チケットの実装範囲に含めない。

## 修正（2026-11-20、code-review 指摘への対応）

code-review（files/2026-11-20-p2t6-code-review.md）の判定は「修
正が必要」（must-fix 1件、should-fix 1件、note 11件）。対応は
次のとおり。

- M-1（must-fix）: `_mirror_upsert` は挿入前に当該主キーの既存
  行を NULL 安全な条件で削除してから挿入する形に直した。
  SQLite の複合主キーは NULL を含む行の一意性を強制しないため、
  `INSERT OR REPLACE` だけでは NULL キーの行が置換されず旧行が
  残留していた。修正に先立って再現テスト
  （`test_null_key_dividend_update_propagates`）を書き、修正前
  は `count_mismatches` が `{'gold_dividends': {'d1': 1,
  'mirror': 2}}` になることを実測で確認してから修正し、修正後
  は D1・ミラーとも1行で不一致なしになることを確認した。
- S-1（should-fix）: `run_sync` の complete 判定を、行数照合
  の後に移し、`count_mismatches` が空でない場合は complete
  にしない形に直した。不一致の回は snapshot_date と
  gold_generated_at を進めず、applied_snapshot_date も進め
  ない（synced_at と table_counts の記録は残す）。回帰テスト
  （`test_mismatch_blocks_snapshot_advance`）で、不一致時に
  版が進まないことを確認した。修正前のコードでは同テストが
  失敗することも確認した（突然変異）。
- N-3: `batched_insert_sqls` は、単一行だけで 100 KB 上限を
  超える場合に ValueError で明示的に止める形にした。テスト
  （`test_batched_insert_sqls_rejects_oversize_single_row`）
  を追加し、修正前のコードでは同テストが失敗することも確認
  した。
- N-7: `/v1/companies` の `q` は `%`・`_`・バックスラッシュを
  エスケープし、LIKE に `ESCAPE '\'` を付けた。JS テスト
  （`companies の q は % と _ とバックスラッシュをエスケープ
  する`）を追加し、修正前のコードでは同テストが失敗すること
  も確認した。
- N-1: 方式の決定の予算管理の記述を上記のとおり訂正した。

## 修正（2026-11-20、初回同期の SQLITE_TOOBIG への対応）

初回同期の実行で、D1 への最初の INSERT バッチが `D1Error:
status=400 ... 'statement too long: SQLITE_TOOBIG'` で失敗
した。原因は `batched_insert_sqls` のバイト数え間違いで、行
どうしの区切り `", "` は2バイトなのにコードは1バイトとして
数えていた。実測では `companies` の最初のバッチが 1,194行
で実長 101,119 バイトになり、D1 の上限 100,000 バイト（公式
Limits ページで確認済み）を超えた。コード側の計算は 99,926
バイトで上限内と判断していた。差 1,193 バイトは区切り
1,193個分の数え漏れと一致する。

- 修正: 区切り文字のバイト数を 1 → 2 に直した
  （`pipeline/serving_sync.py` の `batched_insert_sqls`）。
- 回帰テスト: `test_batched_insert_sqls_counts_separator_bytes`
  を追加した。2,000行の小さい行でバッチを生成し、各 SQL の
  実長が byte_limit 以下であることを assert する。既存テスト
  は1行が約12,000バイトの行を30行しか使わず、1バッチあた
  り約2行のため数え漏れが1バイトにしかならず、この欠陥を捕
  まえられなかった。
- 初回同期の失敗時点の状態: D1 には DDL のみ適用され、デー
  タ行は0行。ミラーは空で、`sync_state.json` は
  `2026-11-20: 32`（DDL 分の書き込み）。再実行はミラーと
  `sync_state.json` が残っているため、差分の続きから再開で
  きる。D1 側のテーブルを作り直す必要はない。

## 注意点と残件

- 未決: 初期投入の所要日数は初日の実測 rows_written で確定
  する。推定は保守側で最大約13日、設計書の見込みは約3日。
  実測が入った時点で、キャンペーンの運転間隔（設計では
  10:00 と 21:50 JST の1日2回）の妥当性をメインが判断する。
- gold_dividends の主キーは NULL を含むため、D1（SQLite）
  側では一意性が強制されない。データ側で一意であることは
  本記録の検証で確認済みで、同期の差分判定はミラー側の
  Python のキーで行うため同期の正確さには影響しない。将来
  Gold の生成側で重複が出ても D1 は止めない点に注意する。
  ミラー側の NULL キー行の更新残留は M-1 の修正で解消済み
  （修正後は delete＋insert で置換する）。
- 予算切れで停止した回でも `_upsert_meta`（synced_at、
  table_counts）が数行の書き込みを予算外で消費する（N-2）。
  実害は小さいが、予算を完全に使い切った状態では上限側の余
  裕を数行削る。現状のまま運用し、予算の消化が上限に接近す
  る場合の見直し項目とする。
- ミラーを作り直す場合は D1 側のテーブルも作り直す必要があ
  る（N-4）。ミラーだけを作り直して D1 に既存行が残っている
  状態で同期すると、初回 INSERT が主キー衝突でエラーになり
  止まる。初期投入キャンペーンの開始前に、ミラーを作り直す
  手順がある場合は D1 側も作り直すことを確認すること。
- screen の指標ホワイトリスト（`workers/serving_api.js` の
  `INDICATORS`）は登録簿 v4 の19指標をハードコードしたもの
  で、登録簿の改版で指標が増えた場合の追随は手作業である
  （N-5）。同期側の列追加は自動で追随するが、API 側は追随
  漏れで新指標が screen で 400 になる。登録簿の改版時は、
  API のホワイトリストも合わせて更新することを確認項目とす
  る。
- `/v1/companies` の `code` は EDINET コードと主銘柄の証券
  コードに当てる。種類株など主銘柄以外の証券コードでは引け
  ない（N-6）。設計の定めの範囲内だが、利用上の限界として
  ここに記録する。
- `pipeline/serving_deploy.py` はスクリプトの投入と workers.dev
  サブドメインの設定までで、スクリプト側の workers.dev ルート
  の有効化は行っていない（N-8）。初回デプロイで公開 URL が応
  答しない場合は、この有効化が要る可能性がある。実地確認
  （マージ後）の確認点として申し送る。
- Worker の JS テストは CI で回らない。CI への組み込み
  （node のセットアップを tests.yml へ足すか）は別の判断
  事項として残す（N-9）。提供層の運用が始まる前に判断を先送
  りしないこと。
- 既存テーブルの主キー変更には追随しない（N-10）。`_d1_ensure_schema`
  は `CREATE TABLE IF NOT EXISTS` と列追加のみで、主キー定義
  の変更にはテーブルの再作成が要る。列の削除・型の変更と同種
  の限界として位置づける。
- `limit=0` は受け付け、空の items と total を返す（N-11）。
  害はないため現状のままとする。
- 登録簿の改版への追随は列の追加のみ（ALTER TABLE ADD
  COLUMN）。列の削除や型の変更には追随せず、その場合は
  テーブルの再作成が要る。
- Worker の JS テストは CI で回らない。CI への組み込み
  （node のセットアップを tests.yml へ足すか）は別の判断
  事項として残す。
- workers.dev の公開 URL は初回デプロイで確定する。サブ
  ドメインの既定名 edinet-serving が他で取得済みの場合は
  デプロイが失敗し、別の名前の指定が要る。
