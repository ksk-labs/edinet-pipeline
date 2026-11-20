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
  各応答の meta.rows_written の実測値で累計する。推定が実測
  より大きくても、消化は実測で進むため1日の適用総量は予算に
  対して保守側に倒れない。初期投入の推定総量は最大で
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
- workers/serving_api.test.mjs（9件）: meta の応答、companies
  の検索条件の組み立て、404、screen の指標検証と範囲条件、
  不正な limit の 400、GET 以外の 405、events の並び。

## 検証結果

- テストスイート全体: 変更前 331件全件合格 → 変更後 **354件
  全件合格**（`python -m unittest discover -s tests`、CI と
  同じコマンド）。新規は Python 23件（test_serving_sync 16件、
  test_serving_deploy 7件）。
- Worker の JS テスト: `node workers/serving_api.test.mjs` で
  9件全件合格。`node --check workers/serving_api.js` で構文も
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
- 登録簿の改版への追随は列の追加のみ（ALTER TABLE ADD
  COLUMN）。列の削除や型の変更には追随せず、その場合は
  テーブルの再作成が要る。
- Worker の JS テストは CI で回らない。CI への組み込み
  （node のセットアップを tests.yml へ足すか）は別の判断
  事項として残す。
- workers.dev の公開 URL は初回デプロイで確定する。サブ
  ドメインの既定名 edinet-serving が他で取得済みの場合は
  デプロイが失敗し、別の名前の指定が要る。
