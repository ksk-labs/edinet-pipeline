# Issue #25 残件（過去分の表示名の辞書構築）作業記録

- 対象: Issue #25「P2-T10: 拡張タクソノミの表示名の要素辞書」の残件
  （バックフィル期間の書類に現れる拡張要素の表示名を辞書へ追加する）。
- 仕様: Issue #15（工程2仕様）§6・§10。P2-T10 本体は PR #37 で着地済み
  （dictionary/extension_elements.parquet 216行）。
- 実施範囲: 実装と PR 作成まで。マージ・dispatch は行わない。

## 既存の仕組みの確認（実物）

- `pipeline/extension_dict.py`: 辞書構築器。入力は
  `parse/month=<月>/extension_labels.jsonl`（1行 = 1観測
  (doc_id, element, element_ns)、表示名つき）と書類目録
  `catalog/month=<月>/ledger.jsonl`（出所の併記用）。目録に無い書類は
  出所を空にして `docs_without_catalog` に数える。sync は v2 の
  parse/ と catalog/ を落として全体を再構築し dictionary/ へ置き換える。
- `pipeline/parse.py` の `extract_facts(cntlr, zip_path)`: arelle で
  提出 zip から数値事実と拡張要素の表示名 (labels) を取る。ラベル行の
  形式は {element, element_ns, label_ja, label_en} で、書類単位の処理
  (process_document) が doc_id を付けて月別の extension_labels.jsonl に
  分離する。拡張の判定は名前空間 URI に `/taxonomy/` を含まないこと
  (`_is_extension_ns`)。固定事項（終端は endDate、FormulaOptions の
  設定、開示制度による遮断の無効化）は `_new_cntlr` と抽出側に閉じている。
- `parse.run_merge` は月別 extension_labels.jsonl を既存リモートと
  (doc_id, element) で重複除去してマージする（既存が勝つ）。過去分の
  ラベルを同じ場所へ置けば、辞書の sync/build がそのまま拾う。
- 現行の辞書は 216行（2026-11-17 構築、対象月は 2026-11 のみ）。
  一方 extension_labels.jsonl は5か月分（2024-11、2024-12、2025-11、
  2026-03、2026-11）で合計 1,967行まで増えている。辞書の再構築
  （extension_dict sync）は本残件の範囲外だが、過去分のラベルが
  入った後の sync で過去分も辞書へ入る。

## 母数の測定（2026-11-19 実測）

- v2 の Silver 全129か月（2016-03〜2026-11）の (書類, 要素) の組のうち、
  標準タクソノミの要素キーに無い組は 737,968 組 / 130,411 書類。
  既存の extension_labels でカバー済みの組を除くと **737,224 組 /
  130,285 書類** が未カバー（本残件の母数）。
- 接頭辞の内訳: 5,414 種のうち 5,411 種が提出者・ファンドのコード形
  （`E#####-000` / `G#####-000`）で 736,658 組（99.8%）。残りは
  ifrs-full（809組・標準の IFRS タクソノミ）、cte（486組）、
  asr（15組）の3種のみ。
- 標準要素キーの集合は、金融庁の一覧ページから取得できる版
  （2018〜2026年版、10公表分）の和集合でも 2026年版単独でも 11,129
  キーで一致した（要素キーは版をまたいで削除されないことを実測）。
  `pipeline.taxonomy.parse_taxonomy_zip` が作るキー集合は、v2 に
  保存済みの edition=2026 の elements.jsonl と完全一致した。
- 計画段階の対象切り分けに接頭辞や書類種別は使わない。拡張かどうかは
  名前空間 URI が決めるため、抽出時の arelle の判定を真値とし、
  標準名前空間だった要素は結果の分類で in_facts_standard として数える。

## 経路の決定と根拠

原形（提出 zip）の取得経路は **v1 データセット
（Yoshi-Dai/financial-lakehouse）の日次 tar** とした。

- v2 の raw/ にある原形は S2-T5 の取得分（2026-11 以降）しかなく、
  バックフィル期間を賄えない。
- v1 の日次 tar はバックフィル（S2-T2/S2-T4）が Silver の入力に
  使ったものと同一で、固定リビジョン
  `43540407ea82f792551e145598b1cf50cde6cb78` と索引の SHA-256 照合で
  再現性を担保する（backfill.yml と同じ型）。v1 へは読み取りのみ。
- 抽出は新しい抽出器を作らず `pipeline.parse.extract_facts` と
  `parse._new_cntlr` を再利用する（日次経路と同じ判定・同じ固定事項）。
- 全量 130,285 書類は1回の実行では終わらない（ローカル実測では
  1書類あたり平均で数秒、重い書類では数十秒から数分。後述）。
  月単位＋ `--max-docs` で分割し、台帳
  （dictionary/backfill/month=<月>/ledger.jsonl）で再開可能にした。
  ワークフロー `.github/workflows/extension_backfill.yml` から
  月ごとに dispatch して進める運用とする。残りの月の処理は
  本 PR のマージ後の運用であり、本作業記録の時点で未実施である
  ことを明記する（範囲を黙って狭めない）。

## 出所の置き場所の決定

- ラベル行は `parse/month=<月>/extension_labels.jsonl` へ既存行優先で
  マージする（parse.run_merge と同じ規則）。
- 出所は v1 索引の行から catalog 形式に合成し、
  `dictionary/backfill/month=<月>/catalog.jsonl` に置く。
  catalog/ 本体には入れない。catalog は discovery の観測台帳であり、
  合成行を混ぜると会社マスタ（company_master の last_submissions）や
  監視（monitor の件数信号）が他チケットの範囲外で変わってしまうため。
  合成行は source="v1_index"、fetched_at は索引の ope_date_time
  （実測ではほぼ空）とする。extension_dict の catalog_index は
  fetched_at の大きい行を優先するため、実観測の行がある書類では
  合成行は使われない。sync には dictionary/backfill 側の台帳も
  併せて読む最小の拡張を加えた。

## 実装

- `pipeline/extension_backfill.py`（新規）:
  - targets: Silver と標準要素キー（金融庁の一覧ページの全版の和集合、
    作業ディレクトリにキャッシュ）と既存ラベルから対象を算出する。
  - run: 月単位で、日次 tar を固定リビジョンから取得して書類ごとに
    extract_facts を実行し、結果を resolved_text / no_label_text /
    in_facts_standard / not_in_facts に分類して台帳へ記録する。
    ok / quarantined は完了として再実行で飛ばし、failed は再試行する。
    tar は日単位で取得・削除するため中断しても日単位で再開できる。
    アップロード時は日単位でラベル・台帳・出所・要約を公開する。
  - 索引は使う前に SHA-256 を照合する（`--skip-index-hash-check` で
    意図的な別版のみ省略可）。
- `pipeline/extension_dict.py`: sync が dictionary/backfill の
  出所台帳も読むよう拡張（上記）。
- `.github/workflows/extension_backfill.yml`（新規）: backfill.yml の型
  （固定リビジョン索引＋SHA 照合、arelle 固定版の導入）。書き込みの
  競合を避ける concurrency グループつき。
- テスト: `tests/test_extension_backfill.py`（新規9件）と
  `tests/test_extension_dict.py` へ sync の backfill 出所読み取りの
  テスト1件を追加。全テスト 323 件合格（従来 313 件＋新規 10 件）。

## 実データでの構築結果（ローカル、アップロードなし）

対象月は2か月を選んだ。2016-04 は最も古い期の代表（arelle が
2016年版タクソノミの書類を読めるかの確認）、2021-10 は第2期の
最終月でファンド書類を含む代表である。

### 2016-04（完走）

- 対象 199 書類 / 795 組。結果は ok 199・隔離 0・失敗 0。
- 対象の解決: **795 組すべてが表示名（日英）つきで解決
  （resolved_text 795、未解決 0）**。
- 追加ラベル行は 1,400 行（対象組 795 のほか、書類の DTS に現れる
  拡張要素のうち Silver の数値事実には無いものを含む。日次経路の
  process_document と同じ出力規則）。
- 出所行は 199 行（対象書類すべてに v1 索引の合成行が付いた）。
- 所要時間は約19分（199 書類、日次 tar の取得と標準要素キーの
  取得を含む）。1書類あたり平均で数秒だが、重い書類では数十秒に
  なることがある。
- 冪等性の実測: 完走後に同じ作業ディレクトリで再実行
  （`--max-docs 0` で集計のみ）したところ、統合後のラベルファイルと
  出所ファイルの SHA-256 が初回と一致した（バイト同一）。

### 2021-10（一部実施）

- 対象 480 書類 / 2,182 組のうち、37 書類を処理した時点で区切った。
  1書類の処理が数分間前進しなかったため（arelle の読み込み待ちと
  見られる）で、残りは台帳から再開できることを実データで確認した
  うえで停止した。
- 処理済み 37 書類の対象 193 組は **すべて表示名つきで解決
  （未解決 0）**。追加ラベル行は 302 行。
- 停止後に `--max-docs 0` で再開し、部分結果の統合・出所・要約の
  出力まで完走した（summary の pending_docs は 443 で、残りが
  未処理であることが記録に残る形）。

### 合計（実測分）

- 処理した対象は 988 組（795 + 193）で、未解決は 0 組。
- 追加ラベル行は合計 1,702 行（1,400 + 302）。

## 現行216行との照合

- 対象の算出で既存ラベル（全月 1,967行）の (doc_id, element) を
  カバー済みとして除外しており、統合は既存行優先のため、既存行が
  書き換わることはない。今回の2か月の対象は既存ラベルのある月
  （2024-11 等）とは重ならない。
- 辞書本体（parquet）の再構築は extension_dict sync の仕事であり
  本残件では実行しない。ラベルが入った状態で sync を実行すれば、
  過去分の行は出所（source="v1_index"）つきで辞書へ入る。

## 残りの扱い

- 残りは 2021-10 の未処理 443 書類と、その他の127か月である。
  本 PR のマージ後に extension_backfill ワークフローを月ごとに
  dispatch して処理する。2021-10 は v2 側の台帳へ今回のローカル
  台帳を引き継がないため最初から処理し直す（ローカル実行は
  アップロードしていない。処理は冪等であり、やり直しのコストは
  37 書類分に留まる）。1か月の書類数は最大でも約2,700書類
  （2026-06）で、350分のジョブ上限に対して分割用の `--max-docs`
  と台帳による再開を用意してある。重い書類でジョブが止まった
  場合は、その書類を台帳の failed として残す運用ではなく、
  ジョブ自体が台帳の続きから再開する形になる。
- 未解決の内訳のうち in_facts_standard は標準名前空間の要素であり
  辞書の対象外（異常ではない）。not_in_facts は Silver（自前変換器
  由来）と arelle 抽出の差異であり、件数は月ごとの summary と
  台帳に理由つきで残る。
