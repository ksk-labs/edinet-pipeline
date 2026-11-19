# 工程2 P2-T7 テキスト層の器 — 作業記録（設計と実施結果）

対象: Issue #22（P2-T7: テキスト層の器（原文保持の格納、代表者の観測の受け皿））
仕様: Issue #15 §6（工程2仕様「6. テキスト層の器」）。対応する合格条件は合格条件7
（テキスト層の器に原文が保持され、代表者の観測が会社マスタへ渡ることが確認できる）。

## 範囲

- テキスト層の「器」を作る: 原文の保持と格納の形、代表者の観測の受け皿、源泉非依存の取り込み口。
- 対象は仕様 §6 の7項目: 代表者、種類株式の内容、事業の内容・リスク、沿革・変遷、セグメントの説明、株主優待の記述、関係会社の状況。
- 抽出と構造化の本格実装（要素の分類の網羅、文章からの構造化抽出、全書類への一括・日次取り込み）は範囲外であり、別工程（テキスト層工程）とする。
- 会社マスタ（company_master.py）への変更は行わない。代表者の観測は、属性履歴へ「渡せる形」にするところまでを本チケットとする。

## 実測（実書類での確認、2026-11-19）

標本は実書類 S100ZI7X（株式会社ビックカメラ、EDINET コード E03481、有価証券報告書－第46期、2026-11-17 10:59 提出。v2 データセットの raw/ から取得した提出 zip）。

- arelle（2.47.5、parse.py と同じ抽出経路）で読んだ非数値事実は423件（数値事実1,343件、nil 148件）。Silver が捨てている文字事実の実量がこの規模であることを確認した。
- 代表者の観測源は表紙の要素 `jpcrp_cor:TitleAndNameOfRepresentativeCoverPage` であり、値は「代表取締役社長　秋保　徹」（役職＋全角空白＋氏名。氏名内部にも全角空白を含む）だった。DEI（jpdei_cor）側には代表者の要素は無かった。
- 7項目のうち EDINET 有報タクソノミ（jpcrp_cor）に対応要素を実測できたのは6項目: 種類株式の内容=`ClassesOfSharesEtcTextBlock`、事業の内容=`DescriptionOfBusinessTextBlock`、事業等のリスク=`BusinessRisksTextBlock`、沿革=`CompanyHistoryTextBlock`、セグメント=`DescriptionOfReportableSegmentsTextBlock` と `NotesSegmentInformationEtcConsolidatedFinancialStatementsTextBlock`、関係会社の状況=`OverviewOfAffiliatedEntitiesTextBlock`。
- 株主優待には定型要素が無かった（grill の確定事項どおり、文章・表が源泉であり主たる源泉は TDnet）。
- テキストブロックの原文は XHTML（見出し・段落のマークアップと改行を含む文字列）として事実の値に入っている。

## 設計判断と根拠

1. **格納先は v2 データセットに新しい接頭辞 `text/` を切る**（items.parquet、representative_observations.parquet、summary.json）。
   根拠: テキスト層は Silver（数値事実）・Gold（指標）・master/（会社マスタ）と並ぶ独立した層であり、既存の接頭辞はどれも意味が異なる。会社マスタに従属させず独立させることで、研究用途のアクセス面（HF 上の parquet）としても Silver と対の形になる。
2. **items は1行=1文字事実とし、原文（raw_text）を加工せず保持する。**
   あわせて文脈（context_start / context_end / dimension）を Silver と同じ文字列形で保持し、事実の同一性を再現できる形にする。同一書類内の完全な重複（要素・文脈・次元・原文がすべて同一）は1行に畳む（parse.py の数値事実の重複除去と同じ規則）。text_id は（源泉種別、源泉書類 ID、要素、文脈、次元、原文の sha256）の sha256 で決定的に生成する。
3. **正規化した要約は normalized_summary 列に併記できる形にし、取り込み時点では null とする。**
   根拠: 仕様 §6 の「原文を保持したうえで、正規化した要約を併記できる形」を器の形として満たす。要約の生成そのものは本格実装の範囲。
4. **抽出の確度は certainty 列（confirmed / unconfirmed）で明示する。**
   XBRL の文字事実は提出者の記述そのものであるため confirmed とする。確度の低い抽出（将来の PDF 由来など）は取り込み口のレコードで unconfirmed を明示できる。代表者の観測では、役職・氏名へ分解できない場合に観測側の certainty を unconfirmed に倒す（下記6）。
5. **カテゴリは7項目の語彙を定数で持ち、要素→カテゴリの対応は実測した初期対応のみとする。**
   対応表に無い要素は category=null（未分類）のまま原文を格納する。根拠: 分類の網羅は本格実装の範囲であり、器の段階で対応表を推測で広げると誤分類を固定化する。実測で裏の取れた要素だけを初期対応とする。レコード側がカテゴリを明示した場合はそれを優先する（株主優待のように要素の定型が無い項目はこの経路で入る）。
6. **代表者の観測は専用の受け皿（representative_observations）を作り、会社マスタの属性履歴へ渡せる列構成にする。**
   観測は items のうちカテゴリが representative の行から作る。役職・氏名の分解規則は「最初の空白（全角・半角を含む）までを役職、残りを氏名」とする決定的なものとし、氏名内部の空白は保持する。空白が無く分解できない場合は role / name を null とし、certainty=unconfirmed（未確定）として残す。
   受け渡しの形は `to_attribute_rows` が担う: 観測を company_master の ATTRIBUTE_SCHEMA と同じキー構成の行（attribute="representative"、value="役職 氏名"、first/last_observed_date、observation_count、出所）へ変換する。変換の規則は会社マスタの属性履歴の開閉の規則に合わせ、同じ値の観測が続けば1行にまとめ、値が変われば別の行にする。未確定の観測と内部 ID が解決できない観測は属性へ渡さない。
   根拠: 会社マスタの属性は「有効期間つきの行」の仕組みで統一されており（仕様 §1）、代表者も同じ仕組みに矛盾なく載ることが §1 の設計である。本チケットでは company_master 側の取り込み配線は行わず、変換後の行が ATTRIBUTE_SCHEMA でそのまま検証できること（テストで固定）をもって「渡せる形」とする。配線は会社マスタの日次更新の後続作業で行う。
7. **取り込み口は源泉非依存の文字事実レコード（JSONL）とする。**
   レコードは source_kind と source_doc_id で書類を特定し、EDINET XBRL / TDnet XBRL / PDF 抽出のいずれからでも同じ形で入る（TDnet 前提の設計点 (b)(d) に対応）。EDINET 用の抽出（extract）は arelle で提出 zip からこのレコード列を作るアダプタとして分離し、抽出の固定事項（終端は endDate、FormulaOptions の設定、開示制度による読込遮断の無効化）は parse.py の実装を再利用する。
8. **全行に出所（源泉の種別、源泉側の識別子、取得日時、再配布可否）を持たせる（仕様の原則）。**
   再配布可否はレコード側の値を保持し、欠けた場合は unknown とする（可否が無い源泉を公開層へ出さない原則に合わせ、許可側へ倒さない）。EDINET 抽出では allowed を明示する。
9. **株主優待のイベントの器は本チケットでは作らない。**
   根拠: イベントの種別（shareholder_benefit_established / changed / abolished）は P2-T4 で company_events の RESERVED_EVENT_KINDS に予約済みであり、器は既に存在する。テキスト層側は、カテゴリ shareholder_benefits の原文保持で受ける。

## 実施結果（ローカル実行、2026-11-19）

- 実装: `pipeline/text_layer.py`（extract / build の2サブコマンド）、`tests/test_text_layer.py`（新規24件）、`.github/workflows/text_layer.yml`（workflow_dispatch のみ。1書類分の extract→build→配置の運用口）。
- テスト: 新規24件すべて合格。全スイートは289件すべて合格（従来265件＋新規24件）。
  固定した内容: 原文（XHTML を含む文字列）が加工されず保持されること、実測した要素→カテゴリの対応、重複の畳み込み、未分類のままの格納、未確定と要約の保持、明示カテゴリの優先、代表者の分解（全角・半角・分解不能）、観測から属性履歴の形への変換が ATTRIBUTE_SCHEMA で検証できること、同じ値の観測の統合と観測回数、値の変化での行の分離、未確定の観測を渡さないこと、build の成果物（parquet 2点と summary.json）の読み戻し。
- 実データでの実地確認（S100ZI7X、アップロードなし）: extract で文字事実423件を取得し、build で items 423行を構築した。カテゴリ内訳: representative 1、class_shares 1、business_and_risks 2、history 1、segments 2、affiliated_entities 1、未分類415。代表者の観測は1件で、役職「代表取締役社長」・氏名「秋保　徹」に分解され、内部 ID は実物の会社マスタで 2318 に解決し、certainty=confirmed だった。事業の内容の原文は XHTML のまま保持された。

## 配置について

- 本 PR では HF への配置（アップロード）は行わない。配置はマージ後に text_layer ワークフローの手動 dispatch、または `python -m pipeline.text_layer build --upload` で行う。
- build は入力全部からの冪等な置き換えであり、再実行で同じ入力からは同じ成果物になる（generated_at を除く）。

## 残件・限界

- 要素→カテゴリの対応は有報（jpcrp_cor）の実測した初期対応のみ。四半報・半期報等の様式や、提出者拡張要素の分類は本格実装の範囲。現状では大半の文字事実が未分類のまま格納される（実測では423件中415件）。
- 代表者の観測源は有報の表紙の要素のみ。臨時報告書の代表者異動イベントからの観測（仕様 §1 のもう一方の観測源）は、イベント自体は company_events に取り込み済みだが、観測としての受け渡しは未配線。
- 会社マスタへの実際の取り込み（attribute_history への反映と日次更新への配線）は後続作業。器の側は to_attribute_rows で受け渡しの形を用意した。
- 全書類への一括取り込みと日次差分の配線はテキスト層の本格実装（別工程）の範囲。本 PR のワークフローは1書類分の手動経路のみ。
- 株主優待は、EDINET 側に定型要素が無いため、本器では明示カテゴリつきレコードでのみ取り込める。TDnet フェーズで源泉が付く。

## code-review への対応（2026-11-19、should-fix 2件）

レビュー報告書（files/2026-11-19-p2t7-code-review.md）の should-fix 2件に対応した。

- **S-1（certainty の既定値を安全側へ）**: `build_items` は、レコードの certainty が無い・語彙外の場合に confirmed へ倒していた。これを unconfirmed へ変更し、再配布可否の既定（欠けたら unknown、許可側へ倒さない。設計判断8）と同じ安全側の向きに揃えた。取り込み口は源泉非依存であり、確度を明示しない源泉のレコードが「確定」として格納され、代表者の観測を通じて属性履歴へ渡りうることを避けるための変更である。
  - 回帰テスト3件を追加した（tests/test_text_layer.py）: certainty 欠落→unconfirmed、語彙外の値→unconfirmed、明示した confirmed はそのまま保持されること。修正前コードでは欠落・語彙外の2件が失敗すること（'confirmed' != 'unconfirmed'）を確認したうえで修正した。
  - EDINET 経路の実データの挙動は変わらない: EDINET 用の抽出（extract_text_facts）は生成する全レコードに certainty=confirmed を明示しており（設計判断4のとおり XBRL の文字事実は提出者の記述そのもの）、既定値に依存するレコードは存在しない。
- **S-2（ワークフローの置き換え仕様の明記）**: `.github/workflows/text_layer.yml` のヘッダに、dispatch のたびに text/ は当該書類のみの内容で置き換わり、別の書類で再度 dispatch すると先の書類の items は消える（蓄積する経路ではない）ことを明記した。build が入力全部からの全置き換えであることは「配置について」に既記であり、運用口の説明にも同じ前提を書いた。
- テスト: 全スイート292件すべて合格（従来289件＋新規3件）。
