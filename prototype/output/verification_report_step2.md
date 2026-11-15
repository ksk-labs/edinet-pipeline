# Step 2 塊1 照合報告 — 修正後 Silver と XBRL 原文の決定的照合

対象: 修正後の Silver（`prototype/output/silver_sample.parquet`、SHA-256 `a0bef894c7c13ab5fe7497f837f255a1544d5cb421f0b1d7185acbbc4819e8e9`、21,189行・29書類。Step 1 版は22,194行）。
方法: convert.py は使わず、Python 標準ライブラリの ElementTree で標本29書類の .xbrl インスタンスと Inline XBRL（.htm）の両方から数値の事実を独立に再抽出し、書類ごとに（要素名・期間開始・期間終了・値）の多重集合で突き合わせた。比較の前提は仕様どおり: xsi:nil の事実は値なしとして除外し、同一（要素・context・単位・値）の完全な重複事実は1件に畳み、Inline XBRL の sign="-" は負値として扱い、要素名は提出者拡張の基底（提出者の EDINET コード。名前空間末尾の提出日は訂正前後で変わるため同一視）に正規化した。context の同一性は、解決済み context（期間＋次元）と contextRef の ID の2通りで畳んで確認し、結果は両者で同一だった。

## 全体の結果

- 原文から再抽出した数値の事実: 延べ44,386件（.xbrl 22,194件＋Inline XBRL 22,192件）。重複を畳むと21,079件。xsi:nil として除外した事実は5,730件。
- **一致: 21,079件 — 抽出した事実は全件が Silver に存在した（Silver に無い: 0件）**
- **値が違う: 0件**
- **Silver に余分: 110件 — 全件が同一事実の重複行（次節）**
- 一致率: 事実ベース（抽出側）で100%。Silver の行ベースでは 21,079 / 21,189 = 99.48%。

Inline XBRL 側で単純な数値化ができなかった事実が2件あった（S100YN5G の EPS、本文が「87円56銭」「169円44銭」の円銭表記）。同一事実は .xbrl 側に数値（87.56、169.44）で存在し、照合の対象に含まれているため、欠落にはなっていない。

## Silver に余分な110件の内訳（同一事実の重複行）

Silver には、同一の（書類・要素・期間・次元・単位・値）の行が2件ずつ存在するグループが110組あり、余分な行はその2件目で全て説明が付く。値そのものが誤っている行は無い。重複の類型は2つ:

1. **値0の事実の (-0.0, 0.0) ペア: 101組。** Inline XBRL で sign="-"・本文 "0" の事実が -0.0、.xbrl 側の同じ事実が 0.0 となり、変換側の重複排除が数値としては等しい両者を別物として残した。例: S100X8TY（LIFULL）の `jpigp_cor:PurchaseOfTreasurySharesSSIFRS`。
2. **非ゼロの同一値ペア: 9組。** Inline XBRL の本文が小数表記（例: "802.0"、scale="6"）で、文字列の形が .xbrl 側の "802000000" と異なるため、文字列形に依存する重複排除をすり抜けた。内訳は S100YC5C（野村ホールディングス）の役員一人ごとの報酬8件と、S100YG5L（オリックス）の GHG 排出量1件。

書類別の余分件数: S100X8TY 21、S100XD4M 21、S100XCU9 16、S100XZ2X 16、S100Z5AL 13、S100YC5C 8、S100ZCDP 8、S100ZH4D 5、S100YG5L 1、S100YG81 1。

申し送り: 本実装の重複排除は、値の文字列形ではなく数値（符号付きゼロを 0 に正規化した値）をキーにすべきである。Gold 生成で同一事実が二重に数えられる経路が無いかも、Gold 側の確認対象とする。

## 書類ごとの結果

| 書類ID | セル | 原文の事実（重複排除後） | Silver 行数 | 一致 | Silver に余分 |
|---|---|---|---|---|---|
| S100ZH4D | JG-L | 681 | 686 | 681 | 5 |
| S100ZCDP | JG-L | 817 | 825 | 817 | 8 |
| S100ZBQZ | JG-L | 1069 | 1069 | 1069 | 0 |
| S100ZAWS | JG-L | 650 | 650 | 650 | 0 |
| S100ZABE | JG-L | 856 | 856 | 856 | 0 |
| S100ZASH | JG-L | 531 | 531 | 531 | 0 |
| S100ZA8M | JG-U | 74 | 74 | 74 | 0 |
| S100ZBGM | JG-U | 66 | 66 | 66 | 0 |
| S100ZA0F | JG-U | 61 | 61 | 61 | 0 |
| S100ZCII | JG-U | 74 | 74 | 74 | 0 |
| S100ZH3O | JG-U | 69 | 69 | 69 | 0 |
| S100Z6ZN | JG-U | 67 | 67 | 67 | 0 |
| S100ZA5J | IF-L | 871 | 871 | 871 | 0 |
| S100Z5CA | IF-L | 1277 | 1277 | 1277 | 0 |
| S100Z5AL | IF-L | 1175 | 1188 | 1175 | 13 |
| S100Z4CX | IF-L | 1061 | 1061 | 1061 | 0 |
| S100Z4H9 | IF-L | 738 | 738 | 738 | 0 |
| S100YH1E | IF-U | 837 | 837 | 837 | 0 |
| S100YN5G | IF-U | 1128 | 1128 | 1128 | 0 |
| S100YIBH | US-L | 816 | 816 | 816 | 0 |
| S100YC5C | US-L | 933 | 941 | 933 | 8 |
| S100YG81 | US-L | 772 | 773 | 772 | 1 |
| S100YG5L | US-L | 908 | 909 | 908 | 1 |
| S100X8TY | AMEND-120 | 1114 | 1135 | 1114 | 21 |
| S100XD4M | AMEND-130 | 1114 | 1135 | 1114 | 21 |
| S100XD30 | AMEND-120 | 571 | 571 | 571 | 0 |
| S100ZEHZ | AMEND-130 | 571 | 571 | 571 | 0 |
| S100XCU9 | AMEND-120 | 1089 | 1105 | 1089 | 16 |
| S100XZ2X | AMEND-130 | 1089 | 1105 | 1089 | 16 |

全書類で「Silver に無い」と「値が違う」は0件。

## dimension 列の確認（連結/単体の区別）

修正後の Silver では dimension 列に次元メンバーが入っている（21,189行中14,476行）。連結/単体を区別するメンバーの例:

1. 書類 S100ZH4D（コジマ）、要素 `jpcrp_cor:NetSalesSummaryOfBusinessResults`、期間 2021-09-01〜2022-08-31、値 279,374,000,000、dimension = `jppfs_cor:NonConsolidatedMember`（単体）。
2. 書類 S100ZH4D（コジマ）、要素 `jpcrp_cor:GrossScope1GreenhouseGasEmissionsScope1And2GreenhouseGasEmissions`、期間 2017-09-01〜2018-08-31、値 2,062、dimension = `jpcrp_cor:ConsolidatedAccountingGroupMember`（連結会計グループ）。
3. 書類 S100ZH4D（コジマ）、要素 `jppfs_cor:NetAssets`、時点 2024-08-31、値 66,359,000,000、dimension = `jppfs_cor:ShareholdersEquityMember;jppfs_cor:NonConsolidatedMember`（資本の内訳と単体の複合次元）。

## Inline XBRL のみ由来の事実について

訂正ペアの6書類を含む全29書類で、**Inline XBRL のみに由来する事実は0件**だった。重複排除後の事実21,079件のうち21,077件は .xbrl と Inline XBRL の両方に存在し、残り2件（前記の円銭表記の EPS）は数値としては .xbrl 側でのみ確認できた。EDINET の .xbrl インスタンスは Inline XBRL の数値事実を全て含む機械生成物であり、本標本では Inline XBRL を併せて読んでも新しい事実は増えなかった（Step 1 で届かなかった「ixbrl のみの事実」は、本標本の範囲では存在しない）。

参考として、Inline XBRL の事実が Silver に取り込まれている対応づけを、訂正ペアの書類から2件挙げる（いずれも重複ペアの Inline XBRL 側）:

1. S100X8TY（LIFULL・有報）の `ix:nonFraction` — `name="jpigp_cor:PurchaseOfTreasurySharesSSIFRS" contextRef="Prior1YearDuration" unitRef="JPY" scale="6" sign="-"`、本文 "0" → Silver の行: 要素 `jpigp_cor:PurchaseOfTreasurySharesSSIFRS`、期間 2023-10-01〜2024-09-30、dimension なし、値 -0.0。
2. S100XD4M（LIFULL・訂正報告書）の同一要素の `ix:nonFraction`（同じ contextRef・sign="-"、本文 "0"）→ Silver の行: 期間 2023-10-01〜2024-09-30、値 -0.0（point_in_time は final）。

## 結論

修正後の Silver は、原文の数値事実を欠落なく・値の誤りなく保持している（欠落0件、値違い0件）。唯一の不一致は、同一事実の重複行110件（Silver の0.52%）であり、原因は変換側の重複排除が値の文字列形と符号付きゼロに依存していることと特定した。この重複は「分類済みの余分」として本報告に記録する。本実装では数値キーの重複排除に改めること。
