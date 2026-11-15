# T4 照合報告 — Silver と XBRL 原文の決定的照合

対象: T3 が生成した Silver（`prototype/output/silver_sample.parquet`、22,194行）と、標本29書類の XBRL 原文。
方法: T3 の convert.py は使わず、Python 標準ライブラリの ElementTree で .xbrl インスタンスから数値の事実を独立に再抽出し、書類ごとに（要素名・期間開始・期間終了・値）の多重集合で突き合わせた。

## 全体の結果

- 原文から再抽出した数値の事実: 22,194件
- Silver の行数: 22,194件
- **一致: 22,194件（一致率 100%）**
- 値が違う: 0件
- Silver に無い（原文にある）: 0件
- Silver に余分（原文にない）: 0件

値が違った事例はない。

## 書類ごとの結果

| 書類ID | セル | 原文の数値事実 | Silver 行数 | 一致 | 不一致 |
|---|---|---|---|---|---|
| S100ZH4D | JG-L | 702 | 702 | 702 | 0 |
| S100ZCDP | JG-L | 850 | 850 | 850 | 0 |
| S100ZBQZ | JG-L | 1119 | 1119 | 1119 | 0 |
| S100ZAWS | JG-L | 678 | 678 | 678 | 0 |
| S100ZABE | JG-L | 889 | 889 | 889 | 0 |
| S100ZASH | JG-L | 553 | 553 | 553 | 0 |
| S100ZA8M | JG-U | 77 | 77 | 77 | 0 |
| S100ZBGM | JG-U | 69 | 69 | 69 | 0 |
| S100ZA0F | JG-U | 64 | 64 | 64 | 0 |
| S100ZCII | JG-U | 77 | 77 | 77 | 0 |
| S100ZH3O | JG-U | 72 | 72 | 72 | 0 |
| S100Z6ZN | JG-U | 70 | 70 | 70 | 0 |
| S100ZA5J | IF-L | 938 | 938 | 938 | 0 |
| S100Z5CA | IF-L | 1346 | 1346 | 1346 | 0 |
| S100Z5AL | IF-L | 1254 | 1254 | 1254 | 0 |
| S100Z4CX | IF-L | 1141 | 1141 | 1141 | 0 |
| S100Z4H9 | IF-L | 797 | 797 | 797 | 0 |
| S100YH1E | IF-U | 906 | 906 | 906 | 0 |
| S100YN5G | IF-U | 1218 | 1218 | 1218 | 0 |
| S100YIBH | US-L | 839 | 839 | 839 | 0 |
| S100YC5C | US-L | 953 | 953 | 953 | 0 |
| S100YG81 | US-L | 794 | 794 | 794 | 0 |
| S100YG5L | US-L | 926 | 926 | 926 | 0 |
| S100X8TY | AMEND-120 | 1192 | 1192 | 1192 | 0 |
| S100XD4M | AMEND-130 | 1192 | 1192 | 1192 | 0 |
| S100XD30 | AMEND-120 | 599 | 599 | 599 | 0 |
| S100ZEHZ | AMEND-130 | 599 | 599 | 599 | 0 |
| S100XCU9 | AMEND-120 | 1140 | 1140 | 1140 | 0 |
| S100XZ2X | AMEND-130 | 1140 | 1140 | 1140 | 0 |

全29書類で不一致ゼロ。

## T3 の「unparseable（数値として読めなかった）」2,710件の原因内訳

結論: **2,710件すべてが (c) の xsi:nil（値なし）事実**だった。(a) と (b) は0件。

- (a) ダッシュ表記（「-」「－」等）: **0件**。原文の nil 事実 3,020件のテキストは全件が空文字で、ダッシュ表記の事実は無かった。Inline XBRL 側でも、ダッシュのみを内容とする ix:nonFraction は0件だった。
- (b) Inline XBRL の sign="-" 属性による負値: **0件**。sign="-" の ix:nonFraction は標本全体で 2,897件あったが、全件が数値テキストを持ち、対応する数値事実は .xbrl インスタンス側に明示的な負値として存在し、Silver と全件一致した（上記の全体照合に含まれる）。そもそも T3 の Inline XBRL 抽出は、HTML パーサがタグ名を小文字化するため `nonFraction` に一致せず、抽出件数そのものが0件だった。2,710件はすべて .xbrl 側の分類である。
- (c) その他: **2,710件すべて xsi:nil**。値が存在しない事実（該当数値なし）であり、数値化できないのが正しい。ただし T3 の分類名「unparseable」は実態と異なる。

### 原因（convert.py の nil 判定の名前空間の取り違え）

convert.py は nil 属性を XBRL インスタンスの名前空間（`http://www.xbrl.org/2003/instance`）で探していたが、実際の属性は XMLSchema-instance 名前空間の `xsi:nil` である。そのため nil 判定が一度も働かず、nil 事実が後段に流れて「空文字を数値化できない」＝ unparseable に落ちた。

実証（独立抽出との突き合わせ）:

- 原文の nil 事実は 3,020件。そのうち unitRef と有効な contextRef を持つものが **2,710件**で、T3 の unparseable と総数が一致するだけでなく、**書類ごとの件数も29書類すべてで一致**した（例: S100Z4CX 258件、S100YN5G 213件、S100ZA0F 3件）。
- 要素別の上位も一致した（TotalChangesOfItemsDuringThePeriod 136件、InterimDividendPaidPerShareSummaryOfBusinessResults 74件、DilutedEarningsPerShareSummaryOfBusinessResults 74件など）。
- nil で unitRef の無い残り 310件は、T3 では non_numeric に分類されていた（私の分類では nil 3,020件、non_numeric 7,914件。T3 は nil 0件、non_numeric 8,224件で、差の 310件がこれに当たる）。

### Silver 本体への影響

無い。nil 事実は値を持たないため、Silver に入らないのが正しく、数値データの欠落・誤りにはなっていない。影響があるのは対象外分類の内訳表示だけである。本実装では、nil 判定を正しい名前空間（xsi:nil）で行い、nil を独立した分類として数えること。

## 判定

- 合格条件（2）「Silver と XBRL 原文の一致率100%（対象外は明示分類した要素のみ）」を満たす。一致率は実測で 100%（22,194 / 22,194）。
- 指摘事項は1件: T3 の対象外分類の内訳が実態と異なる（unparseable 2,710件の実体は nil、non_numeric のうち310件も nil）。Silver の数値そのものに誤りはない。
