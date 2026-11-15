# 実行ログ（T3 変換、Step 2 塊1 修正・重複除去修正後）

- 処理した書類: 29/29
- Silver の行数: 21079
- parquet の SHA-256: b6fc90b354c0b5a9866f5b183db97778994035babc6e4872eba6ac08702a2559
- 欠落した書類: なし
- 統計: {'numeric': 44388, 'non_numeric': 15828, 'nil': 6040, 'unparseable': 0, 'no_context': 0}
- 統計 (由来別): {'xbrl': {'numeric': 22194, 'non_numeric': 7914, 'nil': 3020, 'unparseable': 0, 'no_context': 0}, 'ixbrl': {'numeric': 22194, 'non_numeric': 7914, 'nil': 3020, 'unparseable': 0, 'no_context': 0}}
- 対象外の内訳: {'non_numeric': 15828, 'nil': 6040, 'unparseable': 0, 'no_context': 0}
- dimension が空でない行数: 14381
- ixbrl 由来で新たに加わった行数: 0

## 要素名の正規化規則 (Step 2 塊1 (d))

要素名の正規化規則 (Step 2 塊1 (d)): 要素の接頭辞は名前空間 URI から作る。URI のパス末尾のセグメントが日付 (YYYY-MM-DD) または数字のみである間はそれを取り除き続け、残った末尾のセグメントを接頭辞とする。例: http://disclosure.edinet-fsa.go.jp/jpcrp030000/asr/001/E00988-000/2026-03-31/01/2026-06-24 -> 接頭辞 E00988-000。標準タクソノミ (例: .../taxonomy/jppfs/2025-11-01/jppfs_cor) は末尾が日付でも数字でもないため変わらない (接頭辞 jppfs_cor)。Inline XBRL の name 属性は、文書の xmlns 宣言で名前空間 URI へ解決してから同じ規則を適用する。

## 修正前後の比較

| 項目 | 修正前 | 修正後 |
|---|---|---|
| Silver の行数 | 21189 | 21079 |
| unparseable 件数 (全体) | 0 | 0 |
| unparseable 件数 (.xbrl 由来のみ) | 0 | 0 |
| nil 件数 (全体) | 6040 | 6040 |
| nil 件数 (.xbrl 由来のみ) | 3020 | 3020 |
| dimension が空でない行数 | 14476 | 14381 |
| ixbrl 由来で新たに加わった行数 | 110 | 0 |
| parquet の SHA-256 | a0bef894c7c13ab5fe7497f837f255a1544d5cb421f0b1d7185acbbc4819e8e9 | b6fc90b354c0b5a9866f5b183db97778994035babc6e4872eba6ac08702a2559 |

重複除去の数値キー化 (修正 (e)) により、修正前 21189 行から 110 件の重複行が落ち、修正後は 21079 行になった。

注: 修正前は、上書き前に読み取った前回の実行結果 (run_log.json と parquet) の値。nil と unparseable は全体と .xbrl 由来のみを併記する。

## tar のハッシュ（取得元）

- 2026-11-13.tar: a66a29e418003e66592dcf0158813105b9a5848248e5da3480431997362c6351
- 2026-11-10.tar: 77f6b43a3fccfae632c51fb7c2a3fdb4b9b82d002408418f7396881318a6b891
- 2026-10-30.tar: 32371486026f258153c24d0215c52883f703b7134b8b85eb45b8843b3f642dfc
- 2026-10-29.tar: af2b4eba8bee701a12119b605c3ded28fdf1582c709e00d42d6ecbdd38740550
- 2026-10-28.tar: 1c4b9c6e84ff8b1f81f923524a548f24eccfd1b09da7a6564091aa1ac16d6c6f
- 2026-09-30.tar: f3084d3db38a642558265ddd3bf42e4ad4fa79a42fd65354e5b09c0b89ecc26d
- 2026-09-28.tar: c1624d996a78b9a32ce29ebdea0562433a89ad7ae3b2b37d2725f1ded52df365
- 2026-06-30.tar: 6928faea1f20a6f6aa80739000f26aee81f2ac654e56f5cf8385a33424e58f38
- 2026-06-24.tar: 0731f8100565c3e467df1b7a50b2d21e5a8bbecf980849e54009e02dd9087d75
- 2026-06-22.tar: e6343998a2d0e0a88d65d6dd72bbf6ab3bace7c369fdaf348d9c63409cbcf187
- 2025-12-22.tar: 7d74ce27b16a1a95a2d86c5ff308d0f3755ca67a5d1d6f96e278ab61410e5a86
- 2025-12-25.tar: c73b46dc5803532f146637087a93497e4b88e07804349818ae8900fd86aa8795
- 2026-11-11.tar: 39b8662747ce69b4e06fe797b6503aa7a49776c8d68aa99a6d11f7c4da83f606
- 2025-12-24.tar: 4df719379288ddd4d1654f9342590d69c2142ba236b923a91e378f0c90319ae3
- 2026-04-15.tar: 365d216e62ae1b3a28d21dec65da1effe5baea3a4326daede115b66e4f0ae9ef

## 書類ごとの結果

- S100ZH4D (JG-L): ok facts=681 extracted=1404 ixbrl_new=0
- S100ZCDP (JG-L): ok facts=817 extracted=1700 ixbrl_new=0
- S100ZBQZ (JG-L): ok facts=1069 extracted=2238 ixbrl_new=0
- S100ZAWS (JG-L): ok facts=650 extracted=1356 ixbrl_new=0
- S100ZABE (JG-L): ok facts=856 extracted=1778 ixbrl_new=0
- S100ZASH (JG-L): ok facts=531 extracted=1106 ixbrl_new=0
- S100ZA8M (JG-U): ok facts=74 extracted=154 ixbrl_new=0
- S100ZBGM (JG-U): ok facts=66 extracted=138 ixbrl_new=0
- S100ZA0F (JG-U): ok facts=61 extracted=128 ixbrl_new=0
- S100ZCII (JG-U): ok facts=74 extracted=154 ixbrl_new=0
- S100ZH3O (JG-U): ok facts=69 extracted=144 ixbrl_new=0
- S100Z6ZN (JG-U): ok facts=67 extracted=140 ixbrl_new=0
- S100ZA5J (IF-L): ok facts=871 extracted=1876 ixbrl_new=0
- S100Z5CA (IF-L): ok facts=1277 extracted=2692 ixbrl_new=0
- S100Z5AL (IF-L): ok facts=1175 extracted=2508 ixbrl_new=0
- S100Z4CX (IF-L): ok facts=1061 extracted=2282 ixbrl_new=0
- S100Z4H9 (IF-L): ok facts=738 extracted=1594 ixbrl_new=0
- S100YH1E (IF-U): ok facts=837 extracted=1812 ixbrl_new=0
- S100YN5G (IF-U): ok facts=1128 extracted=2436 ixbrl_new=0
- S100YIBH (US-L): ok facts=816 extracted=1678 ixbrl_new=0
- S100YC5C (US-L): ok facts=933 extracted=1906 ixbrl_new=0
- S100YG81 (US-L): ok facts=772 extracted=1588 ixbrl_new=0
- S100YG5L (US-L): ok facts=908 extracted=1852 ixbrl_new=0
- S100X8TY (AMEND-120): ok facts=1114 extracted=2384 ixbrl_new=0
- S100XD4M (AMEND-130): ok facts=1114 extracted=2384 ixbrl_new=0
- S100XD30 (AMEND-120): ok facts=571 extracted=1198 ixbrl_new=0
- S100ZEHZ (AMEND-130): ok facts=571 extracted=1198 ixbrl_new=0
- S100XCU9 (AMEND-120): ok facts=1089 extracted=2280 ixbrl_new=0
- S100XZ2X (AMEND-130): ok facts=1089 extracted=2280 ixbrl_new=0