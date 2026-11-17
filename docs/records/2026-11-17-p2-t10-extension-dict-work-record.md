# P2-T10 作業記録: 拡張タクソノミの表示名の要素辞書（別パス構築）

- 対象チケット: Issue #25 (P2-T10: 拡張タクソノミの表示名の要素辞書（別パス構築）)
- 仕様の正本: 仕様 Issue #15 §10（提出者拡張タクソノミの表示名は、Silver の作り直しではなく独立した要素辞書として別パスで構築する。日次分は変換時に併せて取得し、過去分は tar から別途構築する）
- 実施日: 2026-11-17
- 実施役: implementer

## 作ったもの

- `pipeline/extension_dict.py`（新規）: 日次解析（pipeline.parse）が月別に出力する `parse/month=YYYY-MM/extension_labels.jsonl` を入力として、表示名の要素辞書を構築する。Silver の作り直しはしない。
  - 辞書の形: 1行 = 1観測（doc_id, element, element_ns）。書類ごとの対応をそのまま保持し、集約で出所を失わない。element は Silver の element 列と同じ文字列（接頭辞:局所名）で、Silver とは (doc_id, element) で結合できる。あわせて element_local（局所名）を持つ。
  - 出所: 書類目録（catalog/month=YYYY-MM/ledger.jsonl）から、提出者（EDINET コード・提出者名・証券コード・ファンドコード）、書類種別、提出日時、源泉の種別・識別子・取得日時・再配布可否を併記する。目録に無い書類は出所の欄を空にし、行は落とさずサマリ（docs_without_catalog）に数える。
  - 重複と食い違いの扱い: 同一キー（doc_id, element, element_ns）の重複は除く。同じキーで表示名が食い違う場合は、表示名のある組を優先し、その中で決定的に（表示名の辞書順で最小の組を）採り、サマリの key_conflicts に記録する（表示名の無い組が実在する表示名に勝つことはない。should-fix 対応で変更。下記参照）。要素名が同じで書類をまたいで表示名が食い違う場合も、行は書類ごとに残したうえで label_conflicts に記録する（どちらが正かは辞書側では決めない）。不正な行（必須項目の欠け）と壊れた JSONL 行は、黙って捨てず件数をサマリに残す。
  - 目録の重複: 目録は同じ書類が取得のたびに重複して載る（2026-11 の実物では1書類あたり3行で、fetched_at 以外は同一だった）。最新の行をその書類の状態とする。「最新」はファイル内の出現順ではなく fetched_at の最大で決め、fetched_at が同値の場合は行全体の辞書順で決めるため、入力の並びに依存しない（should-fix 対応で変更。下記参照）。
  - 配置先: v2 データセット（Yoshi-Dai/financial-lakehouse-v2）の `dictionary/extension_elements.parquet` と `dictionary/extension_elements_summary.json`。辞書は入力 JSONL 全部からの再構築であり、同じ入力からは同じ行集合（ソート済み）が得られるため、再実行で重複しない（冪等）。
  - CLI: `build`（ローカル JSONL から構築）と `sync`（v2 から対象月の入力を落として再構築し、`--upload` のときだけ dictionary/ へ上げる。対象月を省略すると extension_labels が実在する月をすべて対象にする）。**部分月での `--upload` は辞書全体をその月分だけに置き換えてしまうため、`--allow-partial-upload` で全体の置き換えを明示しない限りエラーで止まる**（should-fix 対応で追加。下記参照）。`--upload` なしの部分月の構築は制限しない。
- `tests/test_extension_dict.py`（新規）: 構築規則の回帰テスト10件（出所の併記、目録なしの扱い、重複除去、キー競合・書類間競合の記録、不正行の計数、表示名欠けの計数、ソート、目録の最新行優先、parquet 往復と壊れた行の報告）。should-fix 対応で10件を追加し計20件（下記参照）。リポジトリ全体のテストは81件すべて合格。
- `.github/workflows/extension_dict.yml`（新規）: 手動 dispatch で sync --upload を実行し、辞書を v2 の dictionary/ へ配置する運用ワークフロー。日次解析のつど自動では回さず、表示名の入力が増えたときに dispatch する。

## 実地の構築と照合（2026-11 分）

既存の `parse/month=2026-11/extension_labels.jsonl`（日次解析の実績分）で実地に構築した。構築物はこの環境に保持した（`extension_elements.parquet` 216行。所在は本記録末尾）。

入力側の実測:

- extension_labels.jsonl: 216行、書類40件、名前空間40件（書類ごとに固有）、要素名174種。表示名は日英とも欠け0件。
- 書類目録（2026-11）: 1,248行 = 416書類 × 3回取得分（fetched_at 以外は同一）。

照合結果（構築経路とは別のスクリプトで parquet 実物と入力実物を直接突き合わせ）:

- 入力216行 → 辞書216行。不正0行、重複0行、キー競合0件、書類間の表示名競合0件。
- キー集合（doc_id, element, element_ns）は入力と完全一致。表示名と出所（提出者名・EDINET コード・提出日時）の不一致0件。
- 出所の付与: 40書類すべてに目録があり、提出者は12者（野村アセットマネジメント 71行、りそなアセットマネジメント 39行、エア・ウォーター 29行、三菱ＵＦＪアセットマネジメント 26行、トヨタファイナンス 19行、ほか7者）。
- Silver との突合（silver/month=2026-11、870,880行）: 日次解析の40書類について、Silver に現れる拡張要素（doc_id, element）は39組で、すべて辞書に収録されていた（欠落0）。辞書の残り177組は非数値事実（TextBlock 等）由来であり、Silver には無いが表示名の観測として保持する（parse が全事実から表示名を集める設計どおり）。

## code-review should-fix への対応（2026-11-17、修正役）

PR #37 の code-review（報告書: 2026-11-17-p2t10-code-review.md §6）で挙がった should-fix 3件を、本 PR のブランチへの追加コミットで解消した。いずれも現在の成果データ（2026-11 分の辞書216行）には影響していないことを、修正後の再構築で確認した（下記）。

1. **部分月の upload で辞書が縮む問題**: `sync` に `allow_partial_upload`（CLI は `--allow-partial-upload`）を追加した。対象月が実在する全月の一部である場合の `--upload` は、この明示指定が無い限りダウンロード前にエラーで止まる。全月指定・月省略時の upload と、`--upload` なしの部分月構築は従来どおり動く。ワークフロー（extension_dict.yml）にも `allow_partial_upload` 入力（既定 false）を追加し、months の説明に置き換えの範囲を明記した。CLI のヘルプ（`--months` / `--upload` / `--allow-partial-upload`）にも同じ注意を書いた。テスト5件で固定した（部分月+upload の拒否、明示指定での許可、全月・月省略時の許可、upload なし部分月の許可）。
2. **catalog_index の最新行の選び方**: ファイル順（最後の行）ではなく、fetched_at の最大行を選ぶ形に変えた。fetched_at が同値の場合は行全体（JSON の辞書順）で決めるため、入力の並びを入れ替えても結果は変わらない。順序を入れ替えた入力と同値タイブレークのテスト2件で固定した。
3. **表示名競合での None の優先**: 同一キーの競合解消の並び替えキーを変え、表示名のある組（日英の少なくとも一方が空でない組）を、表示名の無い組（日英とも空）より優先するようにした。表示名のある組どうしでは従来どおり辞書順で最小の組を採る。テスト3件で固定した（入力順を入れ替えた場合、部分的に表示名がある組、表示名のある組どうしの辞書順）。

検証: 新規・変更テストは修正前コードで該当箇所が失敗すること（失敗6件・エラー1件。残りの新規3件は全月許可等の回帰ガードで修正前から合格）を確認し、修正後にリポジトリ全体81件すべて合格した。2026-11 分の入力（extension_labels 216行・目録1,248行）で辞書を再構築した結果は216行で、修正前の成果 parquet と行の内容が完全一致し、バイト列（SHA-256）まで一致した。

## 残した未決事項

- **v2 データセットへの実配置は未実施**。この環境には v2 への書き込み資格情報が無い（書き込みは Actions の HF_TOKEN 経由が正規の経路。P2-T2 と同じ事情）。配置は、本変更のマージ後に `.github/workflows/extension_dict.yml` を dispatch して行い、`dictionary/extension_elements.parquet` の実在と件数（216行）を HF 側で確認すること。ローカル構築物は照合済みであり、dispatch の sync は同じ入力から同じ内容を再構築する。
- **過去分（バックフィル期間）の表示名は未構築**。バックフィルは lxml 変換（convert_core）で表示名を集めていないため、extension_labels が存在しない。実測では、silver/month=2026-11 だけでも拡張要素を持つ書類が1,757件あり、辞書でカバーできるのは日次解析分の40書類だけである。過去分は仕様どおり tar から別途構築する（抽出は pipeline.parse の extract_facts の経路を再利用でき、出力は extension_labels 形式の JSONL に揃える。本モジュールの build はその JSONL をそのまま取り込める）。全期間分の抽出の実施は本 PR の範囲外とし、チケット #25 の残件として残す。
- 辞書の自動更新（日次解析の merge への相乗り）は入れていない。表示名の入力が増えるたびに手動 dispatch で再構築する運用であり、相乗りさせるかは日次運用の実績を見て判断する。

## 成果物の所在

- コードとテストとワークフロー: 本 PR（pipeline/extension_dict.py、tests/test_extension_dict.py、.github/workflows/extension_dict.yml）
- ローカル構築物（照合済み）: 実施環境の `~/workspace/goals/edinet/hidden_files/p2t10/output/extension_elements.parquet` と `extension_elements_summary.json`（216行、照合結果は上記）

Executor: implementer
