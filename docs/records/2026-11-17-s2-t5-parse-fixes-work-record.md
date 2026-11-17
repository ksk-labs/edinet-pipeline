# S2-T5 作業記録: 塊2 レビュー指摘の修正

- 対象チケット: Issue #14 (S2-T5: 日次自動化の組み立て)
- 対象: `pipeline/parse.py`（塊2）のコードレビュー（2026-11-17-s2t5-chunk2-code-review.md）の should-fix 5件（S-1・S-2・S-3・T-1・T-2）と、作業記録の誤記1件（同レビューの note S-4）
- 実施日: 2026-11-17
- 実施役: fixer
- ブランチ: `s2t5-parse-fixes`（main へは着地させていない。統合はメインの作業）

## 修正内容

### S-1（should-fix）: merge の系譜判定が現在の superseded_by を無条件に勝者候補へ加え、repair-lineage と食い違う

- 修正: `run_merge` の `superseded_for` で、現在の値の扱いを repair-lineage と揃えた。解析目録（parse_latest）に現在の値がある場合は、その doc_type が 130 のときだけ候補に含め（提出日時は目録の値）、130 でないと分かっている値は候補に含めない。候補が他に無ければ勝者は None になり、規則に合わない値は次回の merge で外れる（repair-lineage の「現在の値が 130 でないと分かっており、候補も無い場合は None」と同じ結果）。
- バックフィル期の扱いは保った: 解析目録に現在の値が無い場合は、従来どおり現在の値と Silver の行から引く提出日時で候補にする。silver 目録には doc_type が無く、バックフィル期の勝者を保つには現在の値に頼るしかない、というレビューの前提を壊していない。

### S-2（should-fix）: 実行サマリが月単位の単一ファイルで上書きされる

- 修正: 保存先を塊1 の修正（S-3）と同形の `parse/month=YYYY-MM/run_summary-<対象日>-<実行開始時刻>.json` に変え、実行単位で残す。対象日は計画の date を使い、doc_ids 指定の実行（date が無い）では実行開始日を使う。開始時刻の stamp は塊1 と同じ `%Y%m%dT%H%M%SZ`（UTC）。ローカルの `summaries/<月>/run_summary.json` は従来どおり。

### S-3（should-fix）: 等価性テストの道具がリポジトリに無く、版更新時の必須手順が再実行可能な形で残っていない

- 修正: 照合スクリプト `tests/equiv_check.py` と期待値 `tests/equiv_samples.json` をリポジトリへ置いた。標本8書類について、抽出事実の多重集合（S2-T3 の照合規則）の件数と SHA-256 を期待値として固定する。`--silver-dir` を渡せば Silver 実物との多重集合の直接照合も行う。
- 形の判断: 標本データそのもの（zip と事実の全量）は置かなかった。事実の全量は既存 Silver の中にあり、doc_id と月から再取得できる複製である一方、件数とハッシュは多重集合の同一性を固定でき、版更新時の照合の目的（抽出の等価性の回帰確認）には足りる。zip は EDINET API の書類取得（type=1、doc_id 指定）で再取得でき、手順はスクリプトの冒頭に書いた。
- 期待値の確定: 既存 Silver の実物（月別 parquet）から計算し、塊2 実施役の記録済み抽出出力（8書類）とも全件一致を確認した。合計は 9,078 事実。なお標本の所在月は実物で確認し直し、S100W0ZR は 2025-06、S100Y335 は 2026-05 の Silver にある（実施役のローカル写しの対応表は両方とも 2026-06 としていた）。
- 実走の確認: repo 化した `tests/equiv_check.py` を標本 zip・タクソノミキャッシュ・Silver 実物で実行し、8書類すべて OK（期待値とも Silver 実物とも一致、不一致 0）だった。

### T-1（should-fix）: バナーコメント6件

- コメント規約に従い、`pipeline/parse.py` の区切りバナー6件（抽出・検証・plan・shard・merge・CLI）を除去した。区切りは空行と定義の配置で足りる。

### T-2（should-fix）: `_with_retry` / `_retryable` の再実装と、現行 hf_sync との判定の食い違い

- 修正: parse 側の再実装を削除し、`from .hf_sync import _with_retry` に寄せた（塊1 修正後の discovery.py と同じ形）。これで再試行対象の判定は、塊1 で一般化された現行形（例外の `response.status_code` に加えて `status_code` 属性も見る）になる。「pipeline.hf_sync と同じ方針」という実態と異なる docstring は、関数ごと無くなった。不要になった `import time` も除去した。

### S-4（note）: 塊2 作業記録の等価性の合計の誤記

- `docs/records/2026-11-17-s2-t5-parse-work-record.md` の「合計 8,078 事実」を「合計 9,078 事実」に訂正した（内訳の合計と、期待値の再計算 9,078 に一致）。
- レビューが併せて指摘する STATE.md（リポジトリ外のプロジェクト記録）側の同記は、本ブランチの範囲外のため含んでいない。

## テスト

- `tests/test_parse.py` を新設した（標準ライブラリの unittest、6件）。
  - S-1 の回帰3件: 解析目録で 130 でないと分かる現在の値は、提出日時が新しくても勝者にならないこと。バックフィル期（解析目録に無い）の現在の値は従来どおり勝者に残ること。130 と分かる現在の値は候補に残ること。merge 全体を、HF を fake に差し替えて実行して確認する。
  - S-2 の回帰1件: 実行サマリが `run_summary-<対象日>-<stamp>.json` の形で残り、同じ計画で2回実行しても2件残って上書きされないこと。
  - T-2 の2件: `parse._with_retry` が `hf_sync._with_retry` そのものであること。`status_code` 属性だけを持つ例外（429）が再試行されること。
- 結果: 修正前のコードでは S-1・S-2・T-2 の回帰4件が失敗（3件の失敗と1件のエラー）し、従来の振る舞いの保存を確認する2件は合格した。修正後のコードでは6件すべて合格し、既存の `tests/test_discovery.py` 17件と合わせて全23件が合格した。
- S-3 の実走結果は上記（8書類すべて OK）。

## コミット

- `pipeline/parse.py`: S-1・S-2・T-1・T-2 の修正
- `tests/test_parse.py`・`tests/equiv_check.py`・`tests/equiv_samples.json`: 回帰テストと等価性照合の道具の追加
- `docs/records/2026-11-17-s2-t5-parse-work-record.md`: 等価性の合計の訂正
- 本記録のコミット

Executor: fixer
