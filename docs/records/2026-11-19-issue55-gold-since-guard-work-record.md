# Issue #55 作業記録: Gold update の手動実行で since_month 未指定のときに全期間の状態が縮む問題

## 欠陥

`pipeline/gold.py` の update は、透かしと state／summary を対象月の集合で書き直す。gold.yml の手動 dispatch で since_month を未指定にすると既定値 2021-11 が使われ、全期間129か月（2016-03〜）で構築済みの Gold の state が61か月の集合に縮んでいた（2021-10 以前の月の変化を検出できなくなる）。日次の自動経路（daily_parse の後段ジョブと gold.yml の schedule）は常に since_month=2016-03 を明示するため通常運転には影響しないが、手動実行の足場として危険なまま残っていた。

発端は Issue #50 の code-review（報告書 files/2026-11-19-issue50-code-review.md の N-3）。

## 選んだ方式と根拠

Issue 本文の候補（既定値を 2016-03 に変える／狭い範囲の update を拒否する）と、課題の要件（未指定時は既存 state の範囲を引き継ぐ、または狭い指定を明示的にエラーで止める、のいずれか・併用可）を、gold.py と gold.yml の実物と照らして次の併用にした。

1. **未指定時は既存 state の最古の月を引き継ぐ**（`resolve_update_since_month`）。state が保持する対象月の範囲こそが「全期間」の正であり、未指定の手動実行はその範囲の差分更新として動くのが自然な意味になる。既定値を 2016-03 へ変えるだけの案は、将来 state の範囲が 2016-03 より広がった場合に同じ事故が再発するため採らない。
2. **state の範囲より狭い指定は明示的にエラーで止める**。黙って縮む経路を残さない。範囲を狭める意図がある場合は全量再生成である build を使う、という切り分けをエラーメッセージと docstring に明記した。
3. **state の書き直しでは対象外の月の透かしを既存 state から引き継ぐ**（`{**old_months, **watermarks}`）。Silver が手元に無い月があっても対象月の集合が縮まないことを、呼び出し側の指定に依存せず構造でも保証する。

build は全量再生成であり、対象範囲の指定が意味を持つため、未指定時の既定値 2021-11（第1期）は従来どおり維持した。argparse の `--since-month` の既定値は None に変え、「未指定」を build／update それぞれの規則で解決する形にした。日次経路の既存の呼び出し（daily_parse の `--since-month 2016-03` と gold.yml の schedule の明示指定）は変更していない。

## 変更内容

- `pipeline/gold.py`:
  - `resolve_update_since_month(requested, old_months)` を新設（上記1・2と、state が無い場合は既定値へのフォールバック）。
  - `cmd_update` は既存の Gold（state）を先に読み、解決した開始月で対象月を決める。state の書き直しは上記3のマージ。
  - `cmd_build` は未指定なら既定値 2021-11 を使う（従来動作の維持）。
  - `_remote_months` は since_month=None（全月）を許す（update の未指定時に HF から全月を取得するため）。
  - モジュール docstring に「対象開始月 (--since-month) の扱い (Issue #55)」の節を追加。
- `.github/workflows/gold.yml`: since_month の説明（ヘッダコメントと入力の description）を新しい扱いに合わせて更新。実行ロジックは不変（空の場合は従来どおり `--since-month` を渡さない）。
- `tests/test_gold.py`: 回帰テストを追加。
  - `ResolveUpdateSinceMonthTest`（4件）: 引き継ぎ、state 無しのフォールバック、狭い指定の拒否、同等・より広い指定の受理。
  - `UpdateSinceGuardTest`（2件）: Silver を 2021-11 の前後（2020-05 と 2026-06）に置いて全期間で build した後、(a) since_month 未指定の update で変更が取り込まれ、state と summary の対象月が全期間のまま縮まないこと、(b) 2021-11 の明示指定がエラーで止まり state が書き換わらないこと。

## テスト結果

- 全テスト 331件すべて合格（従来325件＋新規6件）。実行環境は CI と同じ依存（requirements.txt＋arelle-release==2.47.5）の venv で、`python -m unittest discover -s tests` の実測。
- 突然変異の確認: 修正前の gold.py（main の現行版）に新規テストを当てると `UpdateSinceGuardTest` の2件が失敗することを確認した（未指定の update が従来の欠陥どおりに振る舞い、狭い指定も止まらない）。修正後のコードでは全件合格する。
