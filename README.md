# edinet-pipeline

EDINET（金融庁の電子開示システム）に提出された開示書類を自動取得・解析し、財務データ基盤へ蓄積する処理用リポジトリです。

## 目的

- EDINET の全書類を対象に、過去10年分と今後数十年分を自動で蓄積し続ける。
- 完全無料・完全自動で運営する。個人情報は収集しない。

## 構成（予定）

- Bronze: EDINET から取得した書類原文
- Silver: XBRL から抽出した型つきの財務事実
- Gold: 会社×年度の主要指標と派生指標

蓄積先は Hugging Face のデータセット `Yoshi-Dai/financial-lakehouse-v2` です。

## 本番変換 (Silver)

- `pipeline/`: XBRL から Silver (型つき事実) への本番変換。入口は `python -m pipeline.cli`。
- 処理済みは目録 (ledger.jsonl) で管理し、再実行しても重複しない (冪等)。途中から再開でき、1書類の失敗は隔離して記録する。
- 実行は GitHub Actions のワークフロー (`.github/workflows/convert.yml`) から行う。
- 設計と実測の記録: `docs/records/2026-11-15-s2-t1-work-record.md`。

## 状態

構築中。初期段階です。

## ライセンス

All Rights Reserved（`LICENSE` を参照）。
