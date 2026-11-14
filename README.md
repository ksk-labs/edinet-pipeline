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

## 状態

構築中。初期段階です。

## ライセンス

All Rights Reserved（`LICENSE` を参照）。
