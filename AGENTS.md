# AGENTS.md（edinet-pipeline）

## 目的
EDINET の開示書類を毎日自動で取得・解析し、型つきデータへ変換するパイプライン。データ本体は Hugging Face の financial-lakehouse-v2 に置く。

## 規則
- 成果物は日本語で書く。
- コミットには Executor トレーラを付ける（例: Executor: implementer）。
- API キー等の秘密をリポジトリとログに出さない。
- 既存資産（ARIA-3、financial-lakehouse）へは読み取りのみで、書き込まない。
