"""edinet-pipeline 本番変換パッケージ。

XBRL から Silver (型つき事実) への変換の中核 (convert_core) と、
バッチ実行管理 (batch: 目録による冪等・再開可能性・失敗の隔離) を持つ。
実行入口は python -m pipeline.cli。
"""
