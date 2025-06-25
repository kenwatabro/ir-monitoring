# IR Monitoring

本リポジトリは、EDINET / TDnet から上場企業の IR 文書を収集・保存し、PostgreSQL + FastAPI で検索 API を提供するローカル基盤です。

## セットアップ（ローカル）

```bash
# 1. 必要パッケージ
sudo snap install docker
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# 2. 環境変数
cp env.example .env  # 実際のキーを入力

# 3. DB 初期化（Postgres が起動済みの場合）
make db/init

# 4. ETL を 1 日分テスト実行
python -m src.runner --since $(date +%F) --days 1

# 5. API 起動
uvicorn src.api.main:app --reload
```

## テスト & Lint

```bash
ruff check .
black . --check
pytest --cov=src
```

## ライセンス
MIT
