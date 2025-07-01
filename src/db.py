import os
from contextlib import contextmanager
from typing import Iterator, Mapping
from datetime import date, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# -------------------------------------------------------------
# Env helper
# -------------------------------------------------------------


def _first_token(env_name: str, default: str = "") -> str:
    """環境変数値の先頭トークンを返す。

    - 値が空、未設定、または空白のみの場合は *default* を返す。
    - 行末コメント " # xxxx" を除去するために空白区切りの 1 つ目だけ取得。
    """

    val = os.getenv(env_name)
    if not val or not val.strip():
        val = default
    return val.strip().split()[0]


# Build PostgreSQL URL from env vars if DATABASE_URL not provided
if url := os.getenv("DATABASE_URL"):
    DB_URL = url
else:
    host = _first_token("POSTGRES_HOST", "localhost")
    port = _first_token("POSTGRES_PORT", "5432")
    database = _first_token("POSTGRES_DB", "ir_db")
    user = _first_token("POSTGRES_USER", "postgres")
    password = _first_token("POSTGRES_PASSWORD", "")
    DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


@contextmanager
def session_scope() -> Iterator[None]:
    """Provide a transactional scope around a series of operations."""
    with engine.begin() as conn:
        yield conn


def _ensure_documents_partition(pub_date: date) -> None:
    """指定された pub_date 用の月次パーティションを作成 (存在しなければ)。"""
    first_day = pub_date.replace(day=1)
    # 次月 1 日を計算
    next_month = (first_day + timedelta(days=32)).replace(day=1)
    part_name = f"documents_{first_day.strftime('%Y_%m')}"

    create_sql = text(
        f"""
        CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF documents
        FOR VALUES FROM ('{first_day.isoformat()}') TO ('{next_month.isoformat()}');
        """
    )

    with session_scope() as conn:
        conn.execute(create_sql)


def upsert_document(record: Mapping[str, object]) -> None:
    """Insert a row into documents if not exists (by doc_id, pub_date).

    挿入前に対象月のパーティションを自動生成する。
    """

    # パーティションを事前に確保 (月次)
    pub_date = record["pub_date"]
    if isinstance(pub_date, date):
        _ensure_documents_partition(pub_date)

    sql = text(
        """
        INSERT INTO documents (
            doc_id, source, doc_type, pub_date, file_path, sha256, size_bytes, xbrl_flag, pdf_flag
        ) VALUES (
            :doc_id, :source, :doc_type, :pub_date, :file_path, :sha256, :size_bytes, :xbrl_flag, :pdf_flag
        ) ON CONFLICT (doc_id, pub_date) DO NOTHING
        """
    )
    with session_scope() as conn:
        try:
            conn.execute(sql, record)
        except IntegrityError:
            pass


# -------------------------------------------------------------
# Macro series helpers
# -------------------------------------------------------------


def upsert_macro_series(record: Mapping[str, object]) -> None:
    """Insert or update a single macro series point.

    Schema expectation for *macro_series* table::

        CREATE TABLE macro_series (
            series_id  TEXT,
            ts_date    DATE,
            value      DOUBLE PRECISION,
            src        TEXT,
            PRIMARY KEY (series_id, ts_date)
        );
    """

    sql = text(
        """
        INSERT INTO macro_series (
            series_id, ts_date, value, src
        ) VALUES (
            :series_id, :ts_date, :value, :src
        ) ON CONFLICT (series_id, ts_date)
          DO UPDATE SET value = EXCLUDED.value, src = EXCLUDED.src
        """
    )

    with session_scope() as conn:
        try:
            conn.execute(sql, record)
        except IntegrityError:
            pass
