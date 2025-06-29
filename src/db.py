import os
from contextlib import contextmanager
from typing import Iterator, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# Build PostgreSQL URL from env vars if DATABASE_URL not provided
if url := os.getenv("DATABASE_URL"):
    DB_URL = url
else:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "ir_db")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


@contextmanager
def session_scope() -> Iterator[None]:
    """Provide a transactional scope around a series of operations."""
    with engine.begin() as conn:
        yield conn


def upsert_document(record: Mapping[str, object]) -> None:
    """Insert a row into documents if not exists (by doc_id)."""
    sql = text(
        """
        INSERT INTO documents (
            doc_id, source, doc_type, pub_date, file_path, sha256, size_bytes, xbrl_flag, pdf_flag
        ) VALUES (
            :doc_id, :source, :doc_type, :pub_date, :file_path, :sha256, :size_bytes, :xbrl_flag, :pdf_flag
        ) ON CONFLICT (doc_id) DO NOTHING
        """
    )
    with session_scope() as conn:
        try:
            conn.execute(sql, record)
        except IntegrityError:
            pass
