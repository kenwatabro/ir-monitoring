import os
from contextlib import contextmanager
from typing import Iterator, Mapping, Sequence
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
    tokens = val.strip().split()
    return tokens[0] if tokens else default


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


# -------------------------------------------------------------
# Fact & PDF text helpers
# -------------------------------------------------------------


def _ensure_facts_partition(doc_id: str) -> None:
    """指定された doc_id 用のパーティションを作成 (存在しなければ)。

    factsテーブルはdoc_idでRANGEパーティション化されている。
    doc_idの先頭文字に基づいて適切なレンジを設定する。
    """

    # doc_idの先頭文字を取得
    prefix = doc_id[0].upper() if doc_id else "A"

    # 先頭文字に基づいてパーティション名とレンジを決定
    partition_name = f"facts_{prefix}"

    # アルファベット順でレンジを設定
    range_start = prefix
    range_end = chr(ord(prefix) + 1)  # 次の文字 (S -> T)

    # パーティション作成SQL
    create_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {partition_name} 
        PARTITION OF facts 
        FOR VALUES FROM ('{range_start}') TO ('{range_end}')
    """)

    with session_scope() as conn:
        try:
            conn.execute(create_sql)
            import logging

            logger = logging.getLogger(__name__)
            logger.info(
                f"Created facts partition: {partition_name} for range {range_start}-{range_end}"
            )
        except Exception as e:
            # パーティションが既に存在する場合は無視
            import logging

            logger = logging.getLogger(__name__)
            logger.debug(f"Facts partition creation skipped: {e}")
            pass


def _ensure_pdf_texts_partition(doc_id: str) -> None:
    """指定された doc_id 用のパーティションを作成 (存在しなければ)。

    pdf_textsテーブルはdoc_idでRANGEパーティション化されている。
    doc_idの先頭文字に基づいて適切なレンジを設定する。
    """

    # doc_idの先頭文字を取得
    prefix = doc_id[0].upper() if doc_id else "A"

    # 先頭文字に基づいてパーティション名とレンジを決定
    partition_name = f"pdf_texts_{prefix}"

    # アルファベット順でレンジを設定
    range_start = prefix
    range_end = chr(ord(prefix) + 1)  # 次の文字 (S -> T)

    # パーティション作成SQL
    create_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {partition_name} 
        PARTITION OF pdf_texts 
        FOR VALUES FROM ('{range_start}') TO ('{range_end}')
    """)

    with session_scope() as conn:
        try:
            conn.execute(create_sql)
            import logging

            logger = logging.getLogger(__name__)
            logger.info(
                f"Created pdf_texts partition: {partition_name} for range {range_start}-{range_end}"
            )
        except Exception as e:
            # パーティションが既に存在する場合は無視
            import logging

            logger = logging.getLogger(__name__)
            logger.debug(f"PDF texts partition creation skipped: {e}")
            pass


def upsert_facts(records: Sequence[Mapping[str, object]]) -> None:
    """Insert or update multiple XBRL facts.

    Each *record* must contain at least the following keys::

        doc_id, item, context, unit, decimals, value

    The table must have a composite primary key (doc_id, item, context, unit).
    """

    if not records:
        return

    # 必要なパーティションを事前に作成
    processed_prefixes = set()
    for rec in records:
        doc_id = rec.get("doc_id", "")
        if doc_id:
            prefix = doc_id[0].upper() if doc_id else "A"
            if prefix not in processed_prefixes:
                _ensure_facts_partition(doc_id)
                processed_prefixes.add(prefix)

    sql = text(
        """
        INSERT INTO facts (
            doc_id, item, context, unit, decimals, value
        ) VALUES (
            :doc_id, :item, :context, :unit, :decimals, :value
        ) ON CONFLICT (doc_id, item, context, unit)
          DO UPDATE SET
            value    = EXCLUDED.value,
            decimals = EXCLUDED.decimals
        """
    )

    # 各レコードを個別のトランザクションで処理
    for rec in records:
        with session_scope() as conn:
            try:
                conn.execute(sql, rec)
            except Exception as e:
                # 全てのエラーをキャッチしてログに記録
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to upsert fact: {rec}, error: {e}")
                # 処理を継続（他のレコードに影響しないよう）
                pass


def upsert_pdf_texts(records: Sequence[Mapping[str, object]]) -> None:
    """Insert or update extracted PDF page texts.

    Expected columns::

        doc_id, page_no, text, avg_confidence, error_flag, error_type
    """

    if not records:
        return

    # 必要なパーティションを事前に作成
    processed_prefixes = set()
    for rec in records:
        doc_id = rec.get("doc_id", "")
        if doc_id:
            prefix = doc_id[0].upper() if doc_id else "A"
            if prefix not in processed_prefixes:
                _ensure_pdf_texts_partition(doc_id)
                processed_prefixes.add(prefix)

    sql = text(
        """
        INSERT INTO pdf_texts (
            doc_id, page_no, text, avg_confidence, error_flag, error_type
        ) VALUES (
            :doc_id, :page_no, :text, :avg_confidence, :error_flag, :error_type
        ) ON CONFLICT (doc_id, page_no)
          DO UPDATE SET
            text           = EXCLUDED.text,
            avg_confidence = EXCLUDED.avg_confidence,
            error_flag     = EXCLUDED.error_flag,
            error_type     = EXCLUDED.error_type
        """
    )

    # 各レコードを個別のトランザクションで処理
    for rec in records:
        with session_scope() as conn:
            try:
                conn.execute(sql, rec)
            except Exception as e:
                # 全てのエラーをキャッチしてログに記録
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to upsert pdf_text: {rec}, error: {e}")
                # 処理を継続（他のレコードに影響しないよう）
                pass
