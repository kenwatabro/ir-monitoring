import os
from contextlib import contextmanager
from typing import Iterator, Mapping, Sequence, List
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


# ============================================================================
# Star-Schema Support Functions
# ============================================================================


def upsert_company(company_data: Mapping[str, object]) -> None:
    """Insert or update company dimension data.

    Expected keys: code_jpx, name_ja, name_en, sector, industry, market, etc.
    """
    sql = text("""
        INSERT INTO dim_company (
            code_jpx, name_ja, name_en, sector, industry, market,
            listing_date, fiscal_year_end, employee_count, updated_at
        ) VALUES (
            :code_jpx, :name_ja, :name_en, :sector, :industry, :market,
            :listing_date, :fiscal_year_end, :employee_count, CURRENT_TIMESTAMP
        ) ON CONFLICT (code_jpx)
          DO UPDATE SET
            name_ja = EXCLUDED.name_ja,
            name_en = EXCLUDED.name_en,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            market = EXCLUDED.market,
            listing_date = EXCLUDED.listing_date,
            fiscal_year_end = EXCLUDED.fiscal_year_end,
            employee_count = EXCLUDED.employee_count,
            updated_at = CURRENT_TIMESTAMP
    """)

    with session_scope() as conn:
        conn.execute(sql, company_data)


def upsert_dim_document(doc_data: Mapping[str, object]) -> None:
    """Insert or update document dimension data (Star-Schema).

    Expected keys: doc_id, code_jpx, doc_type, pub_date, fiscal_year, etc.
    """
    sql = text("""
        INSERT INTO dim_doc (
            doc_id, code_jpx, doc_type, pub_date, fiscal_year,
            period_type, period_end, xbrl_flag, pdf_flag, file_size_bytes
        ) VALUES (
            :doc_id, :code_jpx, :doc_type, :pub_date, :fiscal_year,
            :period_type, :period_end, :xbrl_flag, :pdf_flag, :file_size_bytes
        ) ON CONFLICT (doc_id)
          DO UPDATE SET
            code_jpx = EXCLUDED.code_jpx,
            doc_type = EXCLUDED.doc_type,
            pub_date = EXCLUDED.pub_date,
            fiscal_year = EXCLUDED.fiscal_year,
            period_type = EXCLUDED.period_type,
            period_end = EXCLUDED.period_end,
            xbrl_flag = EXCLUDED.xbrl_flag,
            pdf_flag = EXCLUDED.pdf_flag,
            file_size_bytes = EXCLUDED.file_size_bytes
    """)

    with session_scope() as conn:
        conn.execute(sql, doc_data)


def upsert_finance_facts(doc_id: str, finance_facts: List) -> None:
    """Insert or update finance facts using FinanceFact objects.

    Args:
        doc_id: Document ID to associate with facts
        finance_facts: List of FinanceFact objects from xbrl parser
    """
    if not finance_facts:
        return

    # Convert FinanceFact objects to dict format for SQL execution
    fact_dicts = []
    for fact in finance_facts:
        fact_dict = {
            "doc_id": doc_id,
            "metric_id": fact.metric_id,
            "period_end": fact.period_end,
            "context_id": fact.context_id,
            "unit": fact.unit,
            "decimals": fact.decimals,
            "value_raw": fact.value_raw,
            "value_converted": fact.value_converted,
            "period_type": fact.period_type,
        }
        fact_dicts.append(fact_dict)

    sql = text("""
        INSERT INTO fact_finance (
            doc_id, metric_id, period_end, context_id, unit, decimals,
            value_raw, value_converted, period_type
        ) VALUES (
            :doc_id, :metric_id, :period_end, :context_id, :unit, :decimals,
            :value_raw, :value_converted, :period_type
        ) ON CONFLICT (doc_id, metric_id, context_id)
          DO UPDATE SET
            period_end = EXCLUDED.period_end,
            unit = EXCLUDED.unit,
            decimals = EXCLUDED.decimals,
            value_raw = EXCLUDED.value_raw,
            value_converted = EXCLUDED.value_converted,
            period_type = EXCLUDED.period_type
    """)

    # Batch insert with individual error handling
    for fact_dict in fact_dicts:
        with session_scope() as conn:
            try:
                conn.execute(sql, fact_dict)
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Failed to upsert finance fact: {fact_dict}, error: {e}"
                )
                pass


def upsert_metric_aliases(aliases: Sequence[Mapping[str, object]]) -> None:
    """Insert or update XBRL tag to metric_id aliases.

    Expected keys: xbrl_tag, metric_id, taxonomy, confidence
    """
    if not aliases:
        return

    sql = text("""
        INSERT INTO dim_metric_alias (
            xbrl_tag, metric_id, taxonomy, confidence
        ) VALUES (
            :xbrl_tag, :metric_id, :taxonomy, :confidence
        ) ON CONFLICT (xbrl_tag)
          DO UPDATE SET
            metric_id = EXCLUDED.metric_id,
            taxonomy = EXCLUDED.taxonomy,
            confidence = EXCLUDED.confidence
    """)

    for alias in aliases:
        with session_scope() as conn:
            try:
                conn.execute(sql, alias)
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to upsert metric alias: {alias}, error: {e}")
                pass


def get_finance_facts_by_company(code_jpx: str, limit: int = 100) -> List[Mapping]:
    """Retrieve finance facts for a specific company using Star-Schema view.

    Returns data from vw_finance_facts view with company and metric details.
    """
    sql = text("""
        SELECT 
            doc_id, code_jpx, company_name, pub_date, fiscal_year,
            metric_name_ja, category, value_raw, value_converted, unit,
            period_end, period_type
        FROM vw_finance_facts
        WHERE code_jpx = :code_jpx
        ORDER BY pub_date DESC, metric_name_ja
        LIMIT :limit
    """)

    with session_scope() as conn:
        result = conn.execute(sql, {"code_jpx": code_jpx, "limit": limit})
        return [dict(row._mapping) for row in result.fetchall()]


def get_latest_finance_metrics(
    metric_ids: List[str] = None, limit: int = 1000
) -> List[Mapping]:
    """Get latest finance metrics across all companies.

    Args:
        metric_ids: Optional list of metric IDs to filter by
        limit: Maximum number of results

    Returns:
        List of latest finance facts with company details
    """
    where_clause = ""
    params = {"limit": limit}

    if metric_ids:
        placeholders = ",".join(f":metric_{i}" for i in range(len(metric_ids)))
        where_clause = f"AND ff.metric_id IN ({placeholders})"
        for i, metric_id in enumerate(metric_ids):
            params[f"metric_{i}"] = metric_id

    sql = text(f"""
        SELECT * FROM vw_latest_finance
        WHERE 1=1 {where_clause}
        ORDER BY pub_date DESC, code_jpx, metric_name_ja
        LIMIT :limit
    """)

    with session_scope() as conn:
        result = conn.execute(sql, params)
        return [dict(row._mapping) for row in result.fetchall()]


# ============================================================================
# Utility Functions for Star-Schema Migration
# ============================================================================


def migrate_legacy_to_star_schema(doc_id: str) -> bool:
    """Migrate legacy facts data to star-schema for a specific document.

    This function reads from the old 'facts' table and converts to 'fact_finance'.
    Used during gradual migration phase.

    Returns:
        True if migration successful, False otherwise
    """
    try:
        # Read legacy facts
        legacy_sql = text("""
            SELECT doc_id, item, context, unit, decimals, value
            FROM facts
            WHERE doc_id = :doc_id
        """)

        with session_scope() as conn:
            legacy_facts = conn.execute(legacy_sql, {"doc_id": doc_id}).fetchall()

            if not legacy_facts:
                return True  # Nothing to migrate

            # Convert to star-schema format
            for fact in legacy_facts:
                # Map legacy 'item' to metric_id (simple conversion)
                metric_id = fact.item.upper().replace(" ", "_")

                # Try to convert value to float
                try:
                    value_raw = float(fact.value)
                except (ValueError, TypeError):
                    continue  # Skip non-numeric values

                star_fact = {
                    "doc_id": fact.doc_id,
                    "metric_id": metric_id,
                    "period_end": None,  # Would need context parsing
                    "context_id": fact.context,
                    "unit": fact.unit,
                    "decimals": fact.decimals,
                    "value_raw": value_raw,
                    "value_converted": None,  # Would need unit conversion
                    "period_type": None,  # Would need context parsing
                }

                # Insert into star-schema
                star_sql = text("""
                    INSERT INTO fact_finance (
                        doc_id, metric_id, context_id, unit, decimals, value_raw
                    ) VALUES (
                        :doc_id, :metric_id, :context_id, :unit, :decimals, :value_raw
                    ) ON CONFLICT (doc_id, metric_id, context_id) DO NOTHING
                """)

                conn.execute(star_sql, star_fact)

        return True

    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Failed to migrate legacy data for {doc_id}: {e}")
        return False
