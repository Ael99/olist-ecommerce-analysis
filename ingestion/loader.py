import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger
from config import DB_URL


def get_engine():
    # WHY: SQLAlchemy engine is the bridge between Python and SQL Server.
    # We create it once here and reuse it across all functions below.
    # fast_executemany=True dramatically speeds up bulk inserts —
    # instead of sending one row at a time, it batches them together.
    return create_engine(DB_URL, fast_executemany=True)


def insert_new_dim_rows(df, table, pk_col):
    # WHY: We only INSERT rows that don't exist yet — we do NOT update
    # existing rows if their data changed. This is called "insert-only"
    # or "Type 1 SCD ignore" pattern.
    #
    # For example: if seller S001 moved from São Paulo to Rio,
    # our code keeps the old city value — it won't update it.
    #
    # A true UPSERT would update existing rows with new values.
    # A Type 2 SCD would keep full history of changes with start/end dates.
    # Both are more complex — insert-only is fine for a learning project.
    #
    # HOW: Fetch all existing primary keys from SQL Server first,
    # then only insert rows whose pk value isn't already there.
    engine = get_engine()
    with engine.connect() as conn:
        # Pull only the primary key column — no need to fetch all data
        existing = pd.read_sql(f"SELECT {pk_col} FROM {table}", conn)
        existing_ids = set(existing[pk_col].tolist())

        # Keep only rows whose primary key is not already in the table
        new_rows = df[~df[pk_col].isin(existing_ids)]

        if len(new_rows) > 0:
            new_rows.to_sql(table, engine, if_exists="append", index=False)
            logger.info(f"  {table}: +{len(new_rows)} new rows "
                        f"({len(existing_ids)} already existed, skipped)")
        else:
            logger.info(f"  {table}: no new rows to insert, all already exist")


def insert_new_fact_rows(df, table, pk_col):
    # WHY: Same insert-only logic as dimensions.
    # Protects against duplicate orders if the pipeline is re-run
    # for the same window (e.g. after a crash or manual re-run).
    # Without this check, revenue totals in Power BI would be doubled.
    engine = get_engine()
    with engine.connect() as conn:
        existing = pd.read_sql(f"SELECT {pk_col} FROM {table}", conn)
        existing_ids = set(existing[pk_col].tolist())

        new_rows = df[~df[pk_col].isin(existing_ids)]

        if len(new_rows) > 0:
            new_rows.to_sql(table, engine, if_exists="append", index=False)

        logger.info(f"  {table}: +{len(new_rows)} new rows "
                    f"({len(existing_ids)} already existed, skipped)")
        return len(new_rows)


def insert_items(df):
    # WHY: Order items use a different approach — no duplicate check here.
    # fact_order_items has an auto-increment primary key (IDENTITY column)
    # so we can't check for duplicates the same way.
    # Instead we rely on insert_new_fact_rows already filtering out
    # duplicate orders upstream — if an order wasn't inserted,
    # its items won't be inserted either (see main.py logic).
    engine = get_engine()
    df.to_sql("fact_order_items", engine, if_exists="append", index=False)
    logger.info(f"  fact_order_items: +{len(df)} rows")
    return len(df)


def insert_new_date_rows(df):
    # WHY: Date dimension uses the same insert-only pattern.
    # date_key is the primary key (integer like 20160904).
    # Duplicate date rows would break Power BI time intelligence —
    # it expects exactly one row per date in the date table.
    insert_new_dim_rows(df, "dim_date", "date_key")


def log_run(batch_start, batch_end, orders_loaded, items_loaded, status):
    # WHY: Every pipeline run writes one row to ingestion_log.
    # This gives you a full audit trail:
    #   - When did each run happen?
    #   - How many rows were loaded?
    #   - Did it succeed or fail?
    # Invaluable for debugging, and you can show this in Power BI
    # as a "last updated" indicator on your dashboard.
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO ingestion_log 
            (batch_start, batch_end, orders_loaded, items_loaded, status)
            VALUES (:bs, :be, :ol, :il, :st)
        """), {
            "bs": batch_start,
            "be": batch_end,
            "ol": orders_loaded,
            "il": items_loaded,
            "st": status
        })
        conn.commit()