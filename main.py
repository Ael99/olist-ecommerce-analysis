import datetime
from loguru import logger

from ingestion.downloader  import download_dataset
from ingestion.simulator   import get_current_window, save_state, get_batch_orders
from ingestion.transformer import build_dims, build_fact_orders, build_dim_date
from ingestion.loader      import (insert_new_dim_rows, insert_new_fact_rows,
                                   insert_items, insert_new_date_rows, log_run)


def run_pipeline():
    logger.info("=" * 50)
    logger.info(f"Pipeline started: {datetime.datetime.now()}")

    # --- Step 1: Download dataset ---
    # WHY: We call this every run but it only actually downloads once.
    # After the first run, the CSVs exist and it skips immediately.
    # This means main.py is always safe to run without worrying
    # about whether the data is already there or not.
    download_dataset()

    # --- Step 2: Get this week's window ---
    # WHY: The simulator reads the state file to know where we left off,
    # then returns the next 7-day window to process.
    # e.g. window_start=2016-09-04, window_end=2016-09-11
    window_start, window_end = get_current_window()

    # Filter the full orders CSV down to just this week's orders
    batch_orders = get_batch_orders(window_start, window_end)

    if batch_orders.empty:
        # WHY: If no orders fall in this window (e.g. a gap in the data),
        # we skip loading entirely rather than inserting empty tables.
        # We still advance the state so next run moves forward.
        logger.warning("No orders in this window, skipping load.")
        save_state(window_end)
        return

    # --- Step 3: Load dimension tables ---
    # WHY: We load dimensions BEFORE facts because fact tables have
    # foreign keys pointing to dimensions. If we inserted a fact row
    # referencing a customer_id that doesn't exist yet in dim_customers,
    # SQL Server would reject it with a foreign key violation error.
    logger.info("Loading dimension tables...")
    customers, sellers, products = build_dims()

    insert_new_dim_rows(customers, "dim_customers", "customer_id")
    insert_new_dim_rows(sellers,   "dim_sellers",   "seller_id")
    insert_new_dim_rows(products,  "dim_products",  "product_id")

    # --- Step 4: Load date dimension ---
    # WHY: Same reason as above — fact_orders references date_key,
    # so dim_date must be populated first. We build it from the
    # current batch so only relevant dates are added each run.
    logger.info("Loading date dimension...")
    dim_date = build_dim_date(batch_orders)
    insert_new_date_rows(dim_date)

    # --- Step 5: Load fact tables ---
    # WHY: Facts come last — they depend on all dimensions being ready.
    # We pass today's date as ingestion_date so every row in
    # fact_order_items is stamped with which weekly run inserted it.
    logger.info("Loading fact tables...")
    today = datetime.date.today()
    fact_orders, fact_items = build_fact_orders(batch_orders, today)

    orders_loaded = insert_new_fact_rows(fact_orders, "fact_orders", "order_id")

    # WHY: We only insert items if at least some orders were inserted.
    # If all orders already existed (re-run scenario), inserting items
    # would create orphan rows with no matching parent in fact_orders.
    if orders_loaded > 0:
        items_loaded = insert_items(fact_items)
    else:
        items_loaded = 0
        logger.info("  fact_order_items: skipped (no new orders)")

    # --- Step 6: Advance the state window ---
    # WHY: Only save state AFTER successful loading.
    # If the pipeline crashes mid-load, the state file stays at the
    # previous window so the next run retries the same window safely.
    save_state(window_end)

    # --- Step 7: Write audit log ---
    # WHY: Records this run in ingestion_log table for traceability.
    log_run(window_start, window_end, orders_loaded, items_loaded, "success")

    logger.success(
        f"Pipeline complete — "
        f"{orders_loaded} orders and {items_loaded} items loaded "
        f"for window {window_start} → {window_end}"
    )


if __name__ == "__main__":
    # WHY: This block only runs when you execute main.py directly
    # (python main.py). It does NOT run when main.py is imported
    # by scheduler.py — which also calls run_pipeline() but on a schedule.
    run_pipeline()