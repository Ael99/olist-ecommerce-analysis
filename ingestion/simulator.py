import pandas as pd
import os
from loguru import logger
from config import DATA_DIR

# This file remembers where we left off between runs.
# It's just a text file containing a single date, e.g. "2016-09-11"
STATE_FILE = "./data/pipeline_state.txt"


def get_all_orders():
    # WHY: Read the full orders CSV from disk into a pandas DataFrame.
    # We always read the full file — the filtering happens later in get_batch_orders.
    path = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
    df = pd.read_csv(path, parse_dates=["order_purchase_timestamp"])

    # Sort by date so the earliest orders come first
    return df.sort_values("order_purchase_timestamp")


def get_current_window():
    # WHY: Figure out which 7-day window to load this run.
    # Returns (window_start, window_end) as date objects.

    all_orders = get_all_orders()

    # The dataset spans Sep 2016 → Sep 2018
    dataset_start = all_orders["order_purchase_timestamp"].min().date()
    dataset_end   = all_orders["order_purchase_timestamp"].max().date()

    if os.path.exists(STATE_FILE):
        # We've run before — read the last saved date
        # e.g. state file contains "2016-09-11"
        with open(STATE_FILE) as f:
            last_end = pd.to_datetime(f.read().strip()).date()
    else:
        # First ever run — start from the very beginning of the dataset
        last_end = dataset_start

    # This week's window starts where last week ended
    window_start = last_end

    # And ends 7 days later (or at the dataset end, whichever comes first)
    window_end = min(last_end + pd.Timedelta(days=7), dataset_end)

    if window_start >= dataset_end:
        # We've loaded everything — reset back to the beginning
        # so the pipeline keeps running indefinitely
        logger.warning("Dataset fully consumed. Resetting to beginning.")
        window_start = dataset_start
        window_end   = dataset_start + pd.Timedelta(days=7)

    return window_start, window_end


def save_state(window_end):
    # WHY: After a successful run, save window_end to the state file.
    # Next run will pick up from this date.
    # e.g. writes "2016-09-11" to ./data/pipeline_state.txt
    os.makedirs("./data", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(str(window_end))
    logger.info(f"State saved: next run starts from {window_end}")


def get_batch_orders(window_start, window_end):
    # WHY: Filter the full orders DataFrame to only rows that fall
    # within this week's window.
    # e.g. if window is Sep 4-11, only return orders from those 7 days.
    # Note: >= start but < end so dates don't overlap between runs.
    all_orders = get_all_orders()

    mask = (
        (all_orders["order_purchase_timestamp"].dt.date >= window_start) &
        (all_orders["order_purchase_timestamp"].dt.date <  window_end)
    )

    batch = all_orders[mask]
    logger.info(f"Window {window_start} → {window_end}: {len(batch)} orders")
    return batch