import pandas as pd
import os
from loguru import logger
from config import DATA_DIR


def load_csv(filename):
    # WHY: Every function below needs to load a CSV from data/raw/.
    # Instead of repeating os.path.join(DATA_DIR, ...) everywhere,
    # we centralize it here. If the folder ever changes, fix it in one place.
    return pd.read_csv(os.path.join(DATA_DIR, filename))

def build_dim_date(orders_df=None):
    # WHY: Power BI requires a continuous date table with no gaps —
    # every single day must have a row, even days with no orders.
    # We hardcode the full Olist dataset range so dim_date always
    # covers the entire possible data range regardless of which
    # weekly window is currently being loaded.

    start_date = "2016-09-01"
    end_date   = "2018-10-31"

    # Generate every single day in the range — no gaps
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({"full_date": dates})
    df["date_key"]     = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]         = df["full_date"].dt.year
    df["quarter"]      = df["full_date"].dt.quarter
    df["month"]        = df["full_date"].dt.month
    df["month_name"]   = df["full_date"].dt.strftime("%B")
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["day_of_week"]  = df["full_date"].dt.day_name()
    df["is_weekend"]   = df["full_date"].dt.dayofweek >= 5

    return df[[
        "date_key", "full_date", "year", "quarter", "month",
        "month_name", "week_of_year", "day_of_week", "is_weekend"
    ]]

def build_dims():
    # WHY: Dimension tables store the "who/what/where" context of your data.
    # They change rarely (a product's category doesn't change every week).
    # By separating them from facts, we avoid repeating "Electronics" or
    # "São Paulo" thousands of times in the fact table — we just store
    # the ID and join when needed. This saves storage and keeps data consistent.

    # --- Customers ---
    # WHY: We keep customer_unique_id alongside customer_id because in Olist,
    # the same real person can have multiple customer_id values across orders
    # (Olist generates a new customer_id per order for privacy reasons).
    # customer_unique_id is the true unique person identifier — useful for
    # calculating real customer retention and repeat purchase rates.
    customers = load_csv("olist_customers_dataset.csv")[[
        "customer_id", "customer_unique_id", "customer_city", "customer_state"
    ]].drop_duplicates("customer_id")

    # --- Sellers ---
    # WHY: Seller location lets us analyze whether sellers in certain states
    # deliver faster, or whether geography affects review scores.
    sellers = load_csv("olist_sellers_dataset.csv")[[
        "seller_id", "seller_city", "seller_state"
    ]].drop_duplicates("seller_id")

    # --- Products ---
    products = load_csv("olist_products_dataset.csv")

    # WHY: The raw CSV has a typo ("lenght" instead of "length") and a verbose
    # column name ("product_category_name" instead of "product_category").
    # We fix them here in the pipeline so the database and Power BI always
    # see clean, correct column names — not the raw data's mistakes.
    products = products.rename(columns={
        "product_category_name":      "product_category",
        "product_name_lenght":        "product_name_length",
        "product_description_lenght": "product_description_length"
    })

    products = products[[
        "product_id", "product_category",
        "product_name_length", "product_description_length",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]].drop_duplicates("product_id")

    # Fill blank categories with "uncategorized" so they don't show as (Blank) in Power BI
    products["product_category"] = products["product_category"].fillna("uncategorized")

    return customers, sellers, products


def build_fact_orders(orders_batch, ingestion_date):
    # WHY: Fact tables store the "what happened" — every order event with
    # numbers we can measure (revenue, quantity, delivery days).
    # We enrich each order by joining payments, reviews, and items
    # so Power BI only needs to query one table for most calculations,
    # rather than joining multiple tables every time.

    payments = load_csv("olist_order_payments_dataset.csv")
    reviews  = load_csv("olist_order_reviews_dataset.csv")
    items    = load_csv("olist_order_items_dataset.csv")

    order_ids = orders_batch["order_id"].tolist()

    # --- Payments ---
    # WHY: Olist allows split payments (e.g. part credit card, part voucher),
    # so one order can have 2-3 payment rows. We sum them into one total
    # so fact_orders has exactly one row per order — a clean grain.
    # "Grain" means what one row represents — here, one row = one order.
    pay_agg = (
        payments[payments["order_id"].isin(order_ids)]
        .groupby("order_id")
        .agg(total_payment_value=("payment_value", "sum"))
        .reset_index()
    )

    # --- Reviews ---
    # WHY: Similarly, an order can have multiple reviews if the customer
    # edited their review. We take only the most recent one to avoid
    # double-counting or inflating review averages in Power BI.
    rev_agg = (
        reviews[reviews["order_id"].isin(order_ids)]
        .sort_values("review_answer_timestamp", ascending=False)
        .drop_duplicates("order_id")[["order_id", "review_score"]]
    )

    # --- Items ---
    # WHY: We aggregate item count and freight at the order level for
    # fact_orders (one row per order), but we ALSO keep the raw item
    # rows for fact_order_items (one row per product per order).
    # This gives Power BI flexibility — analyze at order level OR
    # drill down to individual product performance.
    items_batch = items[items["order_id"].isin(order_ids)].copy()
    item_agg = (
        items_batch
        .groupby("order_id")
        .agg(
            order_item_count=("order_item_id", "count"),
            total_freight_value=("freight_value", "sum")
        )
        .reset_index()
    )

    # --- Merge everything ---
    # WHY: left join keeps ALL orders even if they have no payment or review yet
    # (e.g. a pending order has no delivery date). Inner join would silently
    # drop those orders, causing gaps in your data you might not notice.
    fact = orders_batch.copy()
    fact = fact.merge(pay_agg,  on="order_id", how="left")
    fact = fact.merge(rev_agg,  on="order_id", how="left")
    fact = fact.merge(item_agg, on="order_id", how="left")

    # --- Parse dates ---
    # WHY: CSV files store everything as text. We convert date columns to
    # proper datetime objects so we can do date math below (subtraction etc).
    # errors="coerce" turns invalid/missing dates into NaT (null)
    # instead of crashing the whole pipeline on one bad row.
    for col in [
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "order_approved_at"
    ]:
        fact[col] = pd.to_datetime(fact[col], errors="coerce")

    # --- Convert dates to integer date keys ---
    # WHY: fact_orders stores date_key integers (e.g. 20160904) to join
    # with dim_date, not raw timestamps. This is the standard way to
    # link fact tables to date dimensions in a data warehouse.
    # Int64 (capital I) is pandas nullable integer — allows NULL values,
    # unlike int64 which would crash on NaT dates.
    def to_key(series):
        return (
            series.dt.strftime("%Y%m%d")
            .where(series.notna(), None)
            .astype("Int64")
        )

    fact["purchase_date_key"]           = to_key(fact["order_purchase_timestamp"])
    fact["approved_date_key"]           = to_key(fact["order_approved_at"])
    fact["delivered_date_key"]          = to_key(fact["order_delivered_customer_date"])
    fact["estimated_delivery_date_key"] = to_key(fact["order_estimated_delivery_date"])

    # --- Delivery metrics ---
    # WHY: Pre-calculating these in Python saves Power BI from doing it
    # at query time on every dashboard refresh. Faster dashboards,
    # and the logic lives in one place (here) not scattered across DAX.

    # How many total days from clicking "buy" to receiving the package?
    fact["days_to_deliver"] = (
        fact["order_delivered_customer_date"] -
        fact["order_purchase_timestamp"]
    ).dt.days

    # WHY: This metric directly measures delivery promise accuracy.
    # Negative = delivered BEFORE the estimate (good), positive = LATE (bad).
    # Great for an operations dashboard showing seller reliability.
    fact["delivery_vs_estimate_days"] = (
        fact["order_delivered_customer_date"] -
        fact["order_estimated_delivery_date"]
    ).dt.days

    fact_orders_final = fact[[
        "order_id", "customer_id", "order_status",
        "purchase_date_key", "approved_date_key",
        "delivered_date_key", "estimated_delivery_date_key",
        "order_item_count", "total_payment_value",
        "total_freight_value", "review_score",
        "days_to_deliver", "delivery_vs_estimate_days"
    ]]

    # --- Order items fact table ---
    # WHY: We store ingestion_batch (today's date) on every row so we can
    # track exactly which weekly pipeline run inserted each record.
    # Useful for auditing, debugging, and showing data freshness in Power BI.
    items_batch["ingestion_batch"] = ingestion_date
    fact_items_final = items_batch[[
        "order_id", "product_id", "seller_id",
        "price", "freight_value", "ingestion_batch"
    ]]

    return fact_orders_final, fact_items_final