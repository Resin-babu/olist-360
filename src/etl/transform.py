"""
transform.py
------------
Builds the Star Schema warehouse inside DuckDB.
Takes the 9 processed CSVs and creates:
- 1 fact table   : fact_orders
- 6 dim tables   : dim_customer, dim_product, dim_seller,
                   dim_date, dim_payment, dim_geolocation
"""

import duckdb
import pandas as pd
import logging
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parents[2]
PROCESSED = BASE_DIR / "data/processed"
WAREHOUSE = BASE_DIR / "data/warehouse/olist.duckdb"

LOGS = BASE_DIR / "logs"
LOGS.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler(LOGS / "pipeline.log")
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# ── Connect to DuckDB ────────────────────────────────────────────────────
def get_connection():
    """
    Creates and returns a DuckDB connection.
    DuckDB stores everything in a single file — olist.duckdb
    If the file doesn't exist, DuckDB creates it automatically.
    """
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(WAREHOUSE))
    logger.info(f"CONNECTED TO WAREHOUSE: {WAREHOUSE}")
    return conn


# ── Load processed CSVs ──────────────────────────────────────────────────
def load_processed() -> dict:
    """
    Reads all 9 processed CSVs from data/processed/
    Returns them as a dictionary of DataFrames.
    Same pattern as your ingestion style.
    """
    tables = {}
    files = {
        "orders":               "orders_processed.csv",
        "order_items":          "order_items_processed.csv",
        "customers":            "customers_processed.csv",
        "sellers":              "sellers_processed.csv",
        "products":             "products_processed.csv",
        "payments":             "payments_processed.csv",
        "reviews":              "reviews_processed.csv",
        "geolocation":          "geolocation_processed.csv",
        "category_translation": "category_translation_processed.csv",
    }
    for name, filename in files.items():
        path = PROCESSED / filename
        tables[name] = pd.read_csv(path)
        logger.info(f"LOADED {filename}")
    return tables


# ── Build Dimension Tables ───────────────────────────────────────────────
def build_dim_customer(conn, tables: dict):
    """
    dim_customer — WHO bought
    Contains unique customer info.
    """
    df = tables["customers"][[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    ]].drop_duplicates()

    conn.execute("DROP TABLE IF EXISTS dim_customer")
    conn.execute("""
        CREATE TABLE dim_customer AS
        SELECT * FROM df
    """)
    logger.info(f"dim_customer CREATED — {len(df)} rows")


def build_dim_seller(conn, tables: dict):
    """
    dim_seller — WHO sold
    Contains unique seller info.
    """
    df = tables["sellers"][[
        "seller_id",
        "seller_city",
        "seller_state"
    ]].drop_duplicates()

    conn.execute("DROP TABLE IF EXISTS dim_seller")
    conn.execute("""
        CREATE TABLE dim_seller AS
        SELECT * FROM df
    """)
    logger.info(f"dim_seller CREATED — {len(df)} rows")


def build_dim_product(conn, tables: dict):
    """
    dim_product — WHAT was bought
    Joins products with English category names.
    """
    df = tables["products"].merge(
        tables["category_translation"],
        on="product_category_name",
        how="left"
    )[[
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]].drop_duplicates()

    conn.execute("DROP TABLE IF EXISTS dim_product")
    conn.execute("""
        CREATE TABLE dim_product AS
        SELECT * FROM df
    """)
    logger.info(f"dim_product CREATED — {len(df)} rows")


def build_dim_date(conn, tables: dict):
    """
    dim_date — WHEN it happened
    Extracts date parts from order_purchase_timestamp.
    Year, month, day, weekday — useful for time based analysis.
    """
    df = tables["orders"][["order_purchase_timestamp"]].copy()
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    )
    df = df.dropna(subset=["order_purchase_timestamp"])
    df = df.drop_duplicates()

    # Extract useful date parts
    df["date"]       = df["order_purchase_timestamp"].dt.date
    df["year"]       = df["order_purchase_timestamp"].dt.year
    df["month"]      = df["order_purchase_timestamp"].dt.month
    df["month_name"] = df["order_purchase_timestamp"].dt.strftime("%B")
    df["day"]        = df["order_purchase_timestamp"].dt.day
    df["weekday"]    = df["order_purchase_timestamp"].dt.strftime("%A")
    df["quarter"]    = df["order_purchase_timestamp"].dt.quarter

    conn.execute("DROP TABLE IF EXISTS dim_date")
    conn.execute("""
        CREATE TABLE dim_date AS
        SELECT * FROM df
    """)
    logger.info(f"dim_date CREATED — {len(df)} rows")


def build_dim_payment(conn, tables: dict):
    """
    dim_payment — HOW they paid
    Payment type and installment info per order.
    """
    df = tables["payments"][[
        "order_id",
        "payment_type",
        "payment_installments",
        "payment_value"
    ]].drop_duplicates()

    conn.execute("DROP TABLE IF EXISTS dim_payment")
    conn.execute("""
        CREATE TABLE dim_payment AS
        SELECT * FROM df
    """)
    logger.info(f"dim_payment CREATED — {len(df)} rows")


def build_dim_geolocation(conn, tables: dict):
    """
    dim_geolocation — WHERE they are
    Average lat/lng per zip code prefix and state.
    """
    df = tables["geolocation"].groupby(
        ["geolocation_zip_code_prefix", "geolocation_state"]
    ).agg(
        avg_lat=("geolocation_lat", "mean"),
        avg_lng=("geolocation_lng", "mean"),
        city=("geolocation_city", "first")
    ).reset_index()

    conn.execute("DROP TABLE IF EXISTS dim_geolocation")
    conn.execute("""
        CREATE TABLE dim_geolocation AS
        SELECT * FROM df
    """)
    logger.info(f"dim_geolocation CREATED — {len(df)} rows")


# ── Build Fact Table ─────────────────────────────────────────────────────
def build_fact_orders(conn, tables: dict):
    """
    fact_orders — the CENTER of the star
    Joins orders + order_items + reviews together.
    This is the main table every analysis will query.
    """
    orders     = tables["orders"]
    items      = tables["order_items"]
    reviews    = tables["reviews"]
    customers  = tables["customers"]

    # Fix date columns
    for col in ["order_purchase_timestamp", "order_delivered_customer_date",
                "order_estimated_delivery_date"]:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # Calculate if order was delivered late
    orders["is_late"] = (
        orders["order_delivered_customer_date"] >
        orders["order_estimated_delivery_date"]
    ).astype(int)
    # 1 = late, 0 = on time

    # Aggregate items per order
    items_agg = items.groupby("order_id").agg(
        total_price=("price", "sum"),
        total_freight=("freight_value", "sum"),
        item_count=("order_item_id", "count"),
        seller_id=("seller_id", "first"),
        product_id=("product_id", "first")
    ).reset_index()

    # Get one review score per order
    reviews_agg = reviews.groupby("order_id").agg(
        review_score=("review_score", "mean")
    ).reset_index()

    # Join everything together
    fact = orders.merge(items_agg, on="order_id", how="left")
    fact = fact.merge(reviews_agg, on="order_id", how="left")
    fact = fact.merge(
        customers[["customer_id", "customer_state"]],
        on="customer_id", how="left"
    )

    # Keep only the columns we need
    fact = fact[[
        "order_id",
        "customer_id",
        "seller_id",
        "product_id",
        "order_purchase_timestamp",
        "order_status",
        "total_price",
        "total_freight",
        "item_count",
        "review_score",
        "is_late",
        "customer_state",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]]

    conn.execute("DROP TABLE IF EXISTS fact_orders")
    conn.execute("""
        CREATE TABLE fact_orders AS
        SELECT * FROM fact
    """)
    logger.info(f"fact_orders CREATED — {len(fact)} rows")


# ── Run all ──────────────────────────────────────────────────────────────
def run_all():
    logger.info("STARTING WAREHOUSE BUILD...")

    conn   = get_connection()
    tables = load_processed()

    build_dim_customer(conn, tables)
    build_dim_seller(conn, tables)
    build_dim_product(conn, tables)
    build_dim_date(conn, tables)
    build_dim_payment(conn, tables)
    build_dim_geolocation(conn, tables)
    build_fact_orders(conn, tables)

    # Show all tables created in warehouse
    result = conn.execute("SHOW TABLES").fetchall()
    logger.info(f"WAREHOUSE TABLES: {[r[0] for r in result]}")

    conn.close()
    logger.info("WAREHOUSE BUILD COMPLETE")


if __name__ == "__main__":
    run_all()