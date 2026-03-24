"""
extract.py
----------
Ingests all 9 Olist CSV files from data/raw/
Cleans column names, fixes data types, and saves to data/processed/
"""

import pandas as pd
import logging
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
# __file__ is src/etl/extract.py
# .parents[2] goes up 2 levels to olist-360gith/

RAW       = BASE_DIR / "data/raw"
PROCESSED = BASE_DIR / "data/processed"
LOGS      = BASE_DIR / "logs"

PROCESSED.mkdir(parents=True, exist_ok=True)
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



def ingestion(file_path: Path) -> pd.DataFrame:
    """Reads a CSV file and returns a DataFrame."""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"INGESTION COMPLETED {file_path.name} | ROWS: {df.shape[0]} | COLS: {df.shape[1]}")
        return df
    except FileNotFoundError:
        logger.error(f"INGESTION FAILED | {file_path} NOT FOUND")
        raise
    except Exception as e:
        logger.error(f"INGESTION FAILED | {e}")
        raise


def standardization(df: pd.DataFrame) -> pd.DataFrame:
    """Strips and lowercases all column names, replaces spaces with underscores."""
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def save(df: pd.DataFrame, filename: str) -> None:
    """Saves a processed DataFrame to data/processed/"""
    output = PROCESSED / filename
    df.to_csv(output, index=False)
    logger.info(f"SAVED: {output}")



def ingest_orders() -> pd.DataFrame:
    df = ingestion(RAW / "olist_orders_dataset.csv")
    df = standardization(df)
    # Fix all date columns
    date_cols = ["order_purchase_timestamp", "order_approved_at",
                 "order_delivered_carrier_date", "order_delivered_customer_date",
                 "order_estimated_delivery_date"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    logger.info("ORDERS INGESTION COMPLETED")
    return df


def ingest_order_items() -> pd.DataFrame:
    df = ingestion(RAW / "olist_order_items_dataset.csv")
    df = standardization(df)
    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")
    numeric_cols = ["price", "freight_value"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info("ORDER ITEMS INGESTION COMPLETED")
    return df


def ingest_customers() -> pd.DataFrame:
    df = ingestion(RAW / "olist_customers_dataset.csv")
    df = standardization(df)
    logger.info("CUSTOMERS INGESTION COMPLETED")
    return df


def ingest_sellers() -> pd.DataFrame:
    df = ingestion(RAW / "olist_sellers_dataset.csv")
    df = standardization(df)
    logger.info("SELLERS INGESTION COMPLETED")
    return df


def ingest_products() -> pd.DataFrame:
    df = ingestion(RAW / "olist_products_dataset.csv")
    df = standardization(df)
    numeric_cols = ["product_name_lenght", "product_description_lenght",
                    "product_photos_qty", "product_weight_g",
                    "product_length_cm", "product_height_cm", "product_width_cm"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info("PRODUCTS INGESTION COMPLETED")
    return df


def ingest_payments() -> pd.DataFrame:
    df = ingestion(RAW / "olist_order_payments_dataset.csv")
    df = standardization(df)
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce")
    logger.info("PAYMENTS INGESTION COMPLETED")
    return df


def ingest_reviews() -> pd.DataFrame:
    df = ingestion(RAW / "olist_order_reviews_dataset.csv")
    df = standardization(df)
    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
    date_cols = ["review_creation_date", "review_answer_timestamp"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    logger.info("REVIEWS INGESTION COMPLETED")
    return df


def ingest_geolocation() -> pd.DataFrame:
    df = ingestion(RAW / "olist_geolocation_dataset.csv")
    df = standardization(df)
    df["geolocation_lat"] = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"] = pd.to_numeric(df["geolocation_lng"], errors="coerce")
    logger.info("GEOLOCATION INGESTION COMPLETED")
    return df


def ingest_category_translation() -> pd.DataFrame:
    df = ingestion(RAW / "product_category_name_translation.csv")
    df = standardization(df)
    logger.info("CATEGORY TRANSLATION INGESTION COMPLETED")
    return df



def run_all():
    logger.info("STARTING FULL INGESTION PIPELINE...")

    tables = {
        "orders":               ingest_orders(),
        "order_items":          ingest_order_items(),
        "customers":            ingest_customers(),
        "sellers":              ingest_sellers(),
        "products":             ingest_products(),
        "payments":             ingest_payments(),
        "reviews":              ingest_reviews(),
        "geolocation":          ingest_geolocation(),
        "category_translation": ingest_category_translation(),
    }

    for name, df in tables.items():
        save(df, f"{name}_processed.csv")

    logger.info(f"PIPELINE COMPLETE — {len(tables)} tables ingested and saved")


if __name__ == "__main__":
    run_all()