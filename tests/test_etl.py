"""
test_etl.py
-----------
Unit tests for the Olist ETL pipeline.
Tests data quality, schema correctness, and business rules.
Run with: pytest tests/test_etl.py -v
"""

import pytest
import pandas as pd
import duckdb
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parents[1]
WAREHOUSE = BASE_DIR / "data/warehouse/olist.duckdb"
PROCESSED = BASE_DIR / "data/processed"


# ── Fixtures ─────────────────────────────────────────────────────────────
@pytest.fixture
def conn():
    """
    Creates a DuckDB connection for each test.
    Closes it automatically when test finishes.
    A fixture is a reusable setup that pytest injects
    into any test function that needs it.
    """
    connection = duckdb.connect(str(WAREHOUSE), read_only=True)
    yield connection
    connection.close()


@pytest.fixture
def orders_df():
    """Loads the processed orders CSV for testing."""
    return pd.read_csv(PROCESSED / "orders_processed.csv")


@pytest.fixture
def customers_df():
    """Loads the processed customers CSV for testing."""
    return pd.read_csv(PROCESSED / "customers_processed.csv")


# ── Tests: Processed CSV files ───────────────────────────────────────────
class TestProcessedFiles:
    """Tests that all 9 processed CSV files exist and are not empty."""

    def test_orders_file_exists(self):
        assert (PROCESSED / "orders_processed.csv").exists()

    def test_customers_file_exists(self):
        assert (PROCESSED / "customers_processed.csv").exists()

    def test_sellers_file_exists(self):
        assert (PROCESSED / "sellers_processed.csv").exists()

    def test_products_file_exists(self):
        assert (PROCESSED / "products_processed.csv").exists()

    def test_payments_file_exists(self):
        assert (PROCESSED / "payments_processed.csv").exists()

    def test_reviews_file_exists(self):
        assert (PROCESSED / "reviews_processed.csv").exists()

    def test_geolocation_file_exists(self):
        assert (PROCESSED / "geolocation_processed.csv").exists()

    def test_order_items_file_exists(self):
        assert (PROCESSED / "order_items_processed.csv").exists()

    def test_category_translation_file_exists(self):
        assert (PROCESSED / "category_translation_processed.csv").exists()

    def test_orders_not_empty(self, orders_df):
        assert len(orders_df) > 0

    def test_customers_not_empty(self, customers_df):
        assert len(customers_df) > 0


# ── Tests: Orders schema ─────────────────────────────────────────────────
class TestOrdersSchema:
    """Tests that orders CSV has correct columns and data types."""

    def test_orders_has_order_id(self, orders_df):
        assert "order_id" in orders_df.columns

    def test_orders_has_customer_id(self, orders_df):
        assert "customer_id" in orders_df.columns

    def test_orders_has_status(self, orders_df):
        assert "order_status" in orders_df.columns

    def test_orders_no_duplicate_ids(self, orders_df):
        assert orders_df["order_id"].duplicated().sum() == 0

    def test_orders_no_null_order_id(self, orders_df):
        assert orders_df["order_id"].isnull().sum() == 0

    def test_orders_row_count(self, orders_df):
        assert len(orders_df) >= 99000


# ── Tests: Warehouse tables ──────────────────────────────────────────────
class TestWarehouseTables:
    """Tests that all warehouse tables exist and have correct row counts."""

    def test_fact_orders_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM fact_orders"
        ).fetchone()[0]
        assert result > 0

    def test_dim_customer_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM dim_customer"
        ).fetchone()[0]
        assert result > 0

    def test_dim_seller_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM dim_seller"
        ).fetchone()[0]
        assert result > 0

    def test_dim_product_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM dim_product"
        ).fetchone()[0]
        assert result > 0

    def test_dim_payment_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM dim_payment"
        ).fetchone()[0]
        assert result > 0

    def test_dim_geolocation_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM dim_geolocation"
        ).fetchone()[0]
        assert result > 0

    def test_dim_date_exists(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM dim_date"
        ).fetchone()[0]
        assert result > 0

    def test_fact_orders_row_count(self, conn):
        result = conn.execute(
            "SELECT COUNT(*) FROM fact_orders"
        ).fetchone()[0]
        assert result >= 99000


# ── Tests: Business rules ────────────────────────────────────────────────
class TestBusinessRules:
    """Tests that data follows business logic rules."""

    def test_no_negative_prices(self, conn):
        result = conn.execute("""
            SELECT COUNT(*) FROM fact_orders
            WHERE total_price < 0
        """).fetchone()[0]
        assert result == 0

    def test_review_scores_valid_range(self, conn):
        result = conn.execute("""
            SELECT COUNT(*) FROM fact_orders
            WHERE review_score < 1
            OR review_score > 5
        """).fetchone()[0]
        assert result == 0

    def test_is_late_binary(self, conn):
        result = conn.execute("""
            SELECT COUNT(*) FROM fact_orders
            WHERE is_late NOT IN (0, 1)
            AND is_late IS NOT NULL
        """).fetchone()[0]
        assert result == 0

    def test_order_status_valid_values(self, conn):
        valid_statuses = (
            "'delivered','shipped','canceled',"
            "'unavailable','invoiced','processing',"
            "'created','approved'"
        )
        result = conn.execute(f"""
            SELECT COUNT(*) FROM fact_orders
            WHERE order_status NOT IN ({valid_statuses})
        """).fetchone()[0]
        assert result == 0


# ── Tests: Referential integrity ─────────────────────────────────────────
class TestReferentialIntegrity:
    """Tests foreign key relationships between tables."""

    def test_customer_ids_in_dim(self, conn):
        result = conn.execute("""
            SELECT COUNT(*) FROM fact_orders f
            LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """).fetchone()[0]
        assert result == 0

    def test_seller_ids_in_dim(self, conn):
        result = conn.execute("""
            SELECT COUNT(*) FROM fact_orders f
            LEFT JOIN dim_seller s ON f.seller_id = s.seller_id
            WHERE s.seller_id IS NULL
            AND f.seller_id IS NOT NULL
        """).fetchone()[0]
        assert result == 0