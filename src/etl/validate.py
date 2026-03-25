"""
validate.py
-----------
Runs data quality checks on the Olist warehouse.
Checks for nulls, duplicates, referential integrity,
and business rule violations.
Produces a final data quality score.
"""

import duckdb
import logging
from pathlib import Path
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parents[2]
WAREHOUSE = BASE_DIR / "data/warehouse/olist.duckdb"
LOGS      = BASE_DIR / "logs"
LOGS.mkdir(exist_ok=True)

# ── Logging — prints to terminal AND saves to file ───────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Terminal handler — prints to screen
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
))

# File handler — saves to logs/validation.log
file_handler = logging.FileHandler(LOGS / "validation.log")
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
))

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# ── Connect ──────────────────────────────────────────────────────────────
def get_connection():
    conn = duckdb.connect(str(WAREHOUSE))
    logger.info("CONNECTED TO WAREHOUSE")
    return conn


# ── Check 1 — Null Checks ────────────────────────────────────────────────
def check_nulls(conn) -> list:
    """
    Checks critical columns that should never be null.
    Returns a list of failed checks.
    """
    logger.info("RUNNING NULL CHECKS...")
    failures = []

    # Define which columns must never be null
    critical_columns = {
        "fact_orders":   ["order_id", "customer_id", "order_status"],
        "dim_customer":  ["customer_id", "customer_state"],
        "dim_seller":    ["seller_id", "seller_state"],
        "dim_product":   ["product_id"],
        "dim_payment":   ["order_id", "payment_type", "payment_value"],
    }

    for table, columns in critical_columns.items():
        for col in columns:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE {col} IS NULL
            """).fetchone()[0]

            if result > 0:
                msg = f"NULL CHECK FAILED | {table}.{col} | {result} null values"
                logger.warning(msg)
                failures.append(msg)
            else:
                logger.info(f"NULL CHECK PASSED | {table}.{col}")

    return failures


# ── Check 2 — Duplicate Checks ───────────────────────────────────────────
def check_duplicates(conn) -> list:
    """
    Checks for duplicate primary keys.
    Returns a list of failed checks.
    """
    logger.info("RUNNING DUPLICATE CHECKS...")
    failures = []

    # Each table and its primary key
    primary_keys = {
        "fact_orders":  "order_id",
        "dim_customer": "customer_id",
        "dim_seller":   "seller_id",
        "dim_product":  "product_id",
    }

    for table, pk in primary_keys.items():
        result = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {pk}
                HAVING cnt > 1
            )
        """).fetchone()[0]

        if result > 0:
            msg = f"DUPLICATE CHECK FAILED | {table}.{pk} | {result} duplicate keys"
            logger.warning(msg)
            failures.append(msg)
        else:
            logger.info(f"DUPLICATE CHECK PASSED | {table}.{pk}")

    return failures


# ── Check 3 — Referential Integrity ──────────────────────────────────────
def check_referential_integrity(conn) -> list:
    """
    Checks that foreign keys in fact_orders
    exist in their dimension tables.
    Returns a list of failed checks.
    """
    logger.info("RUNNING REFERENTIAL INTEGRITY CHECKS...")
    failures = []

    checks = [
        # (description, SQL query)
        (
            "fact_orders.customer_id -> dim_customer.customer_id",
            """
            SELECT COUNT(*) FROM fact_orders f
            LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        ),
        (
            "fact_orders.seller_id -> dim_seller.seller_id",
            """
            SELECT COUNT(*) FROM fact_orders f
            LEFT JOIN dim_seller s ON f.seller_id = s.seller_id
            WHERE s.seller_id IS NULL
            AND f.seller_id IS NOT NULL
            """
        ),
        (
            "fact_orders.product_id -> dim_product.product_id",
            """
            SELECT COUNT(*) FROM fact_orders f
            LEFT JOIN dim_product p ON f.product_id = p.product_id
            WHERE p.product_id IS NULL
            AND f.product_id IS NOT NULL
            """
        ),
    ]

    for description, query in checks:
        result = conn.execute(query).fetchone()[0]
        if result > 0:
            msg = f"INTEGRITY CHECK FAILED | {description} | {result} orphan records"
            logger.warning(msg)
            failures.append(msg)
        else:
            logger.info(f"INTEGRITY CHECK PASSED | {description}")

    return failures


# ── Check 4 — Business Rules ─────────────────────────────────────────────
def check_business_rules(conn) -> list:
    """
    Checks for impossible values that violate business logic.
    Returns a list of failed checks.
    """
    logger.info("RUNNING BUSINESS RULE CHECKS...")
    failures = []

    rules = [
        (
            "Negative prices",
            "SELECT COUNT(*) FROM fact_orders WHERE total_price < 0"
        ),
        (
            "Zero or negative freight",
            "SELECT COUNT(*) FROM fact_orders WHERE total_freight < 0"
        ),
        (
            "Delivery before purchase",
            """
            SELECT COUNT(*) FROM fact_orders
            WHERE order_delivered_customer_date < order_purchase_timestamp
            AND order_delivered_customer_date IS NOT NULL
            """
        ),
        (
            "Invalid review scores",
            """
            SELECT COUNT(*) FROM fact_orders
            WHERE review_score < 1 OR review_score > 5
            AND review_score IS NOT NULL
            """
        ),
    ]

    for description, query in rules:
        result = conn.execute(query).fetchone()[0]
        if result > 0:
            msg = f"BUSINESS RULE FAILED | {description} | {result} violations"
            logger.warning(msg)
            failures.append(msg)
        else:
            logger.info(f"BUSINESS RULE PASSED | {description}")

    return failures


# ── Data Quality Score ────────────────────────────────────────────────────
def calculate_quality_score(all_failures: list, total_checks: int) -> float:
    """
    Calculates overall data quality score as a percentage.
    Score = (passed checks / total checks) * 100
    """
    passed = total_checks - len(all_failures)
    score = (passed / total_checks) * 100
    return round(score, 2)


# ── Run All ───────────────────────────────────────────────────────────────
def run_all():
    logger.info("=" * 50)
    logger.info("STARTING DATA VALIDATION PIPELINE")
    logger.info("=" * 50)

    conn = get_connection()

    # Run all checks and collect failures
    null_failures      = check_nulls(conn)
    duplicate_failures = check_duplicates(conn)
    integrity_failures = check_referential_integrity(conn)
    rule_failures      = check_business_rules(conn)

    all_failures = (
        null_failures +
        duplicate_failures +
        integrity_failures +
        rule_failures
    )

    # Total checks run
    total_checks = (
    len(null_failures) +
    len(duplicate_failures) +
    len(integrity_failures) +
    len(rule_failures)
)

    # Calculate score
    score = calculate_quality_score(all_failures, total_checks)

    logger.info("=" * 50)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total checks run : {total_checks}")
    logger.info(f"Checks passed    : {total_checks - len(all_failures)}")
    logger.info(f"Checks failed    : {len(all_failures)}")
    logger.info(f"DATA QUALITY SCORE : {score}%")

    if all_failures:
        logger.warning("FAILED CHECKS:")
        for f in all_failures:
            logger.warning(f"  -> {f}")
    else:
        logger.info("ALL CHECKS PASSED — DATA IS CLEAN")

    logger.info("=" * 50)
    conn.close()
    return score


if __name__ == "__main__":
    run_all()