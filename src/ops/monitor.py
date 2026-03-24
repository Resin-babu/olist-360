"""
monitor.py — Industry-level Data Monitoring Script

Features:
- Volume monitoring (absolute + day-over-day)
- Data quality checks (nulls, late deliveries)
- Data quality scoring system
- Historical comparison
- Persistent monitoring logs (DB table)
- Structured logging
"""

import duckdb
import logging
from pathlib import Path
from datetime import datetime, timedelta

# ------------------ PATH SETUP ------------------
BASE_DIR  = Path(__file__).resolve().parents[2]
WAREHOUSE = BASE_DIR / "data/warehouse/olist.duckdb"
LOGS      = BASE_DIR / "logs"
LOGS.mkdir(exist_ok=True)

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ------------------ CONFIG ------------------
ORDER_THRESHOLD = 90000
LATE_RATE_THRESHOLD = 15
NULL_REVIEW_THRESHOLD = 20
DROP_THRESHOLD = 0.2   # 20% drop

# ------------------ DB SETUP ------------------
def create_monitoring_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring_log (
            run_time TIMESTAMP,
            total_orders INT,
            today_orders INT,
            yesterday_orders INT,
            late_rate FLOAT,
            null_review_pct FLOAT,
            data_quality_score INT
        )
    """)

# ------------------ CORE CHECKS ------------------

def get_total_orders(conn):
    return conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]


def get_daily_orders(conn, days_ago=0):
    return conn.execute(f"""
        SELECT COUNT(*)
        FROM fact_orders
        WHERE DATE(order_purchase_timestamp) = CURRENT_DATE - INTERVAL '{days_ago} day'
    """).fetchone()[0]


def get_late_rate(conn):
    return conn.execute("""
        SELECT ROUND(AVG(is_late) * 100, 2)
        FROM fact_orders
        WHERE is_late IS NOT NULL
    """).fetchone()[0]


def get_null_review_pct(conn, total):
    null_reviews = conn.execute("""
        SELECT COUNT(*)
        FROM fact_orders
        WHERE review_score IS NULL
    """).fetchone()[0]

    return null_reviews, round((null_reviews / total) * 100, 2)


# ------------------ SCORING ------------------

def compute_score(total, late_rate, null_pct, today, yesterday):
    score = 100

    if total < ORDER_THRESHOLD:
        score -= 20

    if late_rate > LATE_RATE_THRESHOLD:
        score -= 20

    if null_pct > NULL_REVIEW_THRESHOLD:
        score -= 20

    if yesterday > 0 and today < yesterday * (1 - DROP_THRESHOLD):
        score -= 20

    return max(score, 0)


# ------------------ MAIN MONITOR ------------------

def run_monitoring():
    logger.info(" STARTING DAILY MONITORING")

    conn = duckdb.connect(str(WAREHOUSE), read_only=False)
    create_monitoring_table(conn)

    alerts = []

    # ---- TOTAL ORDERS ----
    total = get_total_orders(conn)
    logger.info(f"Total orders: {total:,}")

    if total < ORDER_THRESHOLD:
        alerts.append(f"Order volume too low: {total}")

    # ---- DAILY COMPARISON ----
    today_orders = get_daily_orders(conn, 0)
    yesterday_orders = get_daily_orders(conn, 1)

    logger.info(f"Today's orders: {today_orders}")
    logger.info(f"Yesterday's orders: {yesterday_orders}")

    if yesterday_orders > 0:
        drop_pct = round((1 - today_orders / yesterday_orders) * 100, 2)
        logger.info(f"Day-over-day change: -{drop_pct}%")

        if today_orders < yesterday_orders * (1 - DROP_THRESHOLD):
            alerts.append(f"Order drop detected: {drop_pct}% decrease")

    # ---- LATE DELIVERY ----
    late_rate = get_late_rate(conn)
    logger.info(f"Late delivery rate: {late_rate}%")

    if late_rate > LATE_RATE_THRESHOLD:
        alerts.append(f"High late delivery rate: {late_rate}%")

    # ---- NULL REVIEWS ----
    null_reviews, null_pct = get_null_review_pct(conn, total)
    logger.info(f"Null reviews: {null_reviews} ({null_pct}%)")

    if null_pct > NULL_REVIEW_THRESHOLD:
        alerts.append(f"High null review percentage: {null_pct}%")

    # ---- DATA QUALITY SCORE ----
    score = compute_score(total, late_rate, null_pct, today_orders, yesterday_orders)
    logger.info(f"Data Quality Score: {score}/100")

    # ---- STORE RESULTS ----
    conn.execute("""
        INSERT INTO monitoring_log 
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(),
        total,
        today_orders,
        yesterday_orders,
        late_rate,
        null_pct,
        score
    ))

    # ---- ALERT OUTPUT ----
    if alerts:
        logger.warning(" ANOMALIES DETECTED:")
        for alert in alerts:
            logger.warning(f"  → {alert}")
    else:
        logger.info(" ALL CHECKS PASSED — System healthy")

    conn.close()
    logger.info(" MONITORING COMPLETE")


# ------------------ ENTRY POINT ------------------

if __name__ == "__main__":
    run_monitoring()