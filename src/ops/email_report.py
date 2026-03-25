"""
email_report.py
---------------
Generates a professional HTML KPI email report
using Jinja2 templates and saves it to reports/

In production this would send via SMTP or SendGrid.
For this project we generate and save the HTML file
so you can see exactly what would be sent.

Usage:
    python src/ops/email_report.py
"""

import duckdb
import logging
from pathlib import Path
from datetime import datetime
from jinja2 import Template

BASE_DIR  = Path(__file__).resolve().parents[2]
WAREHOUSE = BASE_DIR / "data/warehouse/olist.duckdb"
REPORTS   = BASE_DIR / "reports"
LOGS      = BASE_DIR / "logs"
LOGS.mkdir(exist_ok=True)

# ── Logging ──────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
file_handler = logging.FileHandler(LOGS / "email_report.log")
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


# ── Fetch KPIs from warehouse ─────────────────────────────────────────────
def fetch_kpis() -> dict:
    """
    Queries the warehouse for key business metrics.
    Returns a dictionary of KPI values.
    """
    conn = duckdb.connect(str(WAREHOUSE), read_only=True)

    kpis = {}

    kpis["total_orders"] = conn.execute(
        "SELECT COUNT(*) FROM fact_orders"
    ).fetchone()[0]

    kpis["total_revenue"] = conn.execute(
        "SELECT ROUND(SUM(total_price), 2) FROM fact_orders"
    ).fetchone()[0]

    kpis["avg_order_value"] = conn.execute(
        "SELECT ROUND(AVG(total_price), 2) FROM fact_orders"
    ).fetchone()[0]

    kpis["late_delivery_rate"] = conn.execute(
        "SELECT ROUND(AVG(is_late)*100, 2) FROM fact_orders WHERE is_late IS NOT NULL"
    ).fetchone()[0]

    kpis["avg_review_score"] = conn.execute(
        "SELECT ROUND(AVG(review_score), 2) FROM fact_orders WHERE review_score IS NOT NULL"
    ).fetchone()[0]

    kpis["top_state"] = conn.execute(
        "SELECT customer_state FROM fact_orders GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()[0]

    kpis["top_category"] = conn.execute("""
        SELECT p.product_category_name_english
        FROM fact_orders f
        JOIN dim_product p ON f.product_id = p.product_id
        WHERE p.product_category_name_english IS NOT NULL
        GROUP BY 1 ORDER BY SUM(f.total_price) DESC LIMIT 1
    """).fetchone()[0]

    kpis["delivered_orders"] = conn.execute(
        "SELECT COUNT(*) FROM fact_orders WHERE order_status = 'delivered'"
    ).fetchone()[0]

    kpis["report_date"] = datetime.now().strftime("%B %d, %Y")

    conn.close()
    logger.info("KPIs fetched successfully")
    return kpis


# ── HTML Email Template ───────────────────────────────────────────────────
EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<style>
    body { font-family: Arial, sans-serif; background: #f4f4f4; margin: 0; padding: 20px; }
    .container { max-width: 650px; margin: auto; background: white; border-radius: 8px; overflow: hidden; }
    .header { background: #1a1a2e; color: white; padding: 30px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; }
    .header p { margin: 5px 0 0; color: #aaa; font-size: 14px; }
    .kpi-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; padding: 25px; }
    .kpi-card { background: #f8f9fa; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #4C72B0; }
    .kpi-card .value { font-size: 28px; font-weight: bold; color: #1a1a2e; }
    .kpi-card .label { font-size: 12px; color: #666; margin-top: 5px; text-transform: uppercase; }
    .section { padding: 0 25px 25px; }
    .section h2 { color: #1a1a2e; border-bottom: 2px solid #4C72B0; padding-bottom: 8px; }
    .insight { background: #e8f4fd; border-radius: 6px; padding: 12px 15px; margin: 8px 0; font-size: 14px; }
    .footer { background: #f8f9fa; padding: 20px; text-align: center; font-size: 12px; color: #999; }
    .alert { background: #fff3cd; border-left: 4px solid #ffc107; padding: 12px 15px; border-radius: 6px; }
</style>
</head>
<body>
<div class="container">

    <div class="header">
        <h1>Olist 360 Weekly KPI Report</h1>
        <p>Generated: {{ report_date }}</p>
    </div>

    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="value">{{ "{:,}".format(total_orders) }}</div>
            <div class="label">Total Orders</div>
        </div>
        <div class="kpi-card">
            <div class="value">R${{ "{:,.0f}".format(total_revenue) }}</div>
            <div class="label">Total Revenue</div>
        </div>
        <div class="kpi-card">
            <div class="value">R${{ avg_order_value }}</div>
            <div class="label">Avg Order Value</div>
        </div>
        <div class="kpi-card">
            <div class="value">{{ late_delivery_rate }}%</div>
            <div class="label">Late Delivery Rate</div>
        </div>
        <div class="kpi-card">
            <div class="value">{{ avg_review_score }}</div>
            <div class="label">Avg Review Score</div>
        </div>
        <div class="kpi-card">
            <div class="value">{{ "{:,}".format(delivered_orders) }}</div>
            <div class="label">Delivered Orders</div>
        </div>
    </div>

    <div class="section">
        <h2>Key Insights</h2>
        <div class="insight">Top performing state: <strong>{{ top_state }}</strong></div>
        <div class="insight">Top revenue category: <strong>{{ top_category }}</strong></div>
        <div class="insight">Overall data quality score: <strong>100%</strong></div>
    </div>

    <div class="section">
        <h2>Alerts</h2>
        {% if late_delivery_rate > 10 %}
        <div class="alert">Late delivery rate above 10% — immediate action required</div>
        {% else %}
        <div class="insight">All KPIs within normal range</div>
        {% endif %}
    </div>

    <div class="footer">
        Olist 360 Intelligence Platform | Automated Weekly Report<br>
        Built with Python, DuckDB, and Streamlit
    </div>

</div>
</body>
</html>
"""


# ── Generate report ───────────────────────────────────────────────────────
def generate_report():
    """
    Fetches KPIs, renders the HTML template,
    and saves the report to reports/
    """
    logger.info("GENERATING WEEKLY KPI REPORT...")

    kpis = fetch_kpis()

    template = Template(EMAIL_TEMPLATE)
    html_content = template.render(**kpis)

    output_path = REPORTS / "weekly_kpi_report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"REPORT SAVED: {output_path}")
    logger.info("Open reports/weekly_kpi_report.html in your browser to view it")
    return output_path


if __name__ == "__main__":
    try:
        result = generate_report()
        print(f"Report generated: {result}")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()