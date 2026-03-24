"""
app.py
------
Olist 360 — Enterprise BI Dashboard
Built with Streamlit
6 tabs covering executive overview, customer intelligence,
seller performance, product intelligence, logistics, and live ML predictions
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import joblib
import json
from pathlib import Path

# ── Page config ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Olist 360 — Intelligence Platform",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Paths ────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parents[2]
WAREHOUSE  = BASE_DIR / "data/warehouse/olist.duckdb"
MODELS_DIR = BASE_DIR / "src/ml/saved_models"

# ── Database connection ──────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return duckdb.connect(str(WAREHOUSE), read_only=True)

@st.cache_data
def run_query(query):
    conn = get_connection()
    return conn.execute(query).df()

# ── Load ML models ───────────────────────────────────────────────────────
@st.cache_resource
def load_models():
    delay_model = joblib.load(MODELS_DIR / "delay_predictor.pkl")
    value_model = joblib.load(MODELS_DIR / "order_value_predictor.pkl")
    return delay_model, value_model

# ── Sidebar ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Olist 360")
    st.markdown("Enterprise E-Commerce Intelligence Platform")
    st.markdown("---")
    st.markdown("**Dataset**")
    st.markdown("100K+ real orders")
    st.markdown("Brazil, 2016-2018")
    st.markdown("---")
    st.markdown("**Modules**")
    st.markdown("- Data Engineering")
    st.markdown("- EDA & Analytics")
    st.markdown("- Machine Learning")
    st.markdown("- Business Intelligence")

# ── Tabs ─────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Executive Overview",
    "Customer Intelligence",
    "Seller Performance",
    "Product Intelligence",
    "Logistics & Operations",
    "Live ML Predictions"
])

# ════════════════════════════════════════════════════════════════════════
# TAB 1 — EXECUTIVE OVERVIEW
# ════════════════════════════════════════════════════════════════════════
with tab1:
    st.header("Executive Overview")

    # KPI Cards
    total_orders = run_query("SELECT COUNT(*) FROM fact_orders").iloc[0,0]
    total_revenue = run_query("SELECT ROUND(SUM(total_price),2) FROM fact_orders").iloc[0,0]
    avg_order = run_query("SELECT ROUND(AVG(total_price),2) FROM fact_orders").iloc[0,0]
    late_rate = run_query("SELECT ROUND(AVG(is_late)*100,2) FROM fact_orders WHERE is_late IS NOT NULL").iloc[0,0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Orders", f"{total_orders:,}")
    col2.metric("Total Revenue", f"R${total_revenue:,.0f}")
    col3.metric("Avg Order Value", f"R${avg_order}")
    col4.metric("Late Delivery Rate", f"{late_rate}%")

    st.markdown("---")

    # Monthly trend
    df_trend = run_query("""
        SELECT
            DATE_TRUNC('month', order_purchase_timestamp) AS month,
            COUNT(*) AS total_orders,
            ROUND(SUM(total_price), 2) AS total_revenue
        FROM fact_orders
        WHERE order_purchase_timestamp IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Monthly Order Volume")
        fig = px.area(df_trend, x="month", y="total_orders",
                      color_discrete_sequence=["#4C72B0"])
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Monthly Revenue (BRL)")
        fig = px.area(df_trend, x="month", y="total_revenue",
                      color_discrete_sequence=["#55A868"])
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    # Order status
    st.subheader("Order Status Distribution")
    df_status = run_query("""
        SELECT order_status, COUNT(*) AS total
        FROM fact_orders GROUP BY 1 ORDER BY total DESC
    """)
    fig = px.bar(df_status, x="order_status", y="total",
                 color="order_status", height=350)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════
# TAB 2 — CUSTOMER INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════
with tab2:
    st.header("Customer Intelligence")

    # RFM Segments
    df_rfm = run_query("""
        WITH rfm AS (
            SELECT
                customer_id,
                DATEDIFF('day', MAX(order_purchase_timestamp), '2018-09-03') AS recency,
                COUNT(*) AS frequency,
                SUM(total_price) AS monetary
            FROM fact_orders
            WHERE order_purchase_timestamp IS NOT NULL
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN recency <= 90 AND frequency >= 2 THEN 'Champions'
                WHEN recency <= 180 THEN 'Loyal Customers'
                WHEN recency <= 270 THEN 'At Risk'
                ELSE 'Lost'
            END AS segment,
            COUNT(*) AS customers,
            ROUND(AVG(monetary), 2) AS avg_spend
        FROM rfm
        GROUP BY 1
        ORDER BY customers DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Customer Segments (RFM)")
        fig = px.pie(df_rfm, values="customers", names="segment",
                     color_discrete_sequence=["#55A868","#4C72B0","#DD8452","#C44E52"])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Average Spend by Segment")
        fig = px.bar(df_rfm, x="segment", y="avg_spend",
                     color="segment", height=350,
                     color_discrete_sequence=["#55A868","#4C72B0","#DD8452","#C44E52"])
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Review score distribution
    st.subheader("Review Score Distribution")
    df_reviews = run_query("""
        SELECT ROUND(review_score) AS score, COUNT(*) AS total
        FROM fact_orders WHERE review_score IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)
    fig = px.bar(df_reviews, x="score", y="total",
                 color_discrete_sequence=["#4C72B0"], height=300)
    st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════
# TAB 3 — SELLER PERFORMANCE
# ════════════════════════════════════════════════════════════════════════
with tab3:
    st.header("Seller Performance")

    df_sellers = run_query("""
        SELECT
            s.seller_state,
            COUNT(DISTINCT f.seller_id) AS total_sellers,
            COUNT(*) AS total_orders,
            ROUND(SUM(f.total_price), 2) AS total_revenue,
            ROUND(AVG(f.is_late) * 100, 2) AS late_rate,
            ROUND(AVG(f.review_score), 2) AS avg_review
        FROM fact_orders f
        JOIN dim_seller s ON f.seller_id = s.seller_id
        WHERE s.seller_state IS NOT NULL
        GROUP BY 1
        ORDER BY total_revenue DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Seller State")
        fig = px.bar(df_sellers.head(10), x="seller_state", y="total_revenue",
                     color_discrete_sequence=["#4C72B0"], height=350)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Late Delivery Rate by Seller State")
        fig = px.bar(df_sellers.head(10), x="seller_state", y="late_rate",
                     color_discrete_sequence=["#C44E52"], height=350)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Seller State Performance Table")
    st.dataframe(df_sellers, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════
# TAB 4 — PRODUCT INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════
with tab4:
    st.header("Product Intelligence")

    df_products = run_query("""
        SELECT
            p.product_category_name_english AS category,
            COUNT(*) AS total_orders,
            ROUND(SUM(f.total_price), 2) AS total_revenue,
            ROUND(AVG(f.total_price), 2) AS avg_price,
            ROUND(AVG(f.review_score), 2) AS avg_review
        FROM fact_orders f
        JOIN dim_product p ON f.product_id = p.product_id
        WHERE p.product_category_name_english IS NOT NULL
        GROUP BY 1
        ORDER BY total_revenue DESC
        LIMIT 15
    """)

    st.subheader("Top 15 Categories by Revenue")
    fig = px.bar(df_products, x="total_revenue", y="category",
                 orientation="h",
                 color_discrete_sequence=["#4C72B0"], height=500)
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Avg Price by Category")
        fig = px.bar(df_products.head(10), x="category", y="avg_price",
                     color_discrete_sequence=["#55A868"], height=350)
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Review by Category")
        fig = px.bar(df_products.head(10), x="category", y="avg_review",
                     color_discrete_sequence=["#DD8452"], height=350)
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════
# TAB 5 — LOGISTICS & OPERATIONS
# ════════════════════════════════════════════════════════════════════════
with tab5:
    st.header("Logistics & Operations")

    df_logistics = run_query("""
        SELECT
            customer_state,
            COUNT(*) AS total_orders,
            ROUND(AVG(is_late) * 100, 2) AS late_rate,
            ROUND(AVG(total_freight), 2) AS avg_freight,
            ROUND(AVG(
                DATEDIFF('day',
                    order_purchase_timestamp,
                    order_delivered_customer_date)
            ), 1) AS avg_delivery_days
        FROM fact_orders
        WHERE customer_state IS NOT NULL
        AND order_delivered_customer_date IS NOT NULL
        GROUP BY 1
        ORDER BY late_rate DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Late Delivery Rate by State")
        fig = px.bar(df_logistics.head(10),
                     x="customer_state", y="late_rate",
                     color_discrete_sequence=["#C44E52"], height=350)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Delivery Days by State")
        fig = px.bar(df_logistics.sort_values("avg_delivery_days", ascending=False).head(10),
                     x="customer_state", y="avg_delivery_days",
                     color_discrete_sequence=["#DD8452"], height=350)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Freight Cost Analysis by State")
    fig = px.scatter(df_logistics,
                     x="avg_freight", y="late_rate",
                     size="total_orders", text="customer_state",
                     color_discrete_sequence=["#4C72B0"], height=400)
    fig.update_traces(textposition="top center")
    st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════
# TAB 6 — LIVE ML PREDICTIONS
# ════════════════════════════════════════════════════════════════════════
with tab6:
    st.header("Live ML Predictions")
    st.markdown("Enter order details below to get real-time predictions.")

    delay_model, value_model = load_models()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Order Details")
        total_freight     = st.number_input("Freight Value (BRL)", 0.0, 500.0, 20.0)
        item_count        = st.number_input("Number of Items", 1, 20, 1)
        product_weight_g  = st.number_input("Product Weight (g)", 0, 30000, 500)
        payment_install   = st.number_input("Payment Installments", 1, 24, 1)
        order_month       = st.slider("Order Month", 1, 12, 6)

    with col2:
        st.subheader("Location & Product")
        customer_state = st.selectbox("Customer State", [
            0,1,2,3,4,5,6,7,8,9,10,11,12,13,
            14,15,16,17,18,19,20,21,22,23,24,25,26
        ])
        seller_state = st.selectbox("Seller State", [
            0,1,2,3,4,5,6,7,8,9,10,11,12,13,
            14,15,16,17,18,19,20,21,22,23,24,25,26
        ])
        category        = st.number_input("Category (encoded)", 0, 70, 10)
        payment_type    = st.number_input("Payment Type (encoded)", 0, 4, 0)
        order_hour      = st.slider("Order Hour", 0, 23, 12)
        order_dayofweek = st.slider("Day of Week", 0, 6, 3)

    if st.button("Get Predictions", type="primary"):

        # Delay prediction features
        product_volume = product_weight_g * 10 * 10 * 10
        freight_ratio  = total_freight / (100 + 1)
        is_same_state  = int(customer_state == seller_state)

        delay_input = pd.DataFrame([{
            "total_price":          100,
            "total_freight":        total_freight,
            "item_count":           item_count,
            "product_weight_g":     product_weight_g,
            "product_volume_cm3":   product_volume,
            "freight_ratio":        freight_ratio,
            "is_same_state":        is_same_state,
            "customer_state":       customer_state,
            "seller_state":         seller_state,
            "category":             category,
            "payment_type":         payment_type,
            "payment_installments": payment_install,
            "order_month":          order_month,
            "order_hour":           order_hour,
            "order_dayofweek":      order_dayofweek
        }])

        # Value prediction features
        value_input = pd.DataFrame([{
            "total_freight":        total_freight,
            "item_count":           item_count,
            "product_weight_g":     product_weight_g,
            "product_volume_cm3":   product_volume,
            "freight_ratio":        freight_ratio,
            "is_same_state":        is_same_state,
            "customer_state":       customer_state,
            "seller_state":         seller_state,
            "category":             category,
            "payment_type":         payment_type,
            "payment_installments": payment_install,
            "order_month":          order_month,
            "order_dayofweek":      order_dayofweek
        }])

        delay_pred = delay_model.predict(delay_input)[0]
        delay_prob = delay_model.predict_proba(delay_input)[0][1]
        value_pred = np.expm1(value_model.predict(value_input)[0])

        st.markdown("---")
        st.subheader("Prediction Results")

        res_col1, res_col2 = st.columns(2)

        with res_col1:
            if delay_pred == 1:
                st.error(f"Delivery Delay Risk: HIGH ({delay_prob*100:.1f}% probability)")
            else:
                st.success(f"Delivery Delay Risk: LOW ({delay_prob*100:.1f}% probability)")

        with res_col2:
            st.info(f"Predicted Order Value: R${value_pred:.2f}")