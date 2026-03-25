# Olist 360 — Executive Summary

**Prepared for:** C-Suite Leadership
**Dataset:** Brazilian E-Commerce Public Dataset — 99,441 orders
**Period:** 2016-2018

---

## Business Context

Olist is a Brazilian e-commerce marketplace connecting small businesses
to major retail channels. This analysis covers 100,000+ real transactions
to identify growth opportunities, operational inefficiencies, and
customer behavior patterns.

---

## Key Metrics

| Metric | Value |
|---|---|
| Total Orders Analyzed | 99,441 |
| Total Revenue | R$13,591,643 |
| Average Order Value | R$137.75 |
| Late Delivery Rate | 7.87% |
| Top State by Revenue | São Paulo (SP) |
| Top Category by Revenue | Health & Beauty |
| Data Quality Score | 100% |

---

## Top 5 Business Recommendations

**1. Fix Logistics in High-Delay States**
States outside SP and RJ show significantly higher late delivery rates.
Recommend regional logistics partnerships in northern states to reduce
the 7.87% national late rate — a 50% reduction would impact ~7,800
orders annually.

**2. Invest in Health & Beauty Category**
Health & Beauty generates the highest revenue across all categories.
Recommend increasing seller recruitment and promotional spend in this
category to capitalize on existing demand.

**3. Address Single-Purchase Customer Rate**
97% of customers never return for a second purchase. Recommend
implementing a post-purchase email sequence with personalized
recommendations within 30 days of first order.

**4. Credit Card Installment Strategy**
78% of orders are paid by credit card with an average of 3 installments.
Recommend partnering with financial institutions to offer 0% installment
plans on orders above R$200 to increase average order value.

**5. Seller Performance Management**
Sellers in certain states show consistently higher delay rates and lower
review scores. Recommend implementing a seller scorecard system with
automated SLA alerts to proactively manage delivery performance.

---

## Machine Learning Impact

Three predictive models were deployed:

- **Delivery Delay Predictor** — identifies 55% of late deliveries before
  they happen, enabling proactive customer communication
- **Customer Satisfaction Predictor** — 93% recall on satisfied customers
  enabling targeted retention campaigns
- **Order Value Predictor** — R²=0.98 accuracy enabling dynamic pricing
  and inventory planning

---

*Analysis conducted using Python, DuckDB, XGBoost, and Streamlit.
Full methodology and code available at github.com/Resin-babu/olist-360*