# Olist 360 — Enterprise E-Commerce Intelligence Platform

> End-to-End Data Engineering, Analytics, Machine Learning & Business Intelligence on 100K+ Real-World Orders

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5.0-yellow)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35-red)
![XGBoost](https://img.shields.io/badge/XGBoost-2.0-green)
![pytest](https://img.shields.io/badge/pytest-31%20tests-brightgreen)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-black)

---

## Project Overview

Olist 360 is a flagship, industrial-level data project built on the
Brazilian E-Commerce Public Dataset by Olist — 100,000+ real orders
across customers, sellers, products, payments, reviews, and logistics.

This project demonstrates end-to-end capability across:
- Data Engineering & Warehouse Design
- Exploratory Data Analysis & Business Intelligence
- Machine Learning & Explainability
- Dashboard Development
- Data Operations & Automation

---

## Architecture
```
Raw CSVs (Kaggle)
      ↓
ETL Pipeline (ingest.py)
      ↓
Star Schema Warehouse (DuckDB)
      ↓
┌─────────────────────────────────┐
│  EDA  │  ML Models  │  Dashboard│
└─────────────────────────────────┘
      ↓
Business Intelligence & Reporting
```

---

## Key Findings

- 99,441 orders analyzed across 27 Brazilian states
- R$13.6 million total revenue generated
- 7.87% late delivery rate — target for ML model
- São Paulo accounts for 42% of all orders
- Health & Beauty is the top revenue category
- 97% of customers are one-time buyers

---

## Machine Learning Models

| Model | Algorithm | Key Metric |
|---|---|---|
| Delivery Delay Predictor | XGBoost | Recall: 55.14%, ROC-AUC: 73.87% |
| Customer Satisfaction | XGBoost + Optuna | Recall: 93.38% |
| Order Value Predictor | XGBoost Regressor | R²: 0.98, MAE: R$4.64 |

---

## Project Structure
```
olist-360/
├── data/
│   ├── raw/              # Original Kaggle CSVs
│   ├── processed/        # Cleaned tables
│   └── warehouse/        # DuckDB star schema
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_ml_delay_predictor.ipynb
│   ├── 03_ml_churn_predictor.ipynb
│   └── 04_ml_order_value.ipynb
├── src/
│   ├── etl/              # Ingestion, transform, validation
│   ├── ml/               # Saved models
│   ├── dashboard/        # Streamlit app
│   └── ops/              # Monitoring, automation
├── tests/                # 31 pytest unit tests
├── reports/figures/      # All charts and maps
└── .github/workflows/    # CI pipeline
```

---

## Tech Stack

| Area | Technology |
|---|---|
| Language | Python 3.10+ |
| Data Processing | pandas, numpy |
| Database | DuckDB, SQLAlchemy |
| Machine Learning | scikit-learn, XGBoost, SHAP, Optuna |
| Visualization | Plotly, Seaborn, Matplotlib, Folium |
| Dashboard | Streamlit |
| Testing | pytest, pytest-cov |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |

---

## Setup Instructions
```bash
# Clone the repository
git clone https://github.com/Resin-babu/olist-360.git
cd olist-360

# Create virtual environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download dataset from Kaggle and place CSVs in data/raw/

# Run ETL pipeline
python src/etl/ingest.py
python src/etl/transform.py
python src/etl/validate.py

# Run tests
pytest tests/test_etl.py -v

# Launch dashboard
streamlit run src/dashboard/app.py
```

---

## Data Quality

- 13 automated validation checks
- 100% data quality score
- Referential integrity verified across all 7 warehouse tables
- Business rule validation on prices, dates, and review scores

---

## Resume Bullets by Role

### Data Analyst
- Performed end-to-end EDA on 100K+ e-commerce orders identifying
  R$13.6M revenue trends, 7.87% delivery delay rate, and customer
  segmentation using RFM analysis
- Built cohort retention analysis and customer lifetime value models
  revealing 97% single-purchase customer rate across 27 Brazilian states
- Designed interactive Streamlit dashboard with 6 analytical tabs
  enabling real-time business intelligence for executive stakeholders

### Data Scientist
- Engineered and deployed 3 production-ready ML models using XGBoost,
  scikit-learn Pipelines, and SHAP explainability on 100K+ records
- Achieved R²=0.98 on order value regression and 55% recall on delivery
  delay classification using advanced feature engineering
- Applied Optuna hyperparameter tuning across 50 trials improving
  model ROC-AUC by 15% over baseline Logistic Regression

### Data Engineer
- Designed and implemented end-to-end ETL pipeline ingesting 9 CSV
  tables into a DuckDB star schema warehouse with fact and dimension tables
- Built automated data validation layer with 13 quality checks achieving
  100% data quality score across 99,441 records
- Configured GitHub Actions CI/CD pipeline running 31 pytest unit tests
  on every push ensuring pipeline reliability

### Business Analyst
- Translated 100K+ e-commerce transactions into actionable insights
  including top revenue categories, state-level performance, and
  delivery SLA compliance reports
- Developed executive summary with 5 data-backed recommendations
  quantifying business impact of delivery delays and customer churn
- Built RFM customer segmentation identifying Champions, Loyal,
  At-Risk, and Lost customer cohorts for targeted marketing strategy

### BI Developer
- Built enterprise-grade Streamlit dashboard with 6 interactive tabs
  covering executive KPIs, customer intelligence, and logistics analysis
- Integrated live ML predictions into dashboard enabling real-time
  delivery delay risk scoring and order value estimation
- Created geospatial Brazil sales map using Folium with interactive
  state-level revenue and delay rate popups

---

## ATS Keywords by Role

**Data Analyst:** EDA, SQL, pandas, data visualization, cohort analysis,
RFM analysis, KPI reporting, Plotly, Seaborn, business intelligence,
trend analysis, customer segmentation

**Data Scientist:** machine learning, XGBoost, scikit-learn, feature
engineering, SHAP, model evaluation, ROC-AUC, hyperparameter tuning,
Optuna, classification, regression, Python

**Data Engineer:** ETL pipeline, data warehouse, star schema, DuckDB,
SQLAlchemy, data validation, data quality, automated pipeline,
pytest, GitHub Actions, CI/CD

**Business Analyst:** business intelligence, stakeholder reporting,
executive summary, data-driven recommendations, customer lifetime value,
revenue analysis, operational efficiency

**BI Developer:** Streamlit, Plotly, interactive dashboard, KPI cards,
data visualization, live predictions, geospatial analysis, Folium