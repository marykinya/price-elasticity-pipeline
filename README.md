# Promo ROI Engine

A end-to-end data pipeline that models the relationship between promotional discounts and revenue performance across marketing channels.

Built with Python, BigQuery, and Looker Studio, with a daily automated data pipeline running on GitHub Actions.


## What it does

- Generates realistic e-commerce session, order, and campaign data with baked-in price elasticity signals
- Loads and maintains three BigQuery tables, appending ~215 new sessions daily via GitHub Actions
- Runs SQL transformations to produce four analytical views: channel performance, discount elasticity, campaign ROAS, and product margin analysis
- Visualised in a Looker Studio dashboard connected live to BigQuery


## Stack

| Layer | Tool |
|---|---|
| Data generation | Python (`pandas`, `faker`, `numpy`) |
| Data warehouse | Google BigQuery |
| Transformations | BigQuery SQL |
| Orchestration | GitHub Actions (daily cron) |
| Visualisation | Looker Studio |


## Project structure

```
├── 01_generate_dataset.py    # Generates full historical dataset (Jan 2025 → today)
├── 02_load_to_bq.py          # Loads CSV data into BigQuery
├── 03_generate_daily.py      # Appends one day of data to BigQuery (runs daily via CI)
├── 01_schema.sql             # BigQuery table definitions (local only)
├── 02_transforms.sql         # Analytical views built on top of raw tables (local only)
└── .github/workflows/
    └── daily_pipeline.yml    # GitHub Actions daily schedule
```


## BigQuery schema

### `campaigns`
Paid campaign metadata — one row per campaign across paid search, email, and paid social channels.

| Column | Type | Description |
|---|---|---|
| `campaign_id` | STRING | Primary key (`CMP0001`…) |
| `channel` | STRING | `paid_search`, `email`, `paid_social` |
| `campaign_name` | STRING | Campaign label |
| `start_date` / `end_date` | DATE | Campaign window |
| `discount_pct` | FLOAT | Discount offered (0–0.25) |
| `budget_usd` | FLOAT | Total spend budget |
| `cpc_usd` | FLOAT | Cost per click |

### `sessions`
One row per website visit across all traffic sources.

| Column | Type | Description |
|---|---|---|
| `session_id` | STRING | Primary key |
| `timestamp` | TIMESTAMP | Visit time |
| `channel` | STRING | Traffic source |
| `campaign_id` | STRING | FK → campaigns (null for organic/direct) |
| `discount_pct` | FLOAT | Active discount at time of visit |
| `converted` | BOOL | Whether the visit resulted in a purchase |
| `session_duration_s` | INT | Seconds on site |
| `pages_viewed` | INT | Pages per session |
| `device` | STRING | `desktop`, `mobile`, `tablet` |
| `country` | STRING | ISO 2-letter country code |

### `orders`
One row per order, joined 1:1 with converted sessions.

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Primary key |
| `session_id` | STRING | FK → sessions |
| `channel` | STRING | Attribution channel |
| `campaign_id` | STRING | FK → campaigns (nullable) |
| `order_date` | DATE | Date of purchase |
| `product` | STRING | One of 5 products |
| `category` | STRING | `software`, `services`, `education` |
| `base_price_usd` | FLOAT | List price before discount |
| `discount_pct` | FLOAT | Applied discount |
| `final_price_usd` | FLOAT | Price paid |
| `quantity` | INT | Units ordered |
| `revenue_usd` | FLOAT | `final_price × quantity` |
| `gross_margin_usd` | FLOAT | Revenue minus simulated COGS |


## BigQuery views that feed into Looker Studio

| View | Description |
|---|---|
| `channel_daily` | Daily sessions, CVR, revenue, spend, and ROAS per channel |
| `discount_elasticity` | Conversion lift and implied price elasticity by channel × discount tier |
| `campaign_roas` | Return-on-Ad-Spend, Return-on-Investment, and cost-per-order per campaign |
| `product_discount_summary` | Revenue and margin by product × discount tier |


## Running locally

**1. Install dependencies**
```bash
pip install google-cloud-bigquery pandas numpy faker db-dtypes
```

**2. Authenticate**
```bash
gcloud auth application-default login
```

**3. Generate historical data and load to BigQuery**
```bash
python 01_generate_dataset.py
python 02_load_to_bq.py --project YOUR_PROJECT --data-dir ./data
```

**4. Run SQL transforms**

Open the BigQuery console, create the views. 


## Daily automation

GitHub Actions runs `01_generate_daily.py` every day at 7am UTC, appending ~215 new sessions and their resulting orders to BigQuery. Looker Studio reflects the updates on the next report load.

## Dashboard

[View live dashboard →](https://lookerstudio.google.com/reporting/89178669-a528-408d-8c12-ecd8c86240eb)
