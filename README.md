# Promo ROI Engine — Synthetic Dataset

Part of a portfolio project demonstrating **price elasticity analysis + marketing attribution** using Python, BigQuery SQL, and Looker Studio.

---

## Tables

### `campaigns.csv` — 159 rows
Paid campaign metadata. Covers Jan 2023 – Mar 2024.

| Column | Type | Notes |
|---|---|---|
| `campaign_id` | string | Primary key (`CMP0001`…) |
| `channel` | string | `paid_search`, `email`, `paid_social` |
| `campaign_name` | string | Synthetic name |
| `start_date` / `end_date` | date | Campaign window |
| `discount_pct` | float | Discount offered (0–0.25) |
| `budget_usd` | float | Total spend budget |
| `cpc_usd` | float | Cost per click |

---

### `sessions.csv` — 80,000 rows
One row per website visit. Channels: organic search, paid search, email, social organic, paid social, direct, referral.

| Column | Type | Notes |
|---|---|---|
| `session_id` | string | Primary key |
| `timestamp` | datetime | Visit time |
| `channel` | string | Traffic source |
| `campaign_id` | string | FK → campaigns (null for organic/direct) |
| `discount_pct` | float | Active discount at time of visit |
| `converted` | bool | Whether visit resulted in a purchase |
| `session_duration_s` | int | Seconds on site |
| `pages_viewed` | int | Pages per session |
| `device` | string | desktop / mobile / tablet |
| `country` | string | ISO 2-letter code |

---

### `orders.csv` — ~3,846 rows
One row per order. Joined 1:1 with converted sessions.

| Column | Type | Notes |
|---|---|---|
| `order_id` | string | Primary key |
| `session_id` | string | FK → sessions |
| `channel` | string | Attribution channel |
| `campaign_id` | string | FK → campaigns (nullable) |
| `order_date` | date | |
| `product` | string | One of 5 products |
| `category` | string | software / services / education |
| `base_price_usd` | float | List price before discount |
| `discount_pct` | float | Applied discount |
| `final_price_usd` | float | Price paid |
| `quantity` | int | Units ordered |
| `revenue_usd` | float | `final_price * quantity` |
| `gross_margin_usd` | float | Revenue minus simulated COGS |

---

## Baked-in signals

These patterns are intentionally embedded so the analysis can surface them:

| Channel | Base CVR | Price elasticity |
|---|---|---|
| email | 7.1% | **–2.1** (highly elastic) |
| paid_search | 5.8% | –1.4 |
| paid_social | 4.4% | –1.6 |
| direct | 3.9% | –1.0 |
| organic_search | 3.2% | –0.9 |
| referral | 2.7% | –0.8 |
| social_organic | 2.1% | –0.7 |

- **Email is the most elastic channel** — a 10% discount increases email-attributed orders ~21%
- ~56% of orders have no discount, creating a natural control group
- Country mix skews African markets (KE, NG, ZA, GH) — relevant for regional pricing analysis

---

## Next steps in the project

1. **BigQuery** — load the three CSVs as tables; write transformation SQL (ROAS by channel, cohort CVR, discount uplift)
2. **Python — Attribution** — implement last-touch and data-driven (Shapley) attribution with `scikit-learn`
3. **Python — Elasticity** — run log-log OLS regression (`statsmodels`) per channel
4. **Looker Studio** — connect to BigQuery; build channel elasticity + ROAS dashboard

---

## Regenerating

```bash
pip install pandas numpy faker
python generate_dataset.py
```

Seed is fixed (`np.random.seed(42)`) so output is fully reproducible.
