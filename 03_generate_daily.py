"""
Promo ROI Engine - Weekly Incremental Generator
Generates one week of sessions, orders, and occasional new campaigns,
then appends them to BigQuery.

Self-healing: each run checks the latest date already in BigQuery and replays
every missed daily run up to today, so a failed or skipped run is filled in
by the next successful one. If the data is already current, it does nothing.

Run automatically via cron, or manually:
    python generate_weekly.py --project promo-roi-engine
"""

import argparse
import io
import random
import sys
import warnings
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
from faker import Faker
from google.cloud import bigquery

from config import CHANNELS, PRODUCTS, DISCOUNT_TIERS, PAID_CHANNELS

warnings.filterwarnings("ignore")

fake = Faker()
np.random.seed()   # fresh seed each run so data varies week to week
random.seed()

DATASET  = "promo_roi"
PROJECT  = None   # set from args

SESSIONS_PER_RUN      = 215   # ~1,500/week
NEW_CAMPAIGNS_PER_RUN = 1     # campaigns to create when the daily chance fires
NEW_CAMPAIGN_CHANCE   = 0.4   # 40% chance each daily run adds a campaign
MAX_CATCHUP_DAYS      = 90    # refuse to auto-fill gaps longer than this
GAP_CHECK_DAYS        = 30    # fail the run if any of the last N days is empty


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_ts(start: date, end: date) -> datetime:
    delta = (end - start).days
    return datetime.combine(start, datetime.min.time()) + timedelta(
        days=random.randint(0, max(delta, 0)),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

def session_duration(channel, converted):
    base = {"organic_search": 180, "paid_search": 120, "email": 210,
            "social_organic": 90,  "paid_social": 100, "direct": 240, "referral": 150}
    mu = base.get(channel, 150) * (1.6 if converted else 1.0)
    return max(10, int(np.random.lognormal(np.log(mu), 0.5)))

def pages_viewed(channel, converted):
    base = {"organic_search": 4, "paid_search": 3, "email": 5,
            "social_organic": 2, "paid_social": 3, "direct": 6, "referral": 3}
    mu = base.get(channel, 3) * (1.4 if converted else 1.0)
    return max(1, int(np.random.poisson(mu)))


# ── Fetch existing data from BQ ───────────────────────────────────────────────

def get_existing_campaigns(client) -> pd.DataFrame:
    query = f"SELECT * FROM `{PROJECT}.{DATASET}.campaigns`"
    return client.query(query).to_dataframe()

def get_max_ids(client) -> tuple[int, int]:
    """Return the current max session number and order number."""
    sq = f"SELECT MAX(CAST(SUBSTR(session_id, 2) AS INT64)) FROM `{PROJECT}.{DATASET}.sessions`"
    oq = f"SELECT MAX(CAST(SUBSTR(order_id, 4) AS INT64)) FROM `{PROJECT}.{DATASET}.orders`"
    max_s = client.query(sq).to_dataframe().iloc[0, 0]
    max_o = client.query(oq).to_dataframe().iloc[0, 0]
    return int(max_s) if max_s is not None and str(max_s) != '<NA>' else 0, \
           int(max_o) if max_o is not None and str(max_o) != '<NA>' else 0

def get_max_campaign_id(client) -> int:
    q = f"SELECT MAX(CAST(SUBSTR(campaign_id, 4) AS INT64)) FROM `{PROJECT}.{DATASET}.campaigns`"
    val = client.query(q).to_dataframe().iloc[0, 0]
    return int(val) if val is not None and str(val) != '<NA>' else 0

def get_last_data_date(client) -> date:
    q = f"SELECT MAX(DATE(timestamp)) FROM `{PROJECT}.{DATASET}.sessions`"
    return client.query(q).to_dataframe().iloc[0, 0]

def get_empty_days(client, days: int) -> list[date]:
    """Return dates in the last `days` days (excluding today) with no sessions."""
    q = f"""
        SELECT dt FROM UNNEST(GENERATE_DATE_ARRAY(
            DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY),
            DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) dt
        WHERE dt NOT IN (SELECT DISTINCT DATE(timestamp) FROM `{PROJECT}.{DATASET}.sessions`)
        ORDER BY dt
    """
    return list(client.query(q).to_dataframe()["dt"])


# ── Generators ────────────────────────────────────────────────────────────────

def build_new_campaigns(start_campaign_id: int, week_start: date) -> pd.DataFrame:
    rows = []
    for i in range(NEW_CAMPAIGNS_PER_RUN):
        ch = random.choice(PAID_CHANNELS)
        discount = random.choice(DISCOUNT_TIERS)
        spend = round(random.uniform(800, 8000), 2)
        campaign_id = start_campaign_id + i + 1
        rows.append({
            "campaign_id":   f"CMP{campaign_id:04d}",
            "channel":       ch,
            "campaign_name": f"{ch.replace('_',' ').title()} - {fake.catch_phrase()[:40]}",
            "start_date":    week_start,
            "end_date":      week_start + timedelta(days=random.randint(7, 21)),
            "discount_pct":  discount,
            "budget_usd":    spend,
            "cpc_usd":       round(random.uniform(0.40, 4.50), 2),
        })
    return pd.DataFrame(rows)


def build_weekly_sessions(campaigns_df: pd.DataFrame, week_start: date,
                           week_end: date, start_session_id: int,
                           n_sessions: int = 215) -> pd.DataFrame:
    ch_names   = list(CHANNELS.keys())
    ch_weights = [CHANNELS[c]["weight"] for c in ch_names]
    sessions   = []

    for i in range(n_sessions):
        sid     = start_session_id + i + 1
        channel = random.choices(ch_names, weights=ch_weights)[0]
        ts      = rand_ts(week_start, week_end)

        campaign_id = None
        if channel in PAID_CHANNELS and not campaigns_df.empty:
            live = campaigns_df[
                (campaigns_df["channel"] == channel) &
                (pd.to_datetime(campaigns_df["start_date"]).dt.date <= ts.date()) &
                (pd.to_datetime(campaigns_df["end_date"]).dt.date   >= ts.date())
            ]
            if not live.empty:
                campaign_id = live.sample(1)["campaign_id"].values[0]

        discount = 0.0
        if campaign_id:
            discount = float(campaigns_df.loc[
                campaigns_df["campaign_id"] == campaign_id, "discount_pct"
            ].values[0])

        base_cvr   = CHANNELS[channel]["base_cvr"]
        elasticity = CHANNELS[channel]["elasticity"]
        lift       = 1 + elasticity * (-discount) if discount > 0 else 1.0
        cvr        = min(base_cvr * lift + np.random.normal(0, 0.005), 0.95)
        converted  = random.random() < max(cvr, 0.005)

        sessions.append({
            "session_id":         f"S{sid:07d}",
            "timestamp":          ts,
            "channel":            channel,
            "campaign_id":        campaign_id,
            "discount_pct":       discount,
            "converted":          converted,
            "session_duration_s": session_duration(channel, converted),
            "pages_viewed":       pages_viewed(channel, converted),
            "device":             random.choices(["desktop","mobile","tablet"], weights=[0.52,0.38,0.10])[0],
            "country":            random.choices(
                ["KE","NG","ZA","GH","US","GB","IN","CA","AU","DE"],
                weights=[0.20,0.15,0.12,0.08,0.12,0.08,0.07,0.06,0.06,0.06]
            )[0],
        })

    return pd.DataFrame(sessions)


def build_weekly_orders(sessions_df: pd.DataFrame, start_order_id: int) -> pd.DataFrame:
    converted = sessions_df[sessions_df["converted"]].copy()
    products  = list(PRODUCTS.keys())
    orders    = []

    for idx, (_, s) in enumerate(converted.iterrows()):
        product     = random.choice(products)
        base_price  = PRODUCTS[product]["base_price"]
        discount    = s["discount_pct"]
        final_price = round(base_price * (1 - discount), 2)
        qty         = random.choices([1, 2, 3], weights=[0.75, 0.18, 0.07])[0]
        revenue     = round(final_price * qty, 2)
        margin      = round(revenue * (1 - random.uniform(0.25, 0.45)), 2)

        orders.append({
            "order_id":          f"ORD{start_order_id + idx + 1:07d}",
            "session_id":        s["session_id"],
            "channel":           s["channel"],
            "campaign_id":       s["campaign_id"],
            "order_date":        s["timestamp"].date(),
            "product":           product,
            "category":          PRODUCTS[product]["category"],
            "base_price_usd":    base_price,
            "discount_pct":      discount,
            "final_price_usd":   final_price,
            "quantity":          qty,
            "revenue_usd":       revenue,
            "gross_margin_usd":  margin,
        })

    return pd.DataFrame(orders)


# ── BigQuery loader ───────────────────────────────────────────────────────────

def append_to_bq(client, df: pd.DataFrame, table: str, schema: list):
    table_ref  = f"{PROJECT}.{DATASET}.{table}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )
    buf = io.BytesIO(df.to_csv(index=False).encode())
    job = client.load_table_from_file(buf, table_ref, job_config=job_config)
    job.result()
    return job.output_rows


CAMPAIGN_SCHEMA = [
    bigquery.SchemaField("campaign_id",   "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("channel",        "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("campaign_name",  "STRING"),
    bigquery.SchemaField("start_date",     "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("end_date",       "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("discount_pct",   "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("budget_usd",     "FLOAT64"),
    bigquery.SchemaField("cpc_usd",        "FLOAT64"),
]

SESSION_SCHEMA = [
    bigquery.SchemaField("session_id",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("timestamp",          "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("channel",            "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("campaign_id",        "STRING"),
    bigquery.SchemaField("discount_pct",       "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("converted",          "BOOL",      mode="REQUIRED"),
    bigquery.SchemaField("session_duration_s", "INT64"),
    bigquery.SchemaField("pages_viewed",       "INT64"),
    bigquery.SchemaField("device",             "STRING"),
    bigquery.SchemaField("country",            "STRING"),
]

ORDER_SCHEMA = [
    bigquery.SchemaField("order_id",         "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("session_id",        "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("channel",           "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("campaign_id",       "STRING"),
    bigquery.SchemaField("order_date",        "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("product",           "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("category",          "STRING"),
    bigquery.SchemaField("base_price_usd",    "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("discount_pct",      "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("final_price_usd",   "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("quantity",          "INT64",   mode="REQUIRED"),
    bigquery.SchemaField("revenue_usd",       "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("gross_margin_usd",  "FLOAT64"),
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global PROJECT

    parser = argparse.ArgumentParser(description="Weekly incremental data generator")
    parser.add_argument("--project", default="promo-roi-engine", help="GCP project ID")
    args   = parser.parse_args()
    PROJECT = args.project

    client = bigquery.Client(project=PROJECT)
    today  = date.today()
    print(f"Project: {PROJECT} / Dataset: {DATASET}\n")

    # Work out which daily runs are missing. Each run covers (run_date - 1, run_date),
    # so the run after the latest data date is the first one that never happened.
    last_date = get_last_data_date(client)
    run_dates = [last_date + timedelta(days=i)
                 for i in range(1, (today - last_date).days + 1)]
    print(f"Latest data in BigQuery: {last_date}")

    if not run_dates:
        print("Data is already up to date - nothing to generate.")
    elif len(run_dates) > MAX_CATCHUP_DAYS:
        sys.exit(f"Gap of {len(run_dates)} days exceeds MAX_CATCHUP_DAYS={MAX_CATCHUP_DAYS}; "
                 f"backfill manually or raise the limit.")
    else:
        if len(run_dates) > 1:
            print(f"Catching up {len(run_dates)} missed runs: {run_dates[0]} → {run_dates[-1]}")

        # Fetch existing state
        print("Fetching existing data from BigQuery...")
        campaigns_df   = get_existing_campaigns(client)
        max_session_id, max_order_id = get_max_ids(client)
        max_campaign_id = get_max_campaign_id(client)
        print(f"  Existing: {len(campaigns_df)} campaigns, "
              f"session #{max_session_id:,}, order #{max_order_id:,}\n")

        # Generate every missed day in memory, then load once per table
        all_campaigns = campaigns_df.copy()
        new_campaigns, new_sessions, new_orders = [], [], []
        for run_date in run_dates:
            day_start = run_date - timedelta(days=1)

            # New campaigns (randomly, ~once a week)
            if random.random() < NEW_CAMPAIGN_CHANCE:
                c = build_new_campaigns(max_campaign_id, day_start)
                max_campaign_id += len(c)
                new_campaigns.append(c)
                all_campaigns = pd.concat([all_campaigns, c], ignore_index=True)

            # New sessions - vary daily traffic to simulate real fluctuation
            s = build_weekly_sessions(all_campaigns, day_start, run_date, max_session_id,
                                      random.randint(130, 320))
            o = build_weekly_orders(s, max_order_id)
            max_session_id += len(s)
            max_order_id   += len(o)
            new_sessions.append(s)
            new_orders.append(o)

        sessions = pd.concat(new_sessions, ignore_index=True)
        orders   = pd.concat(new_orders, ignore_index=True)

        if new_campaigns:
            rows = append_to_bq(client, pd.concat(new_campaigns, ignore_index=True),
                                "campaigns", CAMPAIGN_SCHEMA)
            print(f"Campaigns: {rows:,} rows appended ✓")
        else:
            print("No new campaigns.")
        rows = append_to_bq(client, sessions, "sessions", SESSION_SCHEMA)
        print(f"Sessions : {rows:,} rows appended ✓")
        rows = append_to_bq(client, orders, "orders", ORDER_SCHEMA)
        print(f"Orders   : {rows:,} rows appended ✓")

        cvr = sessions["converted"].mean()
        rev = orders["revenue_usd"].sum()
        print(f"\nSummary ({len(run_dates)} run{'s' if len(run_dates) > 1 else ''}):")
        print(f"  Sessions : {len(sessions):,}  (CVR = {cvr:.2%})")
        print(f"  Orders   : {len(orders):,}  (Revenue = ${rev:,.0f})")

    # Final check: fail the job (and trigger GitHub's failure email) if any recent day is empty
    empty = get_empty_days(client, GAP_CHECK_DAYS)
    if empty:
        sys.exit(f"Gap check FAILED - no sessions on: {', '.join(map(str, empty))}")
    print(f"\nGap check passed: no empty days in the last {GAP_CHECK_DAYS} days.")
    print(f"Done. Looker Studio will reflect the new data on next report load.")


if __name__ == "__main__":
    main()
