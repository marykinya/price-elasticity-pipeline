"""
Promo ROI Engine — Synthetic Dataset Generator
Generates three interlocking tables: sessions, orders, campaigns
Designed to produce realistic price elasticity + attribution signals
"""

import os
import random
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
from faker import Faker

from config import CHANNELS, PRODUCTS, DISCOUNT_TIERS, PAID_CHANNELS

fake = Faker()
np.random.seed(42)
random.seed(42)

START_DATE = datetime(2025, 1, 1)
END_DATE   = datetime.combine(date.today(), datetime.min.time())
N_SESSIONS = 80_000
N_CAMPAIGNS = 48


# Helpers

def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta),
                              hours=random.randint(0, 23),
                              minutes=random.randint(0, 59))

def session_duration(channel, converted):
    base = {"organic_search": 180, "paid_search": 120, "email": 210,
            "social_organic": 90, "paid_social": 100, "direct": 240, "referral": 150}
    mu = base.get(channel, 150) * (1.6 if converted else 1.0)
    return max(10, int(np.random.lognormal(np.log(mu), 0.5)))

def pages_viewed(channel, converted):
    base = {"organic_search": 4, "paid_search": 3, "email": 5,
            "social_organic": 2, "paid_social": 3, "direct": 6, "referral": 3}
    mu = base.get(channel, 3) * (1.4 if converted else 1.0)
    return max(1, int(np.random.poisson(mu)))


# Campaigns

def build_campaigns():
    rows = []
    campaign_id = 1

    for month_offset in range(15):  # Jan 2023 – Mar 2024
        month_start = START_DATE + timedelta(days=30 * month_offset)
        for ch in PAID_CHANNELS:
            n = random.randint(2, 5)
            for _ in range(n):
                discount = random.choice(DISCOUNT_TIERS)
                spend = round(random.uniform(800, 8000), 2)
                rows.append({
                    "campaign_id":   f"CMP{campaign_id:04d}",
                    "channel":       ch,
                    "campaign_name": f"{ch.replace('_',' ').title()} – {fake.catch_phrase()[:40]}",
                    "start_date":    month_start.date(),
                    "end_date":      (month_start + timedelta(days=random.randint(7, 28))).date(),
                    "discount_pct":  discount,
                    "budget_usd":    spend,
                    "cpc_usd":       round(random.uniform(0.40, 4.50), 2),
                })
                campaign_id += 1

    return pd.DataFrame(rows)


# Sessions

def build_sessions(campaigns_df):
    ch_names   = list(CHANNELS.keys())
    ch_weights = [CHANNELS[c]["weight"] for c in ch_names]

    sessions = []
    for i in range(N_SESSIONS):
        session_id = f"S{i+1:07d}"
        channel    = random.choices(ch_names, weights=ch_weights)[0]
        ts         = rand_date(START_DATE, END_DATE)

        # Link paid channels to a campaign that was live on that date
        campaign_id = None
        if channel in PAID_CHANNELS:
            live = campaigns_df[
                (campaigns_df["channel"] == channel) &
                (campaigns_df["start_date"] <= ts.date()) &
                (campaigns_df["end_date"]   >= ts.date())
            ]
            if not live.empty:
                campaign_id = live.sample(1)["campaign_id"].values[0]

        # Discount from campaign (or zero)
        discount = 0.0
        if campaign_id:
            discount = campaigns_df.loc[
                campaigns_df["campaign_id"] == campaign_id, "discount_pct"
            ].values[0]

        # Conversion: base CVR * elasticity lift from discount
        base_cvr   = CHANNELS[channel]["base_cvr"]
        elasticity = CHANNELS[channel]["elasticity"]
        lift       = 1 + elasticity * (-discount) if discount > 0 else 1.0
        cvr        = min(base_cvr * lift + np.random.normal(0, 0.005), 0.95)
        converted  = random.random() < max(cvr, 0.005)

        sessions.append({
            "session_id":         session_id,
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


# Orders

def build_orders(sessions_df):
    converted = sessions_df[sessions_df["converted"]].copy()
    products  = list(PRODUCTS.keys())

    orders = []
    for _, s in converted.iterrows():
        product     = random.choice(products)
        base_price  = PRODUCTS[product]["base_price"]
        discount    = s["discount_pct"]
        final_price = round(base_price * (1 - discount), 2)
        qty         = random.choices([1, 2, 3], weights=[0.75, 0.18, 0.07])[0]
        revenue     = round(final_price * qty, 2)
        cogs_rate   = random.uniform(0.25, 0.45)
        margin      = round(revenue * (1 - cogs_rate), 2)

        orders.append({
            "order_id":          f"ORD{len(orders)+1:07d}",
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


if __name__ == "__main__":
    print("Building campaigns...")
    campaigns = build_campaigns()

    print("Building sessions...")
    sessions  = build_sessions(campaigns)

    print("Building orders...")
    orders    = build_orders(sessions)

    os.makedirs("data", exist_ok=True)
    campaigns.to_csv("data/campaigns.csv", index=False)
    sessions.to_csv("data/sessions.csv",   index=False)
    orders.to_csv("data/orders.csv",       index=False)

    conv_rate = sessions["converted"].mean()
    total_rev = orders["revenue_usd"].sum()
    print(f"\n✓ Campaigns : {len(campaigns):,}")
    print(f"✓ Sessions  : {len(sessions):,}  (CVR = {conv_rate:.2%})")
    print(f"✓ Orders    : {len(orders):,}  (Revenue = ${total_rev:,.0f})")
    print("\nChannel breakdown:")
    print(sessions.groupby("channel")["converted"].agg(["count","mean"]).rename(
        columns={"count":"sessions","mean":"cvr"}
    ).sort_values("cvr", ascending=False).to_string())
