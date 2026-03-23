-- =============================================================================
-- Promo ROI Engine — Transformation Pipeline
-- Produces four analytical views consumed by Looker Studio
-- Run in order; each view depends on the raw tables from 01_schema.sql
-- =============================================================================
-- Replace YOUR_PROJECT with your GCP project ID

-- ── View 1: channel_daily ─────────────────────────────────────────────────────
-- Daily rollup of sessions, conversions, revenue, and spend per channel.
-- This is the primary grain for time-series charts in Looker Studio.

CREATE OR REPLACE VIEW `YOUR_PROJECT.promo_roi.channel_daily` AS

WITH sessions_daily AS (
  SELECT
    DATE(timestamp)       AS date,
    channel,
    COUNT(*)              AS sessions,
    COUNTIF(converted)    AS conversions,
    ROUND(
      SAFE_DIVIDE(COUNTIF(converted), COUNT(*)), 4
    )                     AS cvr,
    ROUND(AVG(discount_pct), 4)   AS avg_discount_pct,
    ROUND(AVG(session_duration_s), 1) AS avg_session_duration_s,
    ROUND(AVG(pages_viewed), 2)   AS avg_pages_viewed
  FROM `YOUR_PROJECT.promo_roi.sessions`
  GROUP BY 1, 2
),

orders_daily AS (
  SELECT
    order_date          AS date,
    channel,
    COUNT(*)            AS orders,
    SUM(revenue_usd)    AS revenue_usd,
    SUM(gross_margin_usd) AS gross_margin_usd,
    ROUND(AVG(discount_pct), 4) AS avg_order_discount
  FROM `YOUR_PROJECT.promo_roi.orders`
  GROUP BY 1, 2
),

spend_daily AS (
  -- Allocate campaign budget evenly across campaign days
  SELECT
    day AS date,
    channel,
    SUM(ROUND(budget_usd / GREATEST(DATE_DIFF(end_date, start_date, DAY), 1), 2)) AS spend_usd
  FROM `YOUR_PROJECT.promo_roi.campaigns`,
    UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS day
  GROUP BY 1, 2
)

SELECT
  s.date,
  s.channel,
  s.sessions,
  s.conversions,
  s.cvr,
  s.avg_discount_pct,
  s.avg_session_duration_s,
  s.avg_pages_viewed,
  COALESCE(o.orders, 0)           AS orders,
  COALESCE(o.revenue_usd, 0)      AS revenue_usd,
  COALESCE(o.gross_margin_usd, 0) AS gross_margin_usd,
  COALESCE(sp.spend_usd, 0)       AS spend_usd,
  ROUND(
    SAFE_DIVIDE(COALESCE(o.revenue_usd, 0), NULLIF(COALESCE(sp.spend_usd, 0), 0)), 2
  ) AS roas,
  ROUND(
    SAFE_DIVIDE(COALESCE(sp.spend_usd, 0), NULLIF(COALESCE(o.orders, 0), 0)), 2
  ) AS cost_per_order_usd
FROM sessions_daily s
LEFT JOIN orders_daily o  USING (date, channel)
LEFT JOIN spend_daily  sp USING (date, channel)
ORDER BY s.date, s.channel;


-- ── View 2: discount_elasticity ───────────────────────────────────────────────
-- Price elasticity inputs: average quantity (orders) per session cohort,
-- grouped by channel × discount tier. Feed this into the Python OLS model,
-- or use it directly in Looker Studio to eyeball the demand curve.

CREATE OR REPLACE VIEW `YOUR_PROJECT.promo_roi.discount_elasticity` AS

WITH discount_buckets AS (
  SELECT
    channel,
    CASE
      WHEN discount_pct = 0.00 THEN '0% (control)'
      WHEN discount_pct <= 0.05 THEN '1–5%'
      WHEN discount_pct <= 0.10 THEN '6–10%'
      WHEN discount_pct <= 0.15 THEN '11–15%'
      WHEN discount_pct <= 0.20 THEN '16–20%'
      ELSE '21–25%'
    END                          AS discount_bucket,
    discount_pct,
    COUNT(*)                     AS sessions,
    COUNTIF(converted)           AS conversions,
    ROUND(SAFE_DIVIDE(
      COUNTIF(converted), COUNT(*)
    ), 4)                        AS cvr
  FROM `YOUR_PROJECT.promo_roi.sessions`
  GROUP BY 1, 2, 3
),

with_baseline AS (
  SELECT
    *,
    FIRST_VALUE(cvr) OVER (
      PARTITION BY channel
      ORDER BY discount_pct
    ) AS baseline_cvr
  FROM discount_buckets
)

SELECT
  channel,
  discount_bucket,
  discount_pct,
  sessions,
  conversions,
  cvr,
  baseline_cvr,
  ROUND(
    SAFE_DIVIDE(cvr - baseline_cvr, NULLIF(baseline_cvr, 0)), 4
  )                              AS cvr_lift_pct,
  ROUND(
    SAFE_DIVIDE(
      SAFE_DIVIDE(cvr - baseline_cvr, NULLIF(baseline_cvr, 0)),
      NULLIF(-discount_pct, 0)
    ), 2
  )                              AS implied_elasticity
FROM with_baseline
WHERE sessions >= 50
ORDER BY channel, discount_pct;


-- ── View 3: campaign_roas ─────────────────────────────────────────────────────
-- One row per campaign. Calculates ROAS, CPA, revenue, and attributed orders.
-- Last-touch attribution: credit goes to the channel/campaign of the session.

CREATE OR REPLACE VIEW `YOUR_PROJECT.promo_roi.campaign_roas` AS

WITH campaign_orders AS (
  SELECT
    o.campaign_id,
    COUNT(*)              AS orders,
    SUM(o.revenue_usd)    AS revenue_usd,
    SUM(o.gross_margin_usd) AS gross_margin_usd,
    AVG(o.discount_pct)   AS avg_discount_pct
  FROM `YOUR_PROJECT.promo_roi.orders` o
  WHERE o.campaign_id IS NOT NULL
  GROUP BY 1
),

campaign_sessions AS (
  SELECT
    campaign_id,
    COUNT(*)           AS sessions,
    COUNTIF(converted) AS conversions
  FROM `YOUR_PROJECT.promo_roi.sessions`
  WHERE campaign_id IS NOT NULL
  GROUP BY 1
)

SELECT
  c.campaign_id,
  c.channel,
  c.campaign_name,
  c.start_date,
  c.end_date,
  DATE_DIFF(c.end_date, c.start_date, DAY) + 1  AS duration_days,
  c.discount_pct,
  c.budget_usd                                   AS spend_usd,
  COALESCE(cs.sessions, 0)                       AS sessions,
  COALESCE(cs.conversions, 0)                    AS conversions,
  COALESCE(co.orders, 0)                         AS orders,
  COALESCE(co.revenue_usd, 0)                    AS revenue_usd,
  COALESCE(co.gross_margin_usd, 0)               AS gross_margin_usd,
  ROUND(SAFE_DIVIDE(
    COALESCE(co.revenue_usd, 0), NULLIF(c.budget_usd, 0)
  ), 2)                                          AS roas,
  ROUND(SAFE_DIVIDE(
    COALESCE(co.gross_margin_usd, 0) - c.budget_usd,
    NULLIF(c.budget_usd, 0)
  ), 2)                                          AS roi,
  ROUND(SAFE_DIVIDE(
    c.budget_usd, NULLIF(COALESCE(co.orders, 0), 0)
  ), 2)                                          AS cost_per_order_usd,
  ROUND(SAFE_DIVIDE(
    c.budget_usd, NULLIF(COALESCE(cs.conversions, 0), 0)
  ), 2)                                          AS cost_per_conversion_usd
FROM `YOUR_PROJECT.promo_roi.campaigns` c
LEFT JOIN campaign_sessions cs USING (campaign_id)
LEFT JOIN campaign_orders   co USING (campaign_id)
ORDER BY roas DESC;


-- ── View 4: product_discount_summary ─────────────────────────────────────────
-- Revenue and volume by product × discount tier.
-- Shows whether discount-driven volume offsets the margin hit per SKU.

CREATE OR REPLACE VIEW `YOUR_PROJECT.promo_roi.product_discount_summary` AS

SELECT
  product,
  category,
  CASE
    WHEN discount_pct = 0    THEN '0% (full price)'
    WHEN discount_pct <= 0.10 THEN '1–10%'
    WHEN discount_pct <= 0.20 THEN '11–20%'
    ELSE '21–25%'
  END                          AS discount_tier,
  COUNT(*)                     AS orders,
  SUM(quantity)                AS units_sold,
  ROUND(AVG(final_price_usd), 2) AS avg_selling_price,
  ROUND(AVG(base_price_usd), 2)  AS avg_list_price,
  ROUND(SUM(revenue_usd), 2)     AS total_revenue,
  ROUND(SUM(gross_margin_usd), 2) AS total_margin,
  ROUND(
    SAFE_DIVIDE(SUM(gross_margin_usd), NULLIF(SUM(revenue_usd), 0)), 4
  )                              AS margin_rate
FROM `YOUR_PROJECT.promo_roi.orders`
GROUP BY 1, 2, 3
ORDER BY product, discount_tier;


-- ── Ad-hoc query: country revenue breakdown ───────────────────────────────────
-- Not a view — run interactively in the BigQuery console.
-- Useful for regional pricing strategy analysis.

SELECT
  s.country,
  COUNT(DISTINCT o.order_id)     AS orders,
  ROUND(SUM(o.revenue_usd), 2)   AS revenue_usd,
  ROUND(AVG(o.discount_pct), 4)  AS avg_discount_pct,
  ROUND(AVG(o.final_price_usd), 2) AS avg_order_value,
  ROUND(SAFE_DIVIDE(
    SUM(o.revenue_usd), COUNT(DISTINCT s.session_id)
  ), 4)                          AS revenue_per_session
FROM `YOUR_PROJECT.promo_roi.sessions` s
JOIN `YOUR_PROJECT.promo_roi.orders`   o USING (session_id)
GROUP BY 1
ORDER BY revenue_usd DESC;
