-- =============================================================================
-- Promo ROI Engine — BigQuery Schema DDL
-- Dataset: promo_roi
-- Run once to create all raw tables before loading data
-- =============================================================================

-- Replace YOUR_PROJECT with your GCP project ID

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.promo_roi`
OPTIONS (
  description = "Promo ROI Engine: marketing attribution + price elasticity",
  location    = "US"
);


-- ── campaigns ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE `YOUR_PROJECT.promo_roi.campaigns` (
  campaign_id   STRING  NOT NULL,
  channel       STRING  NOT NULL,
  campaign_name STRING,
  start_date    DATE    NOT NULL,
  end_date      DATE    NOT NULL,
  discount_pct  FLOAT64 NOT NULL,
  budget_usd    FLOAT64,
  cpc_usd       FLOAT64
)
OPTIONS (description = "Paid campaign metadata — one row per campaign flight");


-- ── sessions ──────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE `YOUR_PROJECT.promo_roi.sessions` (
  session_id          STRING  NOT NULL,
  timestamp           TIMESTAMP NOT NULL,
  channel             STRING  NOT NULL,
  campaign_id         STRING,             -- FK → campaigns (NULL for organic/direct)
  discount_pct        FLOAT64 NOT NULL,
  converted           BOOL    NOT NULL,
  session_duration_s  INT64,
  pages_viewed        INT64,
  device              STRING,
  country             STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY channel, converted
OPTIONS (description = "One row per website visit; partitioned by date for cost control");


-- ── orders ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE `YOUR_PROJECT.promo_roi.orders` (
  order_id          STRING  NOT NULL,
  session_id        STRING  NOT NULL,     -- FK → sessions
  channel           STRING  NOT NULL,
  campaign_id       STRING,               -- FK → campaigns (NULL for organic/direct)
  order_date        DATE    NOT NULL,
  product           STRING  NOT NULL,
  category          STRING,
  base_price_usd    FLOAT64 NOT NULL,
  discount_pct      FLOAT64 NOT NULL,
  final_price_usd   FLOAT64 NOT NULL,
  quantity          INT64   NOT NULL,
  revenue_usd       FLOAT64 NOT NULL,
  gross_margin_usd  FLOAT64
)
PARTITION BY order_date
CLUSTER BY channel, product
OPTIONS (description = "One row per order, joined 1:1 with a converted session");
