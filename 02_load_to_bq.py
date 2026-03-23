"""
Promo ROI Engine — BigQuery Data Loader
Loads the three synthetic CSVs into BigQuery using the Python client.

Usage:
    pip install google-cloud-bigquery pandas db-dtypes
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
    python 02_load_to_bq.py --project YOUR_PROJECT --data-dir ./data
"""

import argparse
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

DATASET = "promo_roi"

# Schema maps: matches the DDL in 01_schema.sql
TABLE_SCHEMAS = {
    "campaigns": [
        bigquery.SchemaField("campaign_id",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("channel",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("campaign_name",  "STRING"),
        bigquery.SchemaField("start_date",     "DATE",      mode="REQUIRED"),
        bigquery.SchemaField("end_date",       "DATE",      mode="REQUIRED"),
        bigquery.SchemaField("discount_pct",   "FLOAT64",   mode="REQUIRED"),
        bigquery.SchemaField("budget_usd",     "FLOAT64"),
        bigquery.SchemaField("cpc_usd",        "FLOAT64"),
    ],
    "sessions": [
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
    ],
    "orders": [
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
    ],
}

PARTITION_FIELDS = {
    "sessions": bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp",
    ),
    "orders": bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="order_date",
    ),
}

CLUSTER_FIELDS = {
    "sessions": ["channel", "converted"],
    "orders":   ["channel", "product"],
}


def load_table(client, project, data_dir, table_name):
    csv_path = Path(data_dir) / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    print(f"  Loading {table_name}...", end=" ", flush=True)

    table_ref = f"{project}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMAS[table_name],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
    )
    if table_name in PARTITION_FIELDS:
        job_config.time_partitioning = PARTITION_FIELDS[table_name]
    if table_name in CLUSTER_FIELDS:
        job_config.clustering_fields = CLUSTER_FIELDS[table_name]

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()  # wait

    print(f"{job.output_rows:,} rows loaded ✓")


def main():
    parser = argparse.ArgumentParser(description="Load Promo ROI data to BigQuery")
    parser.add_argument("--project",  required=True, help="GCP project ID")
    parser.add_argument("--data-dir", default="./data", help="Directory with CSV files")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    print(f"Project : {args.project}")
    print(f"Dataset : {DATASET}\n")

    for table in ["campaigns", "sessions", "orders"]:
        load_table(client, args.project, args.data_dir, table)

    print("\nAll tables loaded. Run 03_transforms.sql next.")


if __name__ == "__main__":
    main()
