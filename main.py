"""End-to-end pipeline:  raw CSV + logs  ->  clean  ->  analytics  ->  reports.

    python main.py --input-dir data --output-dir output
"""
import argparse
import json
import os

from pyspark.sql import functions as F

from pipeline import analytics, cleaning, rdd_jobs
from pipeline.schemas import RAW_ORDERS_SCHEMA
from pipeline.spark_session import get_spark


def write_csv(df, path):
    """Single-file CSV is convenient for small report tables."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", default="data")
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--master", default="local[*]")
    args = ap.parse_args()

    spark = get_spark(master=args.master)
    sc = spark.sparkContext
    out = args.output_dir
    orders_path = os.path.abspath(os.path.join(args.input_dir, "orders.csv"))
    log_path = os.path.abspath(os.path.join(args.input_dir, "clickstream.log"))

    # ------------------------------------------------------------------ 1. INGEST
    raw = spark.read.csv(orders_path, header=True, schema=RAW_ORDERS_SCHEMA)

    # ------------------------------------------------------------------ 2. CLEAN (DataFrames)
    clean, rejected = cleaning.clean_orders(spark, raw)
    clean = clean.cache()  # reused by every analytics query below
    rejected = rejected.cache()

    report = cleaning.quality_report(raw, clean, rejected)
    print("\n=== DATA QUALITY REPORT ===")
    report.orderBy(F.desc("count")).show(truncate=False)

    # Partitioned Parquet = columnar + partition pruning for downstream queries.
    clean.write.mode("overwrite").partitionBy("year", "month").parquet(f"{out}/clean_orders")
    rejected.write.mode("overwrite").parquet(f"{out}/rejected_orders")
    write_csv(report, f"{out}/reports/data_quality")

    # ------------------------------------------------------------------ 3. ANALYZE (DataFrames)
    monthly = analytics.monthly_revenue(clean)
    top_products = analytics.top_products_per_category(clean, n=3)
    by_country = analytics.revenue_by_country(clean)
    rfm = analytics.customer_rfm(clean).cache()
    segments = analytics.segment_summary(rfm)

    print("=== MONTHLY REVENUE ===");            monthly.show(truncate=False)
    print("=== TOP 3 PRODUCTS / CATEGORY ===");  top_products.show(15, truncate=False)
    print("=== REVENUE BY COUNTRY ===");         by_country.show(truncate=False)
    print("=== CUSTOMER SEGMENTS (RFM) ===");    segments.show(truncate=False)

    write_csv(monthly, f"{out}/reports/monthly_revenue")
    write_csv(top_products, f"{out}/reports/top_products_per_category")
    write_csv(by_country, f"{out}/reports/revenue_by_country")
    write_csv(segments, f"{out}/reports/customer_segments")
    rfm.write.mode("overwrite").parquet(f"{out}/customer_rfm")

    # ------------------------------------------------------------------ 4. RDD JOBS
    funnel = rdd_jobs.clickstream_funnel(sc, log_path)
    pairs = rdd_jobs.copurchase_pairs(clean, top_n=10)

    print("=== CLICKSTREAM (RDD) ===");  print(json.dumps(funnel, indent=2))
    print("=== TOP CO-PURCHASED PAIRS (RDD) ===")
    for p in pairs:
        print(f"  {p['product_a']} + {p['product_b']}: {p['orders_together']} orders")

    os.makedirs(f"{out}/reports", exist_ok=True)
    with open(f"{out}/reports/clickstream_funnel.json", "w") as f:
        json.dump(funnel, f, indent=2)
    with open(f"{out}/reports/copurchase_pairs.json", "w") as f:
        json.dump(pairs, f, indent=2)

    print(f"\nDone. Outputs written to ./{out}/")
    spark.stop()


if __name__ == "__main__":
    main()
