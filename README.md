# E-Commerce Analytics Pipeline (PySpark: DataFrames + RDDs)

A distributed data-processing project that ingests **messy** order data and raw
clickstream logs, cleans and validates them, and produces business analytics.
It deliberately uses each Spark API where it fits best:

| Stage | API | Why |
|-------|-----|-----|
| Ingest, clean, validate, aggregate, window analytics | **DataFrames / Spark SQL functions** | Schema-aware, optimised by Catalyst, columnar output |
| Parse free-form log lines, build funnels, market-basket pairs | **RDDs** | Custom per-record logic, regex parsing, combinatorics, accumulators |

## Project layout

```
generate_data.py        synthetic messy data (orders.csv + clickstream.log)
main.py                 orchestrates the whole pipeline
pipeline/
  spark_session.py      SparkSession config (AQE, shuffle partitions, ANSI off)
  schemas.py            explicit all-string raw schema
  cleaning.py           clean_orders(), quality_report()           [DataFrame]
  analytics.py          revenue, top-N, country share, RFM         [DataFrame + windows]
  rdd_jobs.py           clickstream_funnel(), copurchase_pairs()   [RDD]
tests/test_cleaning.py  pytest unit tests (local Spark)
```

## Quick start

Requires Python 3.9+ and Java 8/11/17/21 (needed by Spark).

```bash
pip install -r requirements.txt
python generate_data.py --orders 20000 --out data      # ~38k order lines, 100k log lines
python main.py --input-dir data --output-dir output
pytest -q                                              # run unit tests
```

Scale it up: `python generate_data.py --orders 2000000` produces several GB-scale
data you can run locally, or point `--master` at a cluster (see below).

## What the pipeline does

### 1. Cleaning (DataFrames) - `pipeline/cleaning.py`
- Reads everything as strings, then casts explicitly so bad values are *reported*, not lost
- Trims whitespace; converts `""`, `NULL`, `N/A` to real nulls
- Parses **4 different date formats** with `coalesce(to_date(...))`
- Strips `$` and `,` from prices (`"$1,200.50"` -> `1200.5`)
- Standardises country names (`us`, `United States`, `U.S.A` -> `USA`) via a **broadcast join**
- Imputes missing category as `Unknown`
- Validates and **quarantines** bad rows with a `reject_reason` (missing id, bad date,
  non-positive/non-numeric quantity or price, outlier quantity, invalid status)
- Removes exact duplicate lines; adds `line_total`, `year`, `month`
- Writes clean data as **Parquet partitioned by year/month**

### 2. Analytics (DataFrames + window functions) - `pipeline/analytics.py`
- **Monthly revenue** with month-over-month growth (`lag`)
- **Top 3 products per category** (`row_number` over partition)
- **Revenue by country** with share of total
- **RFM customer segmentation** (`ntile`) -> Champions / Loyal / At Risk / Lost

### 3. RDD jobs - `pipeline/rdd_jobs.py`
- **Clickstream funnel**: regex-parse raw log text, count malformed lines with an
  **accumulator**, `reduceByKey` to compute view -> cart -> purchase conversion per product
- **Co-purchase pairs**: `groupByKey` baskets -> `itertools.combinations` -> `reduceByKey`
  -> `takeOrdered` (basic market-basket analysis)

## Outputs (`output/`)

```
clean_orders/            Parquet, partitioned by year/month
rejected_orders/         quarantined rows with reject_reason
customer_rfm/            Parquet
reports/
  data_quality/          rejection counts per reason
  monthly_revenue/       CSV
  top_products_per_category/
  revenue_by_country/
  customer_segments/
  clickstream_funnel.json
  copurchase_pairs.json
```

## Running on a cluster

```bash
zip -r pipeline.zip pipeline
spark-submit --master yarn --deploy-mode cluster \
  --py-files pipeline.zip \
  --conf spark.sql.shuffle.partitions=400 \
  main.py --input-dir hdfs:///data/ecom --output-dir hdfs:///out/ecom
```
(On a real cluster, use `hdfs://` / `s3a://` paths; note `main.py` calls `os.path.abspath`
on inputs, so remove that for remote URIs.)

## Performance notes
- `cache()` on the clean DataFrame since five+ queries reuse it
- Broadcast join for the tiny country lookup (no shuffle of the big table)
- Adaptive Query Execution on; `shuffle.partitions` tuned down for local mode
- Parquet + `partitionBy` for pruning and cheap downstream reads
- RDD `reduceByKey` (map-side combine) is used instead of `groupByKey` wherever possible;
  `groupByKey` appears only where all values per key are genuinely needed (baskets)
- Accumulator caveat: counts can over-count if a task is retried; fine for monitoring, not for
  business-critical numbers

## Ideas to extend
- Replace the CSV source with a Kafka topic + Structured Streaming
- Add Delta Lake / Iceberg for ACID upserts of corrected rows
- Join clickstream (user, product) with orders to compute true attributed conversion
- Add data-quality thresholds that fail the job (e.g. >10% rejected)
