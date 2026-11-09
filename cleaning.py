"""DataFrame-based cleaning: standardise, validate, quarantine bad rows, de-duplicate."""
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_STATUS = ["completed", "shipped", "cancelled", "returned", "pending"]
MAX_QUANTITY = 100  # anything above is treated as a data-entry outlier

COUNTRY_MAP = [
    ("usa", "USA"), ("us", "USA"), ("u.s.a", "USA"), ("united states", "USA"),
    ("uk", "UK"), ("gb", "UK"), ("united kingdom", "UK"),
    ("india", "India"), ("in", "India"),
    ("germany", "Germany"), ("de", "Germany"),
    ("canada", "Canada"), ("ca", "Canada"),
    ("australia", "Australia"), ("au", "Australia"),
]


def _blank_to_null(df: DataFrame) -> DataFrame:
    """Trim every column and turn empty / 'null'-like strings into real NULLs."""
    null_tokens = ["", "null", "none", "n/a", "na", "nan"]
    for c in df.columns:
        trimmed = F.trim(F.col(c))
        df = df.withColumn(
            c, F.when(F.lower(trimmed).isin(null_tokens), None).otherwise(trimmed)
        )
    return df


def _parse_date(col: F.Column) -> F.Column:
    """Try several date formats in order; first one that parses wins."""
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col, "MM-dd-yyyy"),
        F.to_date(col, "yyyy/MM/dd"),
    )


def clean_orders(spark: SparkSession, raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return (clean_df, rejected_df).

    Rejected rows keep a `reject_reason` so data quality can be audited.
    """
    df = _blank_to_null(raw)

    df = (
        df.withColumn("order_id", F.upper("order_id"))
        .withColumn("customer_id", F.upper("customer_id"))
        .withColumn("product_id", F.upper("product_id"))
        .withColumn("category", F.initcap("category"))
        .withColumn("category", F.coalesce("category", F.lit("Unknown")))  # imputation
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn(
            "unit_price",
            F.regexp_replace("unit_price", r"[$,]", "").cast("double"),
        )
        .withColumn("order_date", _parse_date(F.col("order_date")))
        .withColumn("status", F.lower("status"))
    )

    # Standardise country names via a small broadcast lookup join.
    country_lookup = spark.createDataFrame(COUNTRY_MAP, ["country_key", "country_std"])
    df = (
        df.withColumn("country_key", F.lower("country"))
        .join(F.broadcast(country_lookup), "country_key", "left")
        .withColumn(
            "country",
            F.coalesce("country_std", F.initcap("country_key"), F.lit("Unknown")),
        )
        .drop("country_key", "country_std")
    )

    reason = (
        F.when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("order_date").isNull(), "bad_date")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "bad_quantity")
        .when(F.col("quantity") > MAX_QUANTITY, "outlier_quantity")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "bad_price")
        .when(
            F.col("status").isNull() | ~F.col("status").isin(VALID_STATUS),
            "bad_status",
        )
    )
    df = df.withColumn("reject_reason", reason)

    rejected = df.filter(F.col("reject_reason").isNotNull())

    clean = (
        df.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        # The same order line may be exported twice by the source system.
        .dropDuplicates(
            ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_date"]
        )
        .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )
    return clean, rejected


def quality_report(raw: DataFrame, clean: DataFrame, rejected: DataFrame) -> DataFrame:
    """Rejection counts per reason plus overall totals."""
    total_raw = raw.count()
    total_clean = clean.count()
    total_rejected = rejected.count()
    by_reason = rejected.groupBy("reject_reason").count()
    extra = raw.sparkSession.createDataFrame(
        [
            ("duplicate_rows_removed", total_raw - total_clean - total_rejected),
            ("TOTAL_RAW", total_raw),
            ("TOTAL_CLEAN", total_clean),
        ],
        ["reject_reason", "count"],
    )
    return by_reason.unionByName(extra.select("reject_reason", F.col("count").cast("long")))
