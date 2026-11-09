"""DataFrame analytics on the cleaned orders: aggregations + window functions."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REVENUE_STATUSES = ["completed", "shipped"]  # cancelled/returned/pending excluded


def _sales(clean: DataFrame) -> DataFrame:
    return clean.filter(F.col("status").isin(REVENUE_STATUSES))


def monthly_revenue(clean: DataFrame) -> DataFrame:
    """Revenue, orders, customers per month + month-over-month growth (lag window)."""
    w = Window.orderBy("year_month")
    return (
        _sales(clean)
        .groupBy(F.date_format("order_date", "yyyy-MM").alias("year_month"))
        .agg(
            F.round(F.sum("line_total"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .withColumn("prev_revenue", F.lag("revenue").over(w))
        .withColumn(
            "mom_growth_pct",
            F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 2),
        )
        .drop("prev_revenue")
        .orderBy("year_month")
    )


def top_products_per_category(clean: DataFrame, n: int = 3) -> DataFrame:
    """Top-N products by revenue inside each category (row_number window)."""
    w = Window.partitionBy("category").orderBy(F.desc("revenue"))
    return (
        _sales(clean)
        .groupBy("category", "product_id")
        .agg(F.round(F.sum("line_total"), 2).alias("revenue"), F.sum("quantity").alias("units"))
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= n)
        .orderBy("category", "rank")
    )


def revenue_by_country(clean: DataFrame) -> DataFrame:
    sales = _sales(clean)
    total = sales.agg(F.sum("line_total")).first()[0]
    return (
        sales.groupBy("country")
        .agg(
            F.round(F.sum("line_total"), 2).alias("revenue"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .withColumn("share_pct", F.round(F.col("revenue") / F.lit(total) * 100, 2))
        .orderBy(F.desc("revenue"))
    )


def customer_rfm(clean: DataFrame) -> DataFrame:
    """RFM segmentation: Recency, Frequency, Monetary scored 1-4 with ntile()."""
    sales = _sales(clean)
    ref_date = sales.agg(F.max("order_date")).first()[0]

    rfm = sales.groupBy("customer_id").agg(
        F.datediff(F.lit(ref_date), F.max("order_date")).alias("recency_days"),
        F.countDistinct("order_id").alias("frequency"),
        F.round(F.sum("line_total"), 2).alias("monetary"),
    )

    # Higher score = better. Recent (small recency_days) should score 4.
    r = F.ntile(4).over(Window.orderBy(F.desc("recency_days")))
    f = F.ntile(4).over(Window.orderBy("frequency"))
    m = F.ntile(4).over(Window.orderBy("monetary"))

    scored = (
        rfm.withColumn("r_score", r)
        .withColumn("f_score", f)
        .withColumn("m_score", m)
        .withColumn("rfm_total", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )
    return scored.withColumn(
        "segment",
        F.when(F.col("rfm_total") >= 10, "Champions")
        .when(F.col("rfm_total") >= 7, "Loyal")
        .when(F.col("rfm_total") >= 4, "At Risk")
        .otherwise("Lost"),
    ).orderBy(F.desc("monetary"))


def segment_summary(rfm: DataFrame) -> DataFrame:
    return (
        rfm.groupBy("segment")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.avg("monetary"), 2).alias("avg_spend"),
            F.round(F.sum("monetary"), 2).alias("total_spend"),
        )
        .orderBy(F.desc("total_spend"))
    )
