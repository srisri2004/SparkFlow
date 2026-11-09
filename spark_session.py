from pyspark.sql import SparkSession


def get_spark(app_name: str = "ecommerce-pipeline", master: str = "local[*]") -> SparkSession:
    """Create (or reuse) a SparkSession with sensible defaults."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Bad casts / unparseable dates should become NULL (then be rejected by
        # our own validation) instead of crashing the job. Spark 4 turns ANSI on
        # by default, so we pin it off explicitly.
        .config("spark.sql.ansi.enabled", "false")
        # Small dataset locally -> far fewer shuffle partitions than the default 200.
        .config("spark.sql.shuffle.partitions", "8")
        # Adaptive Query Execution coalesces shuffle partitions & handles skew.
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
