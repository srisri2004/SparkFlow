from pyspark.sql.types import StringType, StructField, StructType

# Everything is read as STRING on purpose: we do the type casting ourselves in
# cleaning.py so that malformed values are caught and reported, not silently
# dropped by the CSV reader.
RAW_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("category", StringType()),
        StructField("quantity", StringType()),
        StructField("unit_price", StringType()),
        StructField("order_date", StringType()),
        StructField("country", StringType()),
        StructField("status", StringType()),
    ]
)
