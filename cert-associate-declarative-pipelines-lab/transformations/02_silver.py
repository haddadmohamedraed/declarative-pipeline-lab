from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="orders_silver",
    comment="Cleaned and typed orders with pipeline data quality expectations",
    table_properties={
        "quality": "silver"
    }
)
@dp.expect(
    "valid_notifications",
    "notifications IN ('Y', 'N')"
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
def orders_silver():

    return (
        spark.readStream
        .table("orders_bronze")

        .select(
            F.col("order_id").cast("long").alias("order_id"),

            F.to_timestamp(
                "order_timestamp"
            ).alias("order_timestamp"),

            F.col("customer_id")
                .cast("long")
                .alias("customer_id"),

            F.col("country_code"),

            F.col("amount")
                .cast("double")
                .alias("amount"),

            F.col("notifications"),

            F.col("processing_time"),

            F.col("source_file")
        )

        .withColumn(
            "order_date",
            F.to_date("order_timestamp")
        )
    )

@dp.table(
    name="status_silver",
    comment="Typed and validated order status events",
    table_properties={
        "quality": "silver"
    }
)
@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL"
)
@dp.expect(
    "known_status",
    """
    status IN (
        'CREATED',
        'SHIPPED',
        'DELIVERED',
        'CANCELLED'
    )
    """
)
def status_silver():

    return (
        spark.readStream
        .table("status_bronze")

        .select(
            F.col("order_id")
                .cast("long")
                .alias("order_id"),

            F.col("status"),

            F.to_timestamp(
                "status_timestamp"
            ).alias("status_timestamp"),

            F.col("processing_time"),

            F.col("source_file")
        )
    )

@dp.table(
    name="orders_strict_validation",
    comment="Lab-only table used to demonstrate FAIL expectation behavior"
)
# test drop on failed expectation
# @dp.expect_or_fail(
#     "notifications_must_be_valid",
#     "notifications IN ('Y', 'N')"
# )
def orders_strict_validation():

    return (
        spark.readStream
        .table("orders_bronze")
    )

@dp.table(
    name="orders_enriched_silver",
    comment="Incrementally enriched orders using a stream-static country lookup",
    table_properties={
        "quality": "silver"
    }
)
def orders_enriched_silver():

    orders = (
        spark.readStream
        .table("orders_silver")
    )

    countries = (
        spark.read
        .table(
            "workspace.certification_pipeline_lab.ref_countries"
        )
    )

    return (
        orders.alias("o")

        .join(
            F.broadcast(countries).alias("c"),
            F.col("o.country_code") == F.col("c.country_code"),
            "left"
        )

        .select(
            F.col("o.order_id"),
            F.col("o.order_timestamp"),
            F.col("o.order_date"),
            F.col("o.customer_id"),

            F.col("o.country_code"),
            F.col("c.country_name"),
            F.col("c.capital"),

            F.col("o.amount"),
            F.col("o.notifications"),

            F.col("o.processing_time"),
            F.col("o.source_file")
        )
    )




















