from pyspark import pipelines as dp
from pyspark.sql import functions as F

dp.create_streaming_table(
    name="order_status_events_streaming",
    comment="""
    Stateful stream-stream correlation between orders and status events
    """,
    table_properties={
        "quality": "silver"
    }
)
@dp.append_flow(
    target="order_status_events_streaming",
    name="join_order_status_streams"
)
def join_order_status_streams():

    orders = (
        spark.readStream
        .table("orders_silver")

        .select(
            "order_id",
            "order_timestamp",
            "customer_id",
            "country_code",
            "amount"
        )

        .withWatermark(
            "order_timestamp",
            "2 days"
        )
    )

    statuses = (
        spark.readStream
        .table("status_silver")

        .select(
            "order_id",
            "status",
            "status_timestamp"
        )

        .withWatermark(
            "status_timestamp",
            "2 days"
        )
    )

    joined = (
        orders.alias("o")

        .join(
            statuses.alias("s"),

            F.expr("""
                o.order_id = s.order_id
                AND s.status_timestamp >= o.order_timestamp
                AND s.status_timestamp
                    <= o.order_timestamp + INTERVAL 1 DAY
            """),

            "inner"
        )
    )

    return (
        joined

        .select(
            F.col("o.order_id").alias("order_id"),

            F.col("o.order_timestamp")
                .alias("order_timestamp"),

            F.col("s.status_timestamp")
                .alias("status_timestamp"),

            F.col("s.status")
                .alias("status"),

            F.col("o.customer_id")
                .alias("customer_id"),

            F.col("o.country_code")
                .alias("country_code"),

            F.col("o.amount")
                .alias("amount"),

            (
                F.col("s.status_timestamp")
                - F.col("o.order_timestamp")
            ).alias("status_delay")
        )
    )
