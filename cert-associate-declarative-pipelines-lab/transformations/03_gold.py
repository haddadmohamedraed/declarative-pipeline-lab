from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.temporary_view()
def orders_gold_input():

    return (
        spark.read
        .table("orders_silver")

        .filter(
            (F.col("amount") > 0)
            &
            (F.col("notifications").isin("Y", "N"))
        )

        .select(
            "order_id",
            "order_date",
            "order_timestamp",
            "customer_id",
            "country_code",
            "amount",
            "notifications"
        )
    )

@dp.materialized_view(
    name="gold_orders_by_date",
    comment="Daily Gold order metrics derived from the current Silver order state",
    table_properties={
        "quality": "gold"
    }
)
def gold_orders_by_date():

    return (
        spark.read
        .table("orders_gold_input")

        .groupBy(
            "order_date"
        )

        .agg(
            F.count("*")
                .alias("total_orders"),

            F.sum("amount")
                .alias("total_amount"),

            F.avg("amount")
                .alias("avg_order_amount"),

            F.max("amount")
                .alias("max_order_amount")
        )
    )

@dp.temporary_view()
def latest_status():

    status_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            F.col("status_timestamp").desc()
        )
    )

    return (
        spark.read
        .table("status_silver")

        .withColumn(
            "_row_number",
            F.row_number().over(status_window)
        )

        .filter(
            F.col("_row_number") == 1
        )

        .drop("_row_number")
    )

@dp.materialized_view(
    name="full_order_info_gold",
    comment="Current order state enriched with latest status and country information",
    table_properties={
        "quality": "gold"
    }
)
def full_order_info_gold():

    orders = (
        spark.read
        .table("orders_silver")
    )

    statuses = (
        spark.read
        .table("latest_status")
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
            statuses.alias("s"),
            F.col("o.order_id") == F.col("s.order_id"),
            "left"
        )

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

            F.col("s.status"),
            F.col("s.status_timestamp")
        )
    )

@dp.materialized_view(
    name="delivered_orders_gold",
    comment="Current orders whose latest status is DELIVERED",
    table_properties={
        "quality": "gold"
    }
)
def delivered_orders_gold():

    return (
        spark.read
        .table("full_order_info_gold")

        .filter(
            F.col("status") == "DELIVERED"
        )
    )

@dp.materialized_view(
    name="cancelled_orders_gold",
    comment="Current orders whose latest status is CANCELLED",
    table_properties={
        "quality": "gold"
    }
)
def cancelled_orders_gold():

    return (
        spark.read
        .table("full_order_info_gold")

        .filter(
            F.col("status") == "CANCELLED"
        )
    )