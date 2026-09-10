from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "certification_pipeline_lab"
VOLUME = "raw_files"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

ORDERS_PATH = f"{VOLUME_ROOT}/orders"
STATUS_PATH = f"{VOLUME_ROOT}/status"
CUSTOMERS_CDC_PATH = f"{VOLUME_ROOT}/customers_cdc"

@dp.table(
    name="orders_bronze",
    comment="Raw orders incrementally ingested from JSON files with Auto Loader",
    table_properties={
        "quality": "bronze"
    }
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(ORDERS_PATH)
        .withColumn(
            "processing_time",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.col("_metadata.file_name")
        )
    )

@dp.table(
    name="status_bronze",
    comment="Raw order status events incrementally ingested from JSON files",
    table_properties={
        "quality": "bronze"
    }
)
def status_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(STATUS_PATH)
        .withColumn(
            "processing_time",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.col("_metadata.file_name")
        )
    )

@dp.table(
    name="customers_cdc_bronze",
    comment="Raw customer CDC events ingested incrementally using Auto Loader",
    table_properties={
        "quality": "bronze"
    }
)
def customers_cdc_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(CUSTOMERS_CDC_PATH)
        .withColumn(
            "processing_time",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.col("_metadata.file_name")
        )
    )



