from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================
# SCD TYPE 1
# ============================================================

dp.create_streaming_table(
    name="customers_scd1",
    comment="Current customer state maintained with AUTO CDC SCD Type 1",
    table_properties={
        "quality": "silver"
    }
)

dp.create_auto_cdc_flow(
    target="customers_scd1",

    source="customers_cdc_bronze",

    keys=[
        "customer_id"
    ],

    sequence_by=F.col("sequence"),

    apply_as_deletes=F.expr(
        "operation = 'DELETE'"
    ),

    except_column_list=[
        "operation",
        "sequence",
        "processing_time",
        "source_file"
    ],

    stored_as_scd_type="1",

    name="customers_scd1_flow"
)

# ============================================================
# SCD TYPE 2
# ============================================================

dp.create_streaming_table(
    name="customers_scd2",
    comment="Historical customer dimension maintained with AUTO CDC SCD Type 2",
    table_properties={
        "quality": "silver"
    }
)

dp.create_auto_cdc_flow(
    target="customers_scd2",

    source="customers_cdc_bronze",

    keys=[
        "customer_id"
    ],

    sequence_by=F.col("sequence"),

    apply_as_deletes=F.expr(
        "operation = 'DELETE'"
    ),

    except_column_list=[
        "operation",
        "sequence",
        "processing_time",
        "source_file"
    ],

    stored_as_scd_type="2",

    name="customers_scd2_flow"
)