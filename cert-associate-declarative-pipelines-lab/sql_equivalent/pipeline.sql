CREATE OR REFRESH STREAMING TABLE orders_bronze AS

SELECT
    *,
    current_timestamp() AS processing_time,
    _metadata.file_name AS source_file

FROM STREAM read_files(
    '/Volumes/workspace/certification_pipeline_lab/raw_files/orders',
    format => 'json'
);

---------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE orders_silver
(
    CONSTRAINT valid_notifications
        EXPECT (notifications IN ('Y', 'N')),

    CONSTRAINT valid_customer_id
        EXPECT (customer_id IS NOT NULL)
        ON VIOLATION DROP ROW
)
AS

SELECT
    CAST(order_id AS BIGINT) AS order_id,
    TIMESTAMP(order_timestamp) AS order_timestamp,
    CAST(customer_id AS BIGINT) AS customer_id,
    country_code,
    CAST(amount AS DOUBLE) AS amount,
    notifications,
    processing_time,
    source_file,
    DATE(order_timestamp) AS order_date

FROM STREAM orders_bronze;

---------------------------------------------------------------

CREATE TEMPORARY VIEW orders_gold_input AS

SELECT
    order_id,
    order_date,
    order_timestamp,
    customer_id,
    country_code,
    amount,
    notifications

FROM orders_silver

WHERE amount > 0
  AND notifications IN ('Y', 'N');

---------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_orders_by_date AS

SELECT
    order_date,

    COUNT(*) AS total_orders,

    SUM(amount) AS total_amount,

    AVG(amount) AS avg_order_amount,

    MAX(amount) AS max_order_amount

FROM orders_gold_input

GROUP BY order_date;

---------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE orders_enriched_silver AS

SELECT
    o.order_id,
    o.order_timestamp,
    o.order_date,
    o.customer_id,

    o.country_code,
    c.country_name,
    c.capital,

    o.amount,
    o.notifications,

    o.processing_time,
    o.source_file

FROM STREAM orders_silver AS o

LEFT JOIN
    workspace.certification_pipeline_lab.ref_countries AS c

ON o.country_code = c.country_code;

---------------------------------------------------------------

CREATE TEMPORARY VIEW latest_status AS

SELECT
    order_id,
    status,
    status_timestamp,
    processing_time,
    source_file

FROM status_silver

QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY status_timestamp DESC
    ) = 1;

---------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW full_order_info_gold AS

SELECT
    o.order_id,
    o.order_timestamp,
    o.order_date,
    o.customer_id,

    o.country_code,
    c.country_name,
    c.capital,

    o.amount,
    o.notifications,

    s.status,
    s.status_timestamp

FROM orders_silver AS o

LEFT JOIN latest_status AS s
    ON o.order_id = s.order_id

LEFT JOIN
    workspace.certification_pipeline_lab.ref_countries AS c
    ON o.country_code = c.country_code;

---------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW delivered_orders_gold AS

SELECT *

FROM full_order_info_gold

WHERE status = 'DELIVERED';

---------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW cancelled_orders_gold AS

SELECT *

FROM full_order_info_gold

WHERE status = 'CANCELLED';

---------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE
    order_status_events_streaming
AS

SELECT
    o.order_id,
    o.order_timestamp,
    s.status,
    s.status_timestamp,
    o.customer_id,
    o.country_code,
    o.amount

FROM STREAM(orders_silver)
    WATERMARK order_timestamp
    DELAY OF INTERVAL 2 DAYS AS o

INNER JOIN STREAM(status_silver)
    WATERMARK status_timestamp
    DELAY OF INTERVAL 2 DAYS AS s

ON o.order_id = s.order_id

AND s.status_timestamp >= o.order_timestamp

AND s.status_timestamp
    <= o.order_timestamp + INTERVAL 1 DAY;

---------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE customers_scd1;

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO
    customers_scd1

FROM stream(customers_cdc_bronze)

KEYS (
    customer_id
)

APPLY AS DELETE WHEN
    operation = 'DELETE'

SEQUENCE BY
    sequence

COLUMNS * EXCEPT (
    operation,
    sequence,
    processing_time,
    source_file
)

STORED AS
    SCD TYPE 1;

---------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE customers_scd2;

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO
    customers_scd2

FROM stream(customers_cdc_bronze)

KEYS (
    customer_id
)

APPLY AS DELETE WHEN
    operation = 'DELETE'

SEQUENCE BY
    sequence

COLUMNS * EXCEPT (
    operation,
    sequence,
    processing_time,
    source_file
)

STORED AS
    SCD TYPE 2;

---------------------------------------------------------------