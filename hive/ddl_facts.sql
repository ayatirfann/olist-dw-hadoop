-- =====================================================================
-- Hive external tables over the fact Parquet files written by
-- spark/build_facts.py. Data stays in the curated zone (HDFS_CURATED); DROP of an
-- EXTERNAL table never deletes the files, so this script is re-runnable.
--
-- fact_order_item and fact_payment are partitioned by purchase_year
-- (one folder per year, e.g. purchase_year=2017). Hive must be told which
-- partition folders exist: MSCK REPAIR TABLE scans the location and
-- registers them. Re-run it after every rebuild.
--
-- Run:  ./run_hive.sh hive/ddl_facts.sql   (paths and database come from conf/olist.conf)
-- =====================================================================
USE ${hivevar:DB};

-- Transaction fact. Grain: one row per item on an order (order_id + order_item_id).
DROP TABLE IF EXISTS fact_order_item;
CREATE EXTERNAL TABLE fact_order_item (
  purchase_date_key        INT,
  shipping_limit_date_key  INT,
  customer_key             INT,
  customer_geo_key         INT,
  seller_key               INT,
  seller_geo_key           INT,
  product_key              INT,
  order_id                 STRING,          -- degenerate dimension
  order_item_id            INT,
  source_customer_id       STRING,
  price                    DECIMAL(10,2),
  freight_value            DECIMAL(10,2),
  item_qty                 INT,
  gross_amount             DECIMAL(10,2)
)
PARTITIONED BY (purchase_year INT)
STORED AS PARQUET
LOCATION '${hivevar:CURATED}/fact_order_item';
MSCK REPAIR TABLE fact_order_item;

-- Accumulating snapshot. Grain: one row per order, one date key per milestone.
DROP TABLE IF EXISTS fact_order_fulfillment;
CREATE EXTERNAL TABLE fact_order_fulfillment (
  purchase_date_key   INT,
  approved_date_key   INT,
  carrier_date_key    INT,
  delivered_date_key  INT,
  estimated_date_key  INT,
  customer_key        INT,
  customer_geo_key    INT,
  order_status_key    INT,
  order_id            STRING,             -- degenerate dimension
  approval_lag_days   DOUBLE,
  carrier_lag_days    DOUBLE,
  delivery_lag_days   DOUBLE,
  total_lead_days     DOUBLE,
  days_late           INT,
  is_delivered        INT,
  is_late             INT,
  date_anomaly_flag   INT,
  item_count          INT,
  order_value         DECIMAL(10,2)
)
STORED AS PARQUET
LOCATION '${hivevar:CURATED}/fact_order_fulfillment';

-- Transaction fact. Grain: one row per payment on an order (order_id + payment_sequential).
DROP TABLE IF EXISTS fact_payment;
CREATE EXTERNAL TABLE fact_payment (
  purchase_date_key   INT,
  customer_key        INT,
  payment_type_key    INT,
  order_id            STRING,             -- degenerate dimension
  payment_sequential  INT,
  payment_value       DECIMAL(10,2),
  installments        INT
)
PARTITIONED BY (purchase_year INT)
STORED AS PARQUET
LOCATION '${hivevar:CURATED}/fact_payment';
MSCK REPAIR TABLE fact_payment;

-- Transaction fact. Grain: one row per order (its latest review).
DROP TABLE IF EXISTS fact_review;
CREATE EXTERNAL TABLE fact_review (
  review_created_date_key  INT,
  review_answer_date_key   INT,
  customer_key             INT,
  order_id                 STRING,        -- degenerate dimension
  review_id                STRING,
  review_score             INT,
  response_hours           DOUBLE,
  has_comment              INT,
  has_title                INT
)
STORED AS PARQUET
LOCATION '${hivevar:CURATED}/fact_review';

SHOW TABLES;
SHOW PARTITIONS fact_order_item;
