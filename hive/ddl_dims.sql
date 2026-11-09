-- =====================================================================
-- Hive external tables over the dimension Parquet files written by
-- spark/build_dims.py. Hive stores only the definitions; the data stays in
-- /data/olist/curated. Re-runnable: DROP of an EXTERNAL table never deletes
-- the files.
--
-- Run:  beeline -u jdbc:hive2://localhost:10000 -n hive -f hive/ddl_dims.sql
-- =====================================================================
USE olist_dw;

DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date (
  date_key      INT,
  full_date     DATE,
  year          INT,
  quarter       INT,
  month         INT,
  month_name    STRING,
  day_of_month  INT,
  day_of_week   INT,
  day_name      STRING,
  week_of_year  INT,
  is_weekend    BOOLEAN,
  is_holiday    BOOLEAN,
  holiday_name  STRING
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_date';

DROP TABLE IF EXISTS dim_geography;
CREATE EXTERNAL TABLE dim_geography (
  geo_key     INT,
  zip_prefix  STRING,
  city        STRING,
  state       STRING,
  region      STRING,
  latitude    DOUBLE,
  longitude   DOUBLE
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_geography';

DROP TABLE IF EXISTS dim_customer;
CREATE EXTERNAL TABLE dim_customer (
  customer_key             INT,
  customer_unique_id       STRING,
  zip_prefix               STRING,
  city                     STRING,
  state                    STRING,
  region                   STRING,
  version                  INT,
  effective_from           TIMESTAMP,
  effective_to             TIMESTAMP,
  is_current               BOOLEAN,
  location_change_suspect  BOOLEAN
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_customer';

DROP TABLE IF EXISTS dim_seller;
CREATE EXTERNAL TABLE dim_seller (
  seller_key  INT,
  seller_id   STRING,
  zip_prefix  STRING,
  city        STRING,
  state       STRING,
  region      STRING
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_seller';

DROP TABLE IF EXISTS dim_product;
CREATE EXTERNAL TABLE dim_product (
  product_key         INT,
  product_id          STRING,
  category_pt         STRING,
  category_en         STRING,
  name_length         INT,
  description_length  INT,
  photos_qty          INT,
  weight_g            INT,
  length_cm           INT,
  height_cm           INT,
  width_cm            INT,
  volume_cm3          BIGINT,
  weight_band         STRING
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_product';

DROP TABLE IF EXISTS dim_payment_type;
CREATE EXTERNAL TABLE dim_payment_type (
  payment_type_key  INT,
  payment_type      STRING,
  description       STRING,
  is_card           BOOLEAN
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_payment_type';

DROP TABLE IF EXISTS dim_order_status;
CREATE EXTERNAL TABLE dim_order_status (
  order_status_key  INT,
  order_status      STRING,
  is_terminal       BOOLEAN,
  is_successful     BOOLEAN
)
STORED AS PARQUET
LOCATION '/data/olist/curated/dim_order_status';

SHOW TABLES;
