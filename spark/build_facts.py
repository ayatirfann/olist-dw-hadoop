# -*- coding: utf-8 -*-
"""
Step 5 - Build the Kimball fact tables from one raw load.

Reads  : /data/olist/raw/<table>/load_date=<date>/   (CSV)
         /data/olist/curated/dim_*/                   (Parquet, from build_dims.py)
Writes : /data/olist/curated/fact_*/                  (Parquet, overwrite)

  fact_order_item         transaction fact   - one row per item on an order
  fact_order_fulfillment  accumulating snap. - one row per order, 5 milestones
  fact_payment            transaction fact   - one row per payment on an order
  fact_review             transaction fact   - one row per order (latest review)

Natural keys from the source are swapped for the dimensions' surrogate keys
(the "surrogate key pipeline"). A reference with no matching dimension row
gets key -1 (the Unknown member), so every fact row joins to every dimension.
Runs on Spark2 2.0 + Python 2.6 (HDP 2.5).

Run on the sandbox:   ./run_spark.sh spark/build_facts.py 2026-11-03
"""
from __future__ import print_function

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (CURATED, load_date_arg, get_spark, read_csv, read_reviews,
                    write_parquet, section, line)

LOAD_DATE = load_date_arg()
spark = get_spark("olist_build_facts")
spark.sparkContext.setLogLevel("WARN")
print("Building facts from load_date = %s" % LOAD_DATE)

MONEY = "decimal(10,2)"


def dim(name):
    return spark.read.parquet("%s/%s" % (CURATED, name))


# ------------------------------------------------------------ dimensions
dim_date_keys = dim("dim_date").select("date_key").filter("date_key > 0").cache()
dim_customer = dim("dim_customer").filter("customer_key > 0").select(
    "customer_key", "customer_unique_id", "effective_from", "effective_to")
dim_geo = F.broadcast(dim("dim_geography").filter("geo_key > 0")
                      .select("geo_key", "zip_prefix"))
dim_seller = F.broadcast(dim("dim_seller").filter("seller_key > 0")
                         .select("seller_key", "seller_id"))
dim_product = dim("dim_product").filter("product_key > 0").select(
    "product_key", "product_id")
dim_status = F.broadcast(dim("dim_order_status").filter("order_status_key > 0")
                         .select("order_status_key", "order_status"))
dim_payment = F.broadcast(dim("dim_payment_type").filter("payment_type_key > 0")
                          .select("payment_type_key", "payment_type"))


# ------------------------------------------------------------ key helpers
def with_date_key(df, ts_col, key_col):
    """YYYYMMDD smart key for a timestamp column. A missing timestamp, or a
    date outside dim_date's calendar, gets -1 ("Unknown / not yet occurred")."""
    raw = "_raw_" + key_col
    df = df.withColumn(raw, F.date_format(F.col(ts_col), "yyyyMMdd").cast("int"))
    keys = F.broadcast(dim_date_keys.withColumnRenamed("date_key", raw)
                       .withColumn("_ok_" + key_col, F.lit(True)))
    df = df.join(keys, raw, "left")
    return (df.withColumn(key_col, F.when(F.col("_ok_" + key_col), F.col(raw))
                          .otherwise(F.lit(-1)))
              .drop(raw).drop("_ok_" + key_col))


def with_lookup(df, dim_df, on_col, dim_on_col, key_col):
    """Equi-join lookup of a surrogate key; no match -> -1."""
    d = dim_df.withColumnRenamed(dim_on_col, on_col)
    df = df.join(d, on_col, "left")
    return df.withColumn(key_col, F.coalesce(F.col(key_col), F.lit(-1)))


def with_geo_key(df, zip_col, key_col):
    d = dim_geo.withColumnRenamed("geo_key", key_col).withColumnRenamed("zip_prefix", zip_col)
    df = df.join(d, zip_col, "left")
    return df.withColumn(key_col, F.coalesce(F.col(key_col), F.lit(-1)))


def days_between(later, earlier):
    """Elapsed days (2 decimals) between two timestamps; null if either is missing."""
    return F.round((F.unix_timestamp(F.col(later)) - F.unix_timestamp(F.col(earlier)))
                   / 86400.0, 2)


# ------------------------------------------------------------ source data
orders = read_csv(spark, "orders", LOAD_DATE).select(
    "order_id", "customer_id", "order_status",
    F.col("order_purchase_timestamp").cast("timestamp").alias("purchase_ts"),
    F.col("order_approved_at").cast("timestamp").alias("approved_ts"),
    F.col("order_delivered_carrier_date").cast("timestamp").alias("carrier_ts"),
    F.col("order_delivered_customer_date").cast("timestamp").alias("delivered_ts"),
    F.col("order_estimated_delivery_date").cast("timestamp").alias("estimated_ts"))

customers = read_csv(spark, "customers", LOAD_DATE).select(
    "customer_id", "customer_unique_id",
    F.col("customer_zip_code_prefix").alias("customer_zip"))

# Every order, with the surrogate keys that depend only on the order:
# customer (SCD Type 2 version valid at purchase time), customer geography,
# order status and purchase date.
o = orders.join(customers, "customer_id", "left")
o = o.join(dim_customer,
           (o.customer_unique_id == dim_customer.customer_unique_id) &
           (o.purchase_ts >= dim_customer.effective_from) &
           (o.purchase_ts < dim_customer.effective_to),
           "left").drop(dim_customer.customer_unique_id) \
     .drop("effective_from").drop("effective_to")
o = o.withColumn("customer_key", F.coalesce(F.col("customer_key"), F.lit(-1)))
o = with_geo_key(o, "customer_zip", "customer_geo_key")
o = with_lookup(o, dim_status, "order_status", "order_status", "order_status_key")
o = with_date_key(o, "purchase_ts", "purchase_date_key")
o = o.withColumn("purchase_year", F.year("purchase_ts")).cache()

items = read_csv(spark, "order_items", LOAD_DATE).select(
    "order_id",
    F.col("order_item_id").cast("int").alias("order_item_id"),
    "product_id", "seller_id",
    F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_ts"),
    F.col("price").cast(MONEY).alias("price"),
    F.col("freight_value").cast(MONEY).alias("freight_value"))

sellers = read_csv(spark, "sellers", LOAD_DATE).select(
    "seller_id", F.col("seller_zip_code_prefix").alias("seller_zip"))

results = []


def save(df, table, key_cols, minus_one_cols, partition_col=None):
    df = df.cache()
    path = "%s/%s" % (CURATED, table)
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    rows = df.count()
    dups = df.groupBy(*key_cols).count().filter("count > 1").count()
    results.append((table, "rows", rows))
    results.append((table, "duplicate grain keys", dups))
    for c in minus_one_cols:
        results.append((table, "rows with %s = -1" % c, df.filter(F.col(c) == -1).count()))
    return df


# ======================================================== fact_order_item
fi = (items
      .join(o.select("order_id", "customer_id", "customer_key", "customer_geo_key",
                     "purchase_date_key", "purchase_year"), "order_id", "left")
      .join(sellers, "seller_id", "left"))
fi = with_lookup(fi, dim_product, "product_id", "product_id", "product_key")
fi = with_lookup(fi, dim_seller, "seller_id", "seller_id", "seller_key")
fi = with_geo_key(fi, "seller_zip", "seller_geo_key")
fi = with_date_key(fi, "shipping_limit_ts", "shipping_limit_date_key")
fi = fi.select(
    F.coalesce("purchase_date_key", F.lit(-1)).alias("purchase_date_key"),
    "shipping_limit_date_key",
    F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),
    F.coalesce("customer_geo_key", F.lit(-1)).alias("customer_geo_key"),
    "seller_key", "seller_geo_key", "product_key",
    "order_id", "order_item_id",
    F.col("customer_id").alias("source_customer_id"),
    "price", "freight_value",
    F.lit(1).alias("item_qty"),
    (F.col("price") + F.col("freight_value")).cast(MONEY).alias("gross_amount"),
    "purchase_year")
fact_order_item = save(fi, "fact_order_item", ["order_id", "order_item_id"],
                       ["customer_key", "customer_geo_key", "seller_key",
                        "seller_geo_key", "product_key", "shipping_limit_date_key"],
                       "purchase_year")

# ================================================= fact_order_fulfillment
per_order = (items.groupBy("order_id")
             .agg(F.count(F.lit(1)).alias("item_count"),
                  F.sum(F.col("price") + F.col("freight_value")).alias("order_value")))

ff = o.join(per_order, "order_id", "left")
for ts, key in [("approved_ts", "approved_date_key"),
                ("carrier_ts", "carrier_date_key"),
                ("delivered_ts", "delivered_date_key"),
                ("estimated_ts", "estimated_date_key")]:
    ff = with_date_key(ff, ts, key)

delivered = F.col("delivered_ts").isNotNull()
days_late = F.datediff(F.to_date("delivered_ts"), F.to_date("estimated_ts"))
ff = ff.select(
    "purchase_date_key", "approved_date_key", "carrier_date_key",
    "delivered_date_key", "estimated_date_key",
    "customer_key", "customer_geo_key", "order_status_key",
    "order_id",
    days_between("approved_ts", "purchase_ts").alias("approval_lag_days"),
    days_between("carrier_ts", "approved_ts").alias("carrier_lag_days"),
    days_between("delivered_ts", "carrier_ts").alias("delivery_lag_days"),
    days_between("delivered_ts", "purchase_ts").alias("total_lead_days"),
    F.when(delivered, days_late).alias("days_late"),
    F.when(delivered, 1).otherwise(0).alias("is_delivered"),
    F.when(delivered & (days_late > 0), 1).otherwise(0).alias("is_late"),
    F.when((F.col("carrier_ts") < F.col("approved_ts")) |
           (F.col("delivered_ts") < F.col("carrier_ts")), 1).otherwise(0)
     .alias("date_anomaly_flag"),
    F.coalesce(F.col("item_count"), F.lit(0)).cast("int").alias("item_count"),
    F.coalesce(F.col("order_value"), F.lit(0)).cast(MONEY).alias("order_value"))
fact_fulfillment = save(ff, "fact_order_fulfillment", ["order_id"],
                        ["customer_key", "customer_geo_key", "approved_date_key",
                         "carrier_date_key", "delivered_date_key"])

# =========================================================== fact_payment
payments = read_csv(spark, "order_payments", LOAD_DATE).select(
    "order_id",
    F.col("payment_sequential").cast("int").alias("payment_sequential"),
    "payment_type",
    F.col("payment_installments").cast("int").alias("installments"),
    F.col("payment_value").cast(MONEY).alias("payment_value"))
fp = payments.join(o.select("order_id", "customer_key", "purchase_date_key",
                            "purchase_year"), "order_id", "left")
fp = with_lookup(fp, dim_payment, "payment_type", "payment_type", "payment_type_key")
fp = fp.select(
    F.coalesce("purchase_date_key", F.lit(-1)).alias("purchase_date_key"),
    F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),
    "payment_type_key", "order_id", "payment_sequential",
    "payment_value", "installments", "purchase_year")
fact_payment = save(fp, "fact_payment", ["order_id", "payment_sequential"],
                    ["customer_key", "payment_type_key"], "purchase_year")

# ============================================================ fact_review
# Grain rule: one row per order - its most recent review (some orders were
# reviewed more than once).
reviews = read_reviews(spark, LOAD_DATE).select(
    "review_id", "order_id",
    F.col("review_score").cast("int").alias("review_score"),
    F.col("review_comment_title").alias("title"),
    F.col("review_comment_message").alias("message"),
    F.col("review_creation_date").cast("timestamp").alias("created_ts"),
    F.col("review_answer_timestamp").cast("timestamp").alias("answered_ts"))
latest = Window.partitionBy("order_id").orderBy(
    F.desc("created_ts"), F.desc("answered_ts"), F.desc("review_id"))
reviews = (reviews.withColumn("rn", F.row_number().over(latest))
                  .filter("rn = 1").drop("rn"))

fr = reviews.join(o.select("order_id", "customer_key"), "order_id", "left")
fr = with_date_key(fr, "created_ts", "review_created_date_key")
fr = with_date_key(fr, "answered_ts", "review_answer_date_key")
fr = fr.select(
    "review_created_date_key", "review_answer_date_key",
    F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),
    "order_id", "review_id", "review_score",
    F.round((F.unix_timestamp("answered_ts") - F.unix_timestamp("created_ts"))
            / 3600.0, 1).alias("response_hours"),
    F.when(F.col("message").isNotNull(), 1).otherwise(0).alias("has_comment"),
    F.when(F.col("title").isNotNull(), 1).otherwise(0).alias("has_title"))
fact_review = save(fr, "fact_review", ["order_id"],
                   ["customer_key", "review_created_date_key"])

# ================================================================ checks
raw_items = items.agg(F.sum("price").alias("p"), F.sum("freight_value").alias("f")).first()
fact_items = fact_order_item.agg(F.sum("price").alias("p"),
                                 F.sum("freight_value").alias("f")).first()
raw_pay = payments.agg(F.sum("payment_value").alias("v")).first()
fact_pay = fact_payment.agg(F.sum("payment_value").alias("v")).first()

section("FACT TABLES WRITTEN TO %s" % CURATED)
current = None
for table, label, value in results:
    if table != current:
        print("")
        print("  %s" % table)
        current = table
    line("   " + label, value)

section("RECONCILIATION (fact totals must equal raw totals)")
line("price          raw / fact", "%s / %s" % (raw_items["p"], fact_items["p"]))
line("freight_value  raw / fact", "%s / %s" % (raw_items["f"], fact_items["f"]))
line("payment_value  raw / fact", "%s / %s" % (raw_pay["v"], fact_pay["v"]))

section("FULFILLMENT SNAPSHOT")
line("orders with no items (item_count = 0)",
     fact_fulfillment.filter("item_count = 0").count())
line("delivered orders", fact_fulfillment.filter("is_delivered = 1").count())
line("delivered late", fact_fulfillment.filter("is_late = 1").count())
line("orders with date anomalies (flagged, not fixed)",
     fact_fulfillment.filter("date_anomaly_flag = 1").count())
avg = fact_fulfillment.filter("is_delivered = 1").agg(
    F.round(F.avg("total_lead_days"), 1).alias("d")).first()
line("average days from purchase to delivery", avg["d"])

# Quality gate: every fact must keep its grain and reconcile to the raw
# totals. A non-zero exit fails the Oozie step and stops the pipeline.
failed = ["%s: %s duplicate grain keys" % (t, v)
          for t, label, v in results if label == "duplicate grain keys" and v]
for name, raw_v, fact_v in [("price", raw_items["p"], fact_items["p"]),
                            ("freight_value", raw_items["f"], fact_items["f"]),
                            ("payment_value", raw_pay["v"], fact_pay["v"])]:
    if raw_v != fact_v:
        failed.append("%s does not reconcile (raw %s, fact %s)" % (name, raw_v, fact_v))
print("")
spark.stop()
if failed:
    print("QUALITY GATE FAILED - " + "; ".join(failed))
    sys.exit(1)
print("Quality gate passed. Fact build finished.")
