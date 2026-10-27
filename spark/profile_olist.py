# -*- coding: utf-8 -*-
"""
Step 3 - Profile the raw Olist CSVs in HDFS.

Runs on the HDP 2.5 sandbox: Spark2 2.0 + Python 2.6 (Ambari-managed versions).
Re-checks the grain and data-quality findings from the design document
(section 5) before any dimension or fact table is built.

Run (on YARN), passing the load date to profile:
  export SPARK_MAJOR_VERSION=2
  spark-submit --master yarn --deploy-mode client \
      --driver-memory 768m --executor-memory 768m --num-executors 1 \
      spark/profile_olist.py 2026-10-27 2> profile_errors.log | tee profile_report.txt
"""
from __future__ import print_function

import csv
import sys
from datetime import date
from StringIO import StringIO

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

RAW = "/data/olist/raw"
# One load date at a time: each delivery is a full snapshot, so reading
# every load_date folder at once would double-count rows.
LOAD_DATE = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()

spark = SparkSession.builder.appName("olist_profile").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------- readers
def read_csv(name):
    """Read one raw table. Every column stays a string, so ZIP prefixes
    keep their leading zeros (design doc: never infer schema)."""
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("quote", '"')
            .option("escape", '"')
            .csv("%s/%s/load_date=%s/*.csv" % (RAW, name, LOAD_DATE)))


def read_reviews():
    """Spark 2.0's CSV reader has no multiLine option, and 3,852 review
    comments contain line breaks. Read each file whole, parse it with
    Python's csv module (which understands quoted line breaks), and
    flatten the breaks to spaces."""
    path = "%s/order_reviews/load_date=%s/*.csv" % (RAW, LOAD_DATE)

    def parse(kv):
        return list(csv.reader(StringIO(kv[1].encode("utf-8"))))

    rows = spark.sparkContext.wholeTextFiles(path).flatMap(parse)
    header = rows.first()
    width = len(header)

    def clean(r):
        return [c.decode("utf-8").replace("\r", " ").replace("\n", " ")
                for c in r]

    data = (rows.filter(lambda r: r != header and len(r) == width)
                .map(clean))
    schema = StructType([StructField(h, StringType(), True) for h in header])
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------- helpers
def line(label, value):
    print("  %-58s %s" % (label, value))


def section(title):
    print("")
    print("=" * 72)
    print(title)
    print("=" * 72)


def dupes(df, cols):
    """Number of key values that appear more than once."""
    return df.groupBy(*cols).count().filter("count > 1").count()


# ---------------------------------------------------------------- load
orders = read_csv("orders").cache()
items = read_csv("order_items").cache()
payments = read_csv("order_payments").cache()
customers = read_csv("customers").cache()
products = read_csv("products").cache()
sellers = read_csv("sellers").cache()
geo = read_csv("geolocation").cache()
cat = read_csv("category_translation").cache()
reviews = read_reviews().cache()

# ---------------------------------------------------------------- 1. counts
print("Profiling load_date = %s" % LOAD_DATE)
section("1. ROW COUNTS")
for name, df in [("orders", orders), ("order_items", items),
                 ("order_payments", payments), ("order_reviews", reviews),
                 ("customers", customers), ("products", products),
                 ("sellers", sellers), ("geolocation", geo),
                 ("category_translation", cat)]:
    line(name, df.count())

# ---------------------------------------------------------------- 2. grain
section("2. GRAIN CHECKS (0 duplicate keys = grain proven)")
line("orders: duplicate order_id", dupes(orders, ["order_id"]))
line("order_items: duplicate order_id + order_item_id",
     dupes(items, ["order_id", "order_item_id"]))
line("order_payments: duplicate order_id + payment_sequential",
     dupes(payments, ["order_id", "payment_sequential"]))
line("customers: duplicate customer_id", dupes(customers, ["customer_id"]))
line("products: duplicate product_id", dupes(products, ["product_id"]))
line("sellers: duplicate seller_id", dupes(sellers, ["seller_id"]))
line("reviews: duplicate review_id", dupes(reviews, ["review_id"]))

# ---------------------------------------------------------------- 3. orders
section("3. ORDERS")
rng = orders.agg(F.min("order_purchase_timestamp").alias("lo"),
                 F.max("order_purchase_timestamp").alias("hi")).first()
line("purchase date range", "%s  to  %s" % (rng["lo"], rng["hi"]))
for r in orders.groupBy("order_status").count().orderBy(F.desc("count")).collect():
    line("status = %s" % r["order_status"], r["count"])

no_items = orders.join(items.select("order_id").distinct(), "order_id", "left_anti")
line("orders with no line items", no_items.count())
line("orders where carrier pickup is before approval",
     orders.filter("order_delivered_carrier_date < order_approved_at").count())

multi_seller = (items.groupBy("order_id")
                .agg(F.countDistinct("seller_id").alias("n"))
                .filter("n > 1").count())
line("orders with more than one seller", multi_seller)

# ---------------------------------------------------------------- 4. customers
section("4. CUSTOMERS (natural key + SCD Type 2 candidates)")
line("distinct customer_id (one per order)",
     customers.select("customer_id").distinct().count())
line("distinct customer_unique_id (one per person)",
     customers.select("customer_unique_id").distinct().count())
line("people seen under more than one ZIP prefix",
     customers.groupBy("customer_unique_id")
     .agg(F.countDistinct("customer_zip_code_prefix").alias("n"))
     .filter("n > 1").count())
line("ZIP prefixes not exactly 5 characters",
     customers.filter(F.length("customer_zip_code_prefix") != 5).count())

# ---------------------------------------------------------------- 5. geography
section("5. GEOLOCATION (input to dim_geography)")
geo_rows = geo.count()
line("exact duplicate rows", geo_rows - geo.distinct().count())
line("distinct ZIP prefixes",
     geo.select("geolocation_zip_code_prefix").distinct().count())
line("ZIP prefixes with more than one city spelling",
     geo.groupBy("geolocation_zip_code_prefix")
     .agg(F.countDistinct("geolocation_city").alias("n"))
     .filter("n > 1").count())
geo_zips = geo.select(F.col("geolocation_zip_code_prefix").alias("zip")).distinct()
line("customer rows whose ZIP has no geolocation match",
     customers.join(geo_zips,
                    customers.customer_zip_code_prefix == geo_zips.zip,
                    "left_anti").count())
line("seller rows whose ZIP has no geolocation match",
     sellers.join(geo_zips,
                  sellers.seller_zip_code_prefix == geo_zips.zip,
                  "left_anti").count())

# ---------------------------------------------------------------- 6. products
section("6. PRODUCTS")
line("products with no category",
     products.filter(F.col("product_category_name").isNull()).count())
missing_cat = (products.select("product_category_name").distinct()
               .filter(F.col("product_category_name").isNotNull())
               .join(cat, "product_category_name", "left_anti"))
line("categories missing from translation file", missing_cat.count())
for r in missing_cat.collect():
    line("   missing", r["product_category_name"])
line("products sold at more than one price",
     items.groupBy("product_id").agg(F.countDistinct("price").alias("n"))
     .filter("n > 1").count())

# ---------------------------------------------------------------- 7. payments
section("7. PAYMENTS")
for r in payments.groupBy("payment_type").count().orderBy(F.desc("count")).collect():
    line("type = %s" % r["payment_type"], r["count"])
line("payments with value <= 0",
     payments.filter(F.col("payment_value").cast("double") <= 0).count())

# ---------------------------------------------------------------- 8. reviews
section("8. REVIEWS")
line("orders with more than one review", dupes(reviews, ["order_id"]))
line("orders with no review",
     orders.join(reviews.select("order_id").distinct(), "order_id",
                 "left_anti").count())
line("reviews with comment text",
     reviews.filter(F.length(F.trim(F.col("review_comment_message"))) > 0).count())

print("")
print("Profiling finished.")
spark.stop()
