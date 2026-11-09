# -*- coding: utf-8 -*-
"""
Step 4 - Build the Kimball dimension tables from one raw load.

Reads  : /data/olist/raw/<table>/load_date=<date>/        (CSV)
Writes : /data/olist/curated/dim_<name>/                   (Parquet, overwrite)

  dim_date          calendar 2016-2018, smart key YYYYMMDD, BR holidays
  dim_geography     one row per ZIP prefix (deduplicated geolocation)
  dim_customer      one person per customer_unique_id, SCD Type 2 on location
  dim_seller        one row per seller (Type 1)
  dim_product       one row per product, category translated (Type 1)
  dim_payment_type  one row per payment type
  dim_order_status  one row per order status

Every dimension gets a surrogate key and a -1 "Unknown" member.
Runs on Spark2 2.0 + Python 2.6 (HDP 2.5).

Run on the sandbox:   ./run_spark.sh spark/build_dims.py 2026-11-03
"""
from __future__ import print_function

from datetime import date, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, DateType, BooleanType)
from pyspark.sql.window import Window

from common import (load_date_arg, get_spark, read_csv, region_df,
                    with_surrogate_key, add_unknown_row, write_parquet,
                    section, line)

LOAD_DATE = load_date_arg()
spark = get_spark("olist_build_dims")
spark.sparkContext.setLogLevel("WARN")

print("Building dimensions from load_date = %s" % LOAD_DATE)
results = []


def save(df, table, extra_checks=None):
    """Write a dimension and record its row count and key uniqueness."""
    df = df.cache()
    write_parquet(df, table)
    key = df.columns[0]
    rows = df.count()
    dup_keys = df.groupBy(key).count().filter("count > 1").count()
    results.append((table, rows, dup_keys))
    if extra_checks:
        for label, value in extra_checks(df):
            results.append(("   " + label, value, None))


# ================================================================ dim_date
def easter(year):
    """Gregorian Easter Sunday (anonymous algorithm)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def brazil_holidays(year):
    hol = {
        date(year, 1, 1): "Confraternizacao Universal",
        date(year, 4, 21): "Tiradentes",
        date(year, 5, 1): "Dia do Trabalho",
        date(year, 9, 7): "Independencia",
        date(year, 10, 12): "Nossa Senhora Aparecida",
        date(year, 11, 2): "Finados",
        date(year, 11, 15): "Proclamacao da Republica",
        date(year, 12, 25): "Natal",
    }
    e = easter(year)
    hol[e - timedelta(days=48)] = "Carnaval (segunda)"
    hol[e - timedelta(days=47)] = "Carnaval (terca)"
    hol[e - timedelta(days=2)] = "Sexta-feira Santa"
    hol[e + timedelta(days=60)] = "Corpus Christi"
    return hol


MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday", "Sunday"]

holidays = {}
for y in (2016, 2017, 2018):
    holidays.update(brazil_holidays(y))

date_rows = []
d = date(2016, 1, 1)
while d <= date(2018, 12, 31):
    date_rows.append((
        d.year * 10000 + d.month * 100 + d.day,     # date_key (smart key)
        d,                                          # full_date
        d.year,
        (d.month - 1) // 3 + 1,                     # quarter
        d.month,
        MONTHS[d.month - 1],
        d.day,
        d.isoweekday(),                             # 1 = Monday
        DAYS[d.weekday()],
        d.isocalendar()[1],                         # ISO week
        d.weekday() >= 5,                           # is_weekend
        d in holidays,                              # is_holiday
        holidays.get(d),
    ))
    d += timedelta(days=1)

date_schema = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("full_date", DateType(), True),
    StructField("year", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("day_of_month", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("week_of_year", IntegerType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("is_holiday", BooleanType(), True),
    StructField("holiday_name", StringType(), True),
])
dim_date = spark.createDataFrame(date_rows, date_schema)
dim_date = add_unknown_row(spark, dim_date, {
    "date_key": -1, "month_name": "Unknown / not yet occurred",
    "day_name": "Unknown / not yet occurred",
    "is_weekend": False, "is_holiday": False})
save(dim_date, "dim_date", lambda df: [
    ("holidays flagged", df.filter("is_holiday").count())])

# ============================================================ dim_geography
ACCENTS_FROM = u"áàâãäéèêëíìîïóòôõöúùûüç"
ACCENTS_TO = u"aaaaaeeeeiiiiooooouuuuc"

geo = read_csv(spark, "geolocation", LOAD_DATE).select(
    F.col("geolocation_zip_code_prefix").alias("zip_prefix"),
    F.col("geolocation_lat").cast("double").alias("lat"),
    F.col("geolocation_lng").cast("double").alias("lng"),
    F.translate(F.lower(F.trim(F.col("geolocation_city"))),
                ACCENTS_FROM, ACCENTS_TO).alias("city"),
    F.upper(F.trim(F.col("geolocation_state"))).alias("state"),
).dropDuplicates()


def most_frequent(df, value_col):
    """Per ZIP prefix, the value that appears most often (ties -> alphabetical)."""
    counts = df.groupBy("zip_prefix", value_col).count()
    w = Window.partitionBy("zip_prefix").orderBy(F.desc("count"), F.asc(value_col))
    return (counts.withColumn("rn", F.row_number().over(w))
                  .filter("rn = 1").select("zip_prefix", value_col))


# Brazil's bounding box: drop obviously wrong coordinates before averaging
coords = (geo.filter((F.col("lat").between(-34.0, 6.0)) &
                     (F.col("lng").between(-74.0, -34.0)))
             .groupBy("zip_prefix")
             .agg(F.round(F.avg("lat"), 6).alias("latitude"),
                  F.round(F.avg("lng"), 6).alias("longitude")))

geography = (geo.select("zip_prefix").distinct()
             .join(most_frequent(geo, "city"), "zip_prefix", "left")
             .join(most_frequent(geo, "state"), "zip_prefix", "left")
             .join(coords, "zip_prefix", "left")
             .join(region_df(spark), "state", "left")
             .select("zip_prefix", "city", "state", "region",
                     "latitude", "longitude"))
dim_geography = with_surrogate_key(geography, "geo_key", ["zip_prefix"])
dim_geography = add_unknown_row(spark, dim_geography, {
    "geo_key": -1, "city": "unknown", "state": "NA", "region": "Unknown"})
save(dim_geography, "dim_geography")

# ============================================================= dim_customer
# One row per customer per location: a new version starts whenever the
# person's ZIP/city/state differs from their previous order (SCD Type 2).
customers = read_csv(spark, "customers", LOAD_DATE)
orders = read_csv(spark, "orders", LOAD_DATE).select(
    "customer_id", "order_purchase_timestamp")

c = (customers.join(orders, "customer_id")
     .select("customer_unique_id",
             F.col("customer_zip_code_prefix").alias("zip_prefix"),
             F.lower(F.trim(F.col("customer_city"))).alias("city"),
             F.upper(F.trim(F.col("customer_state"))).alias("state"),
             F.col("order_purchase_timestamp").cast("timestamp").alias("purchase_ts"),
             "customer_id"))

by_person = Window.partitionBy("customer_unique_id").orderBy("purchase_ts", "customer_id")
c = (c.withColumn("location", F.concat_ws("|", "zip_prefix", "city", "state"))
      .withColumn("prev_location", F.lag("location").over(by_person)))

versions = c.filter(F.col("prev_location").isNull() |
                    (F.col("prev_location") != F.col("location")))

by_version = Window.partitionBy("customer_unique_id").orderBy("purchase_ts")
per_person = Window.partitionBy("customer_unique_id")
versions = (versions
            .withColumn("version", F.row_number().over(by_version))
            .withColumn("effective_from", F.col("purchase_ts"))
            .withColumn("next_from", F.lead("purchase_ts").over(by_version))
            .withColumn("is_current", F.col("next_from").isNull())
            .withColumn("effective_to",
                        F.coalesce(F.col("next_from"),
                                   F.lit("9999-12-31 23:59:59").cast("timestamp")))
            .withColumn("location_change_suspect",
                        F.count(F.lit(1)).over(per_person) > 1)
            .join(region_df(spark), "state", "left")
            .select("customer_unique_id", "zip_prefix", "city", "state", "region",
                    "version", "effective_from", "effective_to", "is_current",
                    "location_change_suspect"))

dim_customer = with_surrogate_key(versions, "customer_key",
                                  ["customer_unique_id", "effective_from"])
dim_customer = add_unknown_row(spark, dim_customer, {
    "customer_key": -1, "customer_unique_id": "unknown", "city": "unknown",
    "state": "NA", "region": "Unknown", "version": 1, "is_current": True,
    "location_change_suspect": False})
save(dim_customer, "dim_customer", lambda df: [
    ("current rows (one per person, incl. Unknown)", df.filter("is_current").count()),
    ("people with more than one version",
     df.filter("location_change_suspect").select("customer_unique_id").distinct().count())])

# =============================================================== dim_seller
sellers = (read_csv(spark, "sellers", LOAD_DATE)
           .select("seller_id",
                   F.col("seller_zip_code_prefix").alias("zip_prefix"),
                   F.lower(F.trim(F.col("seller_city"))).alias("city"),
                   F.upper(F.trim(F.col("seller_state"))).alias("state"))
           .join(region_df(spark), "state", "left")
           .select("seller_id", "zip_prefix", "city", "state", "region"))
dim_seller = with_surrogate_key(sellers, "seller_key", ["seller_id"])
dim_seller = add_unknown_row(spark, dim_seller, {
    "seller_key": -1, "seller_id": "unknown", "city": "unknown",
    "state": "NA", "region": "Unknown"})
save(dim_seller, "dim_seller")

# ============================================================== dim_product
# Typos in Kaggle's translation file, corrected here (design doc section 5)
TRANSLATION_FIXES = {
    "costruction_tools_garden": "construction_tools_garden",
    "costruction_tools_tools": "construction_tools_tools",
    "fashio_female_clothing": "fashion_female_clothing",
    "home_confort": "home_comfort",
}

translation = read_csv(spark, "category_translation", LOAD_DATE)
fixed_en = F.col("product_category_name_english")
for wrong, right in TRANSLATION_FIXES.items():
    fixed_en = F.when(F.col("product_category_name_english") == wrong, right) \
                .otherwise(fixed_en)
translation = translation.select(
    F.col("product_category_name").alias("category_pt"),
    fixed_en.alias("category_en_fixed"))

weight = F.col("weight_g")
products = (read_csv(spark, "products", LOAD_DATE)
            .select("product_id",
                    F.coalesce(F.col("product_category_name"), F.lit("unknown")).alias("category_pt"),
                    F.col("product_name_lenght").cast("int").alias("name_length"),
                    F.col("product_description_lenght").cast("int").alias("description_length"),
                    F.col("product_photos_qty").cast("int").alias("photos_qty"),
                    F.col("product_weight_g").cast("int").alias("weight_g"),
                    F.col("product_length_cm").cast("int").alias("length_cm"),
                    F.col("product_height_cm").cast("int").alias("height_cm"),
                    F.col("product_width_cm").cast("int").alias("width_cm"))
            .join(translation, "category_pt", "left")
            .withColumn("category_en",
                        F.coalesce(F.col("category_en_fixed"), F.col("category_pt")))
            .withColumn("volume_cm3",
                        (F.col("length_cm") * F.col("height_cm") * F.col("width_cm")).cast("long"))
            .withColumn("weight_band",
                        F.when(weight.isNull(), "unknown")
                         .when(weight < 500, "under 500 g")
                         .when(weight < 2000, "500 g - 2 kg")
                         .when(weight < 10000, "2 - 10 kg")
                         .otherwise("over 10 kg"))
            .select("product_id", "category_pt", "category_en", "name_length",
                    "description_length", "photos_qty", "weight_g", "length_cm",
                    "height_cm", "width_cm", "volume_cm3", "weight_band"))
dim_product = with_surrogate_key(products, "product_key", ["product_id"])
dim_product = add_unknown_row(spark, dim_product, {
    "product_key": -1, "product_id": "unknown", "category_pt": "unknown",
    "category_en": "unknown", "weight_band": "unknown"})
untranslated = (products.select("category_pt").distinct()
                .filter("category_pt <> 'unknown'")
                .join(translation, "category_pt", "left_anti").count())
save(dim_product, "dim_product", lambda df: [
    ("products in category 'unknown'", df.filter("category_pt = 'unknown'").count() - 1),
    ("categories with no English translation (kept in PT)", untranslated)])

# ========================================================= dim_payment_type
PAYMENT_DESCRIPTIONS = {
    "credit_card": "Credit card",
    "debit_card": "Debit card",
    "boleto": "Boleto bancario (bank slip)",
    "voucher": "Voucher",
    "not_defined": "Not defined in source",
}
desc = F.lit(None).cast("string")
for code, text in PAYMENT_DESCRIPTIONS.items():
    desc = F.when(F.col("payment_type") == code, text).otherwise(desc)

payment_types = (read_csv(spark, "order_payments", LOAD_DATE)
                 .select("payment_type").distinct()
                 .filter(F.col("payment_type").isNotNull())
                 .withColumn("description", desc)
                 .withColumn("is_card", F.col("payment_type").isin("credit_card", "debit_card")))
dim_payment_type = with_surrogate_key(payment_types, "payment_type_key", ["payment_type"])
dim_payment_type = add_unknown_row(spark, dim_payment_type, {
    "payment_type_key": -1, "payment_type": "unknown",
    "description": "Unknown", "is_card": False})
save(dim_payment_type, "dim_payment_type")

# ========================================================= dim_order_status
statuses = (read_csv(spark, "orders", LOAD_DATE)
            .select("order_status").distinct()
            .filter(F.col("order_status").isNotNull())
            .withColumn("is_terminal",
                        F.col("order_status").isin("delivered", "canceled", "unavailable"))
            .withColumn("is_successful", F.col("order_status") == "delivered"))
dim_order_status = with_surrogate_key(statuses, "order_status_key", ["order_status"])
dim_order_status = add_unknown_row(spark, dim_order_status, {
    "order_status_key": -1, "order_status": "unknown",
    "is_terminal": False, "is_successful": False})
save(dim_order_status, "dim_order_status")

# ================================================================== report
section("DIMENSIONS WRITTEN TO /data/olist/curated (incl. -1 Unknown row)")
for name, rows, dups in results:
    if dups is None:
        line(name, rows)
    else:
        line("%s  (duplicate keys: %s)" % (name, dups), rows)
print("")
print("Dimension build finished.")
spark.stop()
