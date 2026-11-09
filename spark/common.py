# -*- coding: utf-8 -*-
"""
Shared helpers for every olist Spark job (Spark2 2.0 + Python 2.6 on HDP 2.5).

Every job reads the raw zone the same way through this module, so fixes such
as "empty CSV field -> null" are applied once and everywhere.

Shipped to the YARN executors with  --py-files spark/common.py
(run_spark.sh does this automatically).
"""
from __future__ import print_function

import csv
import os
import sys
from datetime import date

try:                                   # Python 2 (sandbox)
    from StringIO import StringIO
    PY2 = True
except ImportError:                    # Python 3 (local testing only)
    from io import StringIO
    PY2 = False

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

# ------------------------------------------------------------------ paths
HDFS_BASE = os.environ.get("OLIST_HDFS_BASE", "/data/olist")
RAW = HDFS_BASE + "/raw"
CURATED = HDFS_BASE + "/curated"

# Brazilian states -> the five official macro-regions
STATE_REGION = [
    ("AC", "Norte"), ("AM", "Norte"), ("AP", "Norte"), ("PA", "Norte"),
    ("RO", "Norte"), ("RR", "Norte"), ("TO", "Norte"),
    ("AL", "Nordeste"), ("BA", "Nordeste"), ("CE", "Nordeste"),
    ("MA", "Nordeste"), ("PB", "Nordeste"), ("PE", "Nordeste"),
    ("PI", "Nordeste"), ("RN", "Nordeste"), ("SE", "Nordeste"),
    ("DF", "Centro-Oeste"), ("GO", "Centro-Oeste"), ("MS", "Centro-Oeste"),
    ("MT", "Centro-Oeste"),
    ("ES", "Sudeste"), ("MG", "Sudeste"), ("RJ", "Sudeste"), ("SP", "Sudeste"),
    ("PR", "Sul"), ("RS", "Sul"), ("SC", "Sul"),
]


# ------------------------------------------------------------------ session
def load_date_arg():
    """The load date to process: first command-line argument, else today."""
    return sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()


def get_spark(app_name):
    return (SparkSession.builder
            .appName(app_name)
            # Hive 1.2 can only read DECIMALs written in the legacy layout
            .config("spark.sql.parquet.writeLegacyFormat", "true")
            # small data on one node: 200 shuffle partitions is wasteful
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


# ------------------------------------------------------------------ readers
def blanks_to_null(df):
    """Spark 2.0's CSV reader returns empty fields as "" instead of null.
    Convert them so null checks and comparisons behave correctly."""
    return df.select([F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c)
                      for c in df.columns])


def read_csv(spark, name, load_date):
    """One raw table for one load date. Every column stays a string, so ZIP
    prefixes keep their leading zeros (never infer schema)."""
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .option("quote", '"')
          .option("escape", '"')
          .csv("%s/%s/load_date=%s/*.csv" % (RAW, name, load_date)))
    return blanks_to_null(df)


def _parse_csv_text(kv):
    text = kv[1].encode("utf-8") if PY2 else kv[1]
    return list(csv.reader(StringIO(text)))


def _clean_review_row(r):
    out = []
    for c in r:
        v = c.decode("utf-8") if PY2 else c
        v = v.replace("\r", " ").replace("\n", " ")
        out.append(v if v.strip() != "" else None)
    return out


def read_reviews(spark, load_date):
    """Spark 2.0's CSV reader has no multiLine option and 3,852 review
    comments contain line breaks: read the file whole and parse it with
    Python's csv module, which understands quoted line breaks."""
    path = "%s/order_reviews/load_date=%s/*.csv" % (RAW, load_date)
    rows = spark.sparkContext.wholeTextFiles(path).flatMap(_parse_csv_text)
    header = rows.first()
    width = len(header)
    data = (rows.filter(lambda r: r != header and len(r) == width)
                .map(_clean_review_row))
    names = [h.decode("utf-8") if PY2 else h for h in header]
    schema = StructType([StructField(str(h), StringType(), True) for h in names])
    return spark.createDataFrame(data, schema)


# ------------------------------------------------------------------ building
def region_df(spark):
    return spark.createDataFrame(STATE_REGION, ["state", "region"])


def with_surrogate_key(df, key_col, order_cols):
    """Meaningless integer key 1..n, assigned in a fixed order so reruns on
    the same data give the same keys. Key becomes the first column."""
    w = Window.orderBy(*order_cols)
    keyed = df.withColumn(key_col, F.row_number().over(w))
    return keyed.select([key_col] + [c for c in df.columns])


def add_unknown_row(spark, df, values):
    """Prepend the Kimball 'Unknown' member (key -1). Columns not given in
    `values` are null; types are copied from the dimension itself."""
    types = dict((f.name, f.dataType) for f in df.schema.fields)
    row = spark.range(1).select(
        [F.lit(values.get(c)).cast(types[c]).alias(c) for c in df.columns])
    return row.union(df)


def write_parquet(df, table):
    """Overwrite the table's folder in the curated zone (idempotent)."""
    path = "%s/%s" % (CURATED, table)
    df.write.mode("overwrite").parquet(path)
    return path


# ------------------------------------------------------------------ report
def section(title):
    print("")
    print("=" * 72)
    print(title)
    print("=" * 72)


def line(label, value):
    print("  %-58s %s" % (label, value))
