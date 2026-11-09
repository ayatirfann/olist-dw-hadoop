#!/usr/bin/env bash
# =====================================================================
# RUN SPARK JOB  (runs on the sandbox)
# Submits one PySpark job to YARN with the sandbox-sized memory settings
# from conf/olist.conf, ships spark/common.py to the executors, and keeps
# a report file and an error log per job in /opt/olist/logs/.
#
# Usage:  ./run_spark.sh spark/<job>.py <load_date>
#   e.g.  ./run_spark.sh spark/build_dims.py 2026-11-03
# From the Mac:
#   ssh olist-sandbox "/opt/olist/app/run_spark.sh spark/build_dims.py 2026-11-03"
# =====================================================================
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$APP_DIR/conf/olist.conf"
export SPARK_MAJOR_VERSION=2
export LC_ALL=en_US.UTF-8

JOB="$1"; shift
NAME="$(basename "$JOB" .py)"
mkdir -p "$LOG_DIR"

echo "[spark] $NAME on YARN, args: $* (log: $LOG_DIR/$NAME.err.log)"
cd "$APP_DIR"
spark-submit \
  --master yarn --deploy-mode client \
  --driver-memory "$SPARK_DRIVER_MEM" \
  --conf spark.yarn.am.memory="$SPARK_AM_MEM" \
  --conf spark.yarn.am.memoryOverhead="$SPARK_AM_OVERHEAD" \
  --executor-memory "$SPARK_EXEC_MEM" \
  --conf spark.yarn.executor.memoryOverhead="$SPARK_EXEC_OVERHEAD" \
  --num-executors 1 --executor-cores "$SPARK_EXEC_CORES" \
  --py-files "$APP_DIR/spark/common.py" \
  "$APP_DIR/$JOB" "$@" \
  2> "$LOG_DIR/$NAME.err.log" | tee "$LOG_DIR/$NAME.report.txt"
