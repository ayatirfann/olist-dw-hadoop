#!/usr/bin/env bash
# =====================================================================
# RUN HIVE SCRIPT  (runs on the sandbox)
# Runs one SQL file through beeline/HiveServer2. The database name and the
# curated HDFS path are passed in from conf/olist.conf as Hive variables
# (${hivevar:DB}, ${hivevar:CURATED}), so no path is written in the SQL.
# Output is shown and saved to /opt/olist/logs/<script>.out.
#
# Usage:  ./run_hive.sh hive/<script>.sql
# From the Mac:
#   ssh olist-sandbox "/opt/olist/app/run_hive.sh hive/queries.sql"
# =====================================================================
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$APP_DIR/conf/olist.conf"

SQL="$1"
NAME="$(basename "$SQL" .sql)"
mkdir -p "$LOG_DIR"

echo "[hive] $SQL  (db: $HIVE_DB, curated: $HDFS_CURATED, log: $LOG_DIR/$NAME.out)"
beeline -u "$HIVE_JDBC" -n "$HIVE_USER" --silent=true --showHeader=true \
  --hivevar DB="$HIVE_DB" --hivevar CURATED="$HDFS_CURATED" \
  -f "$APP_DIR/$SQL" 2>&1 | tee "$LOG_DIR/$NAME.out"
