#!/usr/bin/env bash
# =====================================================================
# INGEST  (runs on the sandbox / edge node)
# 1. VALIDATE  every expected file is present, matches its manifest
#              checksum, and has at least the expected number of lines
# 2. LOAD      put each file into HDFS raw/<table>/load_date=<date>/
#              (re-running the same date replaces it: idempotent)
# 3. FLAG      write raw/_ingest/load_date=<date>/_SUCCESS - the signal
#              downstream jobs and the Oozie coordinator wait for
# 4. AUDIT     every step is appended to /opt/olist/logs/ingest_audit.log
#
# Usage:  ingest/ingest_olist.sh [YYYY-MM-DD]     (default: today)
# =====================================================================
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$APP_DIR/conf/olist.conf"

LOAD_DATE="${1:-$(date +%F)}"
SRC="$REMOTE_LANDING/$LOAD_DATE"
FLAG_DIR="$HDFS_INGEST_FLAGS/load_date=$LOAD_DATE"

mkdir -p "$(dirname "$AUDIT_LOG")"
log()  { echo "$(date '+%F %T') | $LOAD_DATE | $*" | tee -a "$AUDIT_LOG"; }
fail() { log "FAILED    | $*"; exit 1; }

log "START     | landing=$SRC"
[ -d "$SRC" ]              || fail "landing folder not found: $SRC"
[ -f "$SRC/MANIFEST.tsv" ] || fail "MANIFEST.tsv missing in $SRC"

# Clear any earlier success flag for this date before reloading.
hdfs dfs -rm -r -f -skipTrash "$FLAG_DIR" >/dev/null 2>&1 || true

# ---------------------------------------------------------- 1. validate
for spec in $OLIST_TABLES; do
  IFS=: read -r tbl name min_lines <<< "$spec"
  f="$SRC/$name.csv"
  [ -s "$f" ] || fail "$name.csv missing or empty"

  expected_md5=$(awk -F'\t' -v n="$name.csv" '$1==n {print $3}' "$SRC/MANIFEST.tsv")
  actual_md5=$(md5sum "$f" | cut -d' ' -f1)
  [ -n "$expected_md5" ]                || fail "$name.csv not listed in manifest"
  [ "$expected_md5" = "$actual_md5" ]   || fail "$name.csv checksum mismatch (corrupt transfer)"

  lines=$(wc -l < "$f")
  [ "$lines" -ge "$min_lines" ]         || fail "$name.csv has $lines lines, expected >= $min_lines"
done
log "VALIDATED | all files present, checksums match, line counts OK"

# ---------------------------------------------------------- 2. load
for spec in $OLIST_TABLES; do
  IFS=: read -r tbl name min_lines <<< "$spec"
  f="$SRC/$name.csv"
  dest="$HDFS_RAW/$tbl/load_date=$LOAD_DATE"

  hdfs dfs -rm -r -f -skipTrash "$dest" >/dev/null 2>&1 || true
  hdfs dfs -mkdir -p "$dest"
  hdfs dfs -put "$f" "$dest/$name.csv"

  log "LOADED    | $tbl | $(stat -c%s "$f") bytes | $(wc -l < "$f") lines | md5 $(md5sum "$f" | cut -d' ' -f1)"
done

# ---------------------------------------------------------- 3. flag
hdfs dfs -mkdir -p "$FLAG_DIR"
hdfs dfs -put -f "$SRC/MANIFEST.tsv" "$FLAG_DIR/MANIFEST.tsv"
hdfs dfs -touchz "$FLAG_DIR/_SUCCESS"
log "SUCCESS   | flag written: $FLAG_DIR/_SUCCESS"
