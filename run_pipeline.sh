#!/usr/bin/env bash
# =====================================================================
# RUN PIPELINE  (runs on the Mac) - the single entry point
#   1. extract   Kaggle API -> Mac landing folder (+ manifest)
#   2. transfer  Mac -> sandbox landing zone
#   3. ingest    validate -> HDFS raw -> _SUCCESS flag   (on the sandbox)
#   4. (later)   the Oozie coordinator sees the flag and runs the
#                Spark/Hive batch workflow on YARN
#
# Usage:  ./run_pipeline.sh [YYYY-MM-DD]          (default: today)
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")"
source conf/olist.conf

LOAD_DATE="${1:-$(date +%F)}"
echo "================ olist pipeline | load date $LOAD_DATE ================"

ingest/extract_kaggle.sh      "$LOAD_DATE"
ingest/transfer_to_sandbox.sh "$LOAD_DATE"
ssh "$SANDBOX_SSH" "'$REMOTE_APP_DIR/ingest/ingest_olist.sh' '$LOAD_DATE'"

echo "================ ingestion complete for $LOAD_DATE ================"
