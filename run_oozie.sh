#!/usr/bin/env bash
# =====================================================================
# RUN OOZIE  (runs on the sandbox)
# Builds job.properties from conf/olist.conf (so nothing is typed twice)
# and submits the batch workflow or the daily coordinator to Oozie.
#
# Usage:
#   ./run_oozie.sh validate                       check the XML files
#   ./run_oozie.sh run      <load_date>           run the batch once for one load date
#   ./run_oozie.sh schedule <start_date> <end_date>  daily coordinator, e.g. 2026-11-03 2026-11-10
#   ./run_oozie.sh status   <job_id>              show a job and its steps
#   ./run_oozie.sh kill     <job_id>              stop a job
# From the Mac:
#   ssh olist-sandbox "/opt/olist/app/run_oozie.sh run 2026-11-03"
# =====================================================================
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$APP_DIR/conf/olist.conf"
export OOZIE_URL
mkdir -p "$LOG_DIR"

CMD="${1:-}"; shift || true
NAME_NODE="$(hdfs getconf -confKey fs.defaultFS)"     # read from the cluster, not typed in
PROPS="$LOG_DIR/oozie_job.properties"

write_props() {
  cat > "$PROPS" <<PROPS
nameNode=$NAME_NODE
appDir=$NAME_NODE$OOZIE_APP_DIR
appHome=$REMOTE_APP_DIR
sshHost=$OOZIE_SSH_HOST
rawFlagsDir=$HDFS_INGEST_FLAGS
batchFlagsDir=$HDFS_BATCH_FLAGS
coordTimeoutMinutes=$OOZIE_COORD_TIMEOUT_MIN
PROPS
}

case "$CMD" in
  validate)
    oozie validate -oozie "$OOZIE_URL" "$APP_DIR/oozie/workflow.xml"
    oozie validate -oozie "$OOZIE_URL" "$APP_DIR/oozie/coordinator.xml"
    ;;
  run)
    LOAD_DATE="${1:?usage: run_oozie.sh run <load_date>}"
    write_props
    { echo "loadDate=$LOAD_DATE"
      echo "oozie.wf.application.path=$NAME_NODE$OOZIE_APP_DIR"; } >> "$PROPS"
    echo "[oozie] submitting batch workflow for load_date=$LOAD_DATE"
    oozie job -config "$PROPS" -run
    ;;
  schedule)
    START="${1:?usage: run_oozie.sh schedule <start_date> <end_date>}"
    END="${2:?usage: run_oozie.sh schedule <start_date> <end_date>}"
    write_props
    { echo "coordStart=${START}T00:00Z"
      echo "coordEnd=${END}T00:00Z"
      echo "oozie.coord.application.path=$NAME_NODE$OOZIE_APP_DIR/coordinator.xml"; } >> "$PROPS"
    echo "[oozie] submitting daily coordinator $START -> $END"
    oozie job -config "$PROPS" -run
    ;;
  status) oozie job -info "${1:?job id}" ;;
  kill)   oozie job -kill "${1:?job id}" ;;
  *) sed -n '3,15p' "$0"; exit 1 ;;
esac
