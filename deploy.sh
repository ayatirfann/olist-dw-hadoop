#!/usr/bin/env bash
# =====================================================================
# DEPLOY  (runs on the Mac)
# Ships the current repo to the sandbox so it always runs what is in Git:
#   Mac repo  ->  sandbox local disk  /opt/olist/app/
#             ->  HDFS                /apps/olist/   (for Oozie, later)
#
# Usage:  ./deploy.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")"
source conf/olist.conf

echo "[deploy] commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'not committed')"

# Pack the repo (minus .git and junk) and unpack it on the sandbox.
# COPYFILE_DISABLE stops macOS adding hidden ._ files to the archive.
COPYFILE_DISABLE=1 tar --exclude=.git --exclude='*.csv' --exclude='.DS_Store' -czf - . \
  | ssh "$SANDBOX_SSH" "rm -rf '$REMOTE_APP_DIR' && mkdir -p '$REMOTE_APP_DIR' && tar -xzf - -C '$REMOTE_APP_DIR' && chmod +x '$REMOTE_APP_DIR'/ingest/*.sh '$REMOTE_APP_DIR'/*.sh"

# Publish code to HDFS, where Oozie workflows will read it.
ssh "$SANDBOX_SSH" "hdfs dfs -mkdir -p '$HDFS_APP_DIR' && hdfs dfs -rm -r -f -skipTrash '$HDFS_APP_DIR'/* >/dev/null 2>&1; hdfs dfs -put -f '$REMOTE_APP_DIR'/conf '$REMOTE_APP_DIR'/ingest '$REMOTE_APP_DIR'/spark '$REMOTE_APP_DIR'/hive '$REMOTE_APP_DIR'/oozie '$HDFS_APP_DIR'/ 2>/dev/null || true"

echo "[deploy] OK -> $SANDBOX_SSH:$REMOTE_APP_DIR and hdfs:$HDFS_APP_DIR"
