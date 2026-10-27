#!/usr/bin/env bash
# =====================================================================
# TRANSFER  (runs on the Mac)
# Copies one dated landing folder from the Mac to the sandbox's
# landing zone (/opt/olist/landing/<date>/) over SSH.
#
# Usage:  ingest/transfer_to_sandbox.sh [YYYY-MM-DD]   (default: today)
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source conf/olist.conf

LOAD_DATE="${1:-$(date +%F)}"
SRC="$LOCAL_LANDING/$LOAD_DATE"

[ -f "$SRC/MANIFEST.tsv" ] || { echo "[transfer] no manifest in $SRC - run extract first"; exit 1; }

echo "[transfer] $SRC -> $SANDBOX_SSH:$REMOTE_LANDING/$LOAD_DATE"
ssh "$SANDBOX_SSH" "rm -rf '$REMOTE_LANDING/$LOAD_DATE' && mkdir -p '$REMOTE_LANDING'"
scp -O -r -q "$SRC" "$SANDBOX_SSH:$REMOTE_LANDING/"
echo "[transfer] OK"
