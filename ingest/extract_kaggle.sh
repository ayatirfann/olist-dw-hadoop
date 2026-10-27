#!/usr/bin/env bash
# =====================================================================
# EXTRACT  (runs on the Mac)
# Downloads the Olist dataset from Kaggle's API into a dated landing
# folder and writes a MANIFEST.tsv (file, bytes, md5) so the sandbox
# can prove the files arrived intact.
#
# Usage:  ingest/extract_kaggle.sh [YYYY-MM-DD]     (default: today)
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source conf/olist.conf

LOAD_DATE="${1:-$(date +%F)}"
OUT="$LOCAL_LANDING/$LOAD_DATE"

echo "[extract] dataset   : $KAGGLE_DATASET"
echo "[extract] load date : $LOAD_DATE"
echo "[extract] target    : $OUT"

rm -rf "$OUT"
mkdir -p "$OUT"

python3 -m kaggle datasets download "$KAGGLE_DATASET" -p "$OUT" --unzip --force

# Every expected file must be present.
missing=0
for spec in $OLIST_TABLES; do
  file="$(echo "$spec" | cut -d: -f2).csv"
  if [ ! -s "$OUT/$file" ]; then
    echo "[extract] MISSING: $file"
    missing=1
  fi
done
[ "$missing" -eq 0 ] || { echo "[extract] FAILED - files missing"; exit 1; }

# Manifest: name <TAB> bytes <TAB> md5
: > "$OUT/MANIFEST.tsv"
for spec in $OLIST_TABLES; do
  file="$(echo "$spec" | cut -d: -f2).csv"
  bytes=$(wc -c < "$OUT/$file" | tr -d ' ')
  md5=$(md5 -q "$OUT/$file")
  printf "%s\t%s\t%s\n" "$file" "$bytes" "$md5" >> "$OUT/MANIFEST.tsv"
done

echo "[extract] OK - $(wc -l < "$OUT/MANIFEST.tsv" | tr -d ' ') files, manifest written"
