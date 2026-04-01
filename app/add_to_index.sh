#!/bin/bash
set -euo pipefail

echo "Add one local document to index"

LOCAL_FILE="${1:-}"
if [ -z "${LOCAL_FILE}" ]; then
  echo "Usage: bash add_to_index.sh /path/to/<doc_id>_<doc_title>.txt"
  exit 1
fi

if [ ! -f "${LOCAL_FILE}" ]; then
  echo "File not found: ${LOCAL_FILE}"
  exit 1
fi

BASE_NAME="$(basename "${LOCAL_FILE}")"
if [[ "${BASE_NAME}" != *.txt ]]; then
  echo "Expected a .txt file, got: ${BASE_NAME}"
  exit 1
fi

NAME_NO_EXT="${BASE_NAME%.txt}"
if [[ "${NAME_NO_EXT}" != *_* ]]; then
  echo "File name must follow <doc_id>_<doc_title>.txt"
  exit 1
fi

DOC_ID="${NAME_NO_EXT%%_*}"
DOC_TITLE="${NAME_NO_EXT#*_}"

# Normalize text to single-line plain text for /input/data format.
DOC_TEXT="$(
python3 - "${LOCAL_FILE}" <<'PY'
import sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    text = f.read()
text = text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
text = " ".join(text.split())
print(text)
PY
)"

if [ -z "${DOC_TEXT}" ]; then
  echo "Document content is empty after normalization."
  exit 1
fi

echo "Uploading source document to HDFS /data ..."
hdfs dfs -mkdir -p /data
hdfs dfs -put -f "${LOCAL_FILE}" /data/

TMP_LOCAL_INPUT="/tmp/input_data_with_new_doc.tsv"
rm -f "${TMP_LOCAL_INPUT}"

if hdfs dfs -test -e /input/data; then
  hdfs dfs -cat /input/data/part-* > "${TMP_LOCAL_INPUT}"
fi
printf "%s\t%s\t%s\n" "${DOC_ID}" "${DOC_TITLE}" "${DOC_TEXT}" >> "${TMP_LOCAL_INPUT}"

echo "Updating HDFS /input/data ..."
hdfs dfs -rm -r -f /input/data >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /input/data
hdfs dfs -put "${TMP_LOCAL_INPUT}" /input/data/part-00000
hdfs dfs -touchz /input/data/_SUCCESS

echo "Incremental Cassandra update ..."
source .venv/bin/activate
python3 incremental_index.py "${LOCAL_FILE}"

echo "Document added (HDFS /input/data updated; Cassandra updated incrementally)."
