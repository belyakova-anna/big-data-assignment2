#!/bin/bash
set -euo pipefail

echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

QUERY="${*:-}"
if [ -z "${QUERY}" ]; then
  echo "Usage: bash search.sh \"your query text\""
  exit 1
fi

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py "${QUERY}"