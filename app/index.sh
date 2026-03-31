#!/bin/bash
set -euo pipefail

echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

INPUT_PATH="${1:-/input/data}"
echo "Input path is : ${INPUT_PATH}"

bash create_index.sh "${INPUT_PATH}"
bash store_index.sh

echo "Indexing completed!"