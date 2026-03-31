#!/bin/bash
set -euo pipefail

echo "store the index and others to Cassandra tables"

source .venv/bin/activate
python3 app.py
