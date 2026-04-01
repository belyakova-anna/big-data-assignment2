#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this

if [ -f "a.parquet" ]; then
  hdfs dfs -put -f a.parquet /
else
  echo "a.parquet not found, using local sample documents from /app/data."
fi

spark-submit --driver-memory 2g --conf spark.sql.parquet.enableVectorizedReader=false prepare_data.py && \
  hdfs dfs -ls /data && \
  hdfs dfs -ls /input/data && \
  echo "done data preparation!"
