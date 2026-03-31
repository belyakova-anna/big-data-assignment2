#!/bin/bash
set -euo pipefail

echo "Create index using MapReduce pipelines"

INPUT_PATH="${1:-/input/data}"
STREAMING_JAR="$(ls "${HADOOP_HOME}"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -n 1)"

if [ -z "${STREAMING_JAR}" ] || [ ! -f "${STREAMING_JAR}" ]; then
  echo "Could not find hadoop-streaming jar in ${HADOOP_HOME}/share/hadoop/tools/lib/"
  exit 1
fi

if ! hdfs dfs -test -e "${INPUT_PATH}"; then
  echo "Input path not found in HDFS: ${INPUT_PATH}"
  exit 1
fi

hdfs dfs -rm -r -f /tmp/indexer >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /indexer >/dev/null 2>&1 || true

echo "Pipeline 1: postings index -> /indexer/index"
hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "${INPUT_PATH}" \
  -output /indexer/index

echo "Pipeline 2: vocabulary(df) -> /indexer/vocabulary"
hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper2.py,mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input /indexer/index \
  -output /indexer/vocabulary

echo "Pipeline 3: per-document stats -> /indexer/doc_stats"
hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper3.py,mapreduce/reducer3.py \
  -mapper "python3 mapper3.py" \
  -reducer "python3 reducer3.py" \
  -input /indexer/index \
  -output /indexer/doc_stats

echo "Pipeline 4: corpus stats for BM25 -> /indexer/corpus_stats"
hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper4.py,mapreduce/reducer4.py \
  -mapper "python3 mapper4.py" \
  -reducer "python3 reducer4.py" \
  -input /indexer/doc_stats \
  -output /indexer/corpus_stats

echo "Indexer outputs:"
hdfs dfs -ls /indexer
