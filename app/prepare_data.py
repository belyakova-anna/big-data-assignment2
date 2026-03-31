import os
import re
import shutil
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


N_DOCS = 1000
LOCAL_DATA_DIR = "/app/data"
HDFS_DOCS_DIR = "/data"
HDFS_INPUT_DIR = "/input/data"


def build_filename(doc_id: str, title: str) -> str:
    safe_title = sanitize_filename(str(title), replacement_text="_")
    safe_title = re.sub(r"\s+", "_", safe_title).strip("_")
    if not safe_title:
        safe_title = "untitled"
    return f"{doc_id}_{safe_title}.txt"


def parse_hdfs_doc(record):
    path, text = record
    name = path.rsplit("/", 1)[-1]
    if name.endswith(".txt"):
        name = name[:-4]
    doc_id, doc_title = name.split("_", 1) if "_" in name else (name, "")
    doc_text = (text or "").replace("\n", " ").strip()
    return f"{doc_id}\t{doc_title}\t{doc_text}"


spark = (
    SparkSession.builder
    .appName("data preparation")
    .master("local")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

df = (
    spark.read.parquet("/a.parquet")
    .select("id", "title", "text")
    .where(F.col("id").isNotNull() & F.col("title").isNotNull() & F.col("text").isNotNull())
    .where(F.length(F.trim(F.col("text"))) > 0)
    .limit(N_DOCS)
)

if os.path.exists(LOCAL_DATA_DIR):
    shutil.rmtree(LOCAL_DATA_DIR)
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

rows = df.collect()
for row in rows:
    file_name = build_filename(str(row["id"]), str(row["title"]))
    file_path = os.path.join(LOCAL_DATA_DIR, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(row["text"])

sc = spark.sparkContext
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_DOCS_DIR], check=False)
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_INPUT_DIR], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DOCS_DIR], check=True)
subprocess.run(f"hdfs dfs -put -f {LOCAL_DATA_DIR}/*.txt {HDFS_DOCS_DIR}/", shell=True, check=True)

# Build /input/data with one partition: <doc_id>\t<doc_title>\t<doc_text>
docs_rdd = sc.wholeTextFiles("hdfs:///data/*.txt")
prepared_rdd = docs_rdd.map(parse_hdfs_doc).filter(lambda line: line.strip()).coalesce(1)
prepared_rdd.saveAsTextFile("hdfs:///input/data")

spark.stop()