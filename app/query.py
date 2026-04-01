#!/usr/bin/env python3
import math
import re
import sys
from typing import Dict, List, Tuple

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


TOKEN_RE = re.compile(r"[a-z0-9]+")
K1 = 1.2
B = 0.75


def tokenize(query: str) -> List[str]:
    return TOKEN_RE.findall(query.lower())


def fetch_corpus_stats(session) -> Tuple[int, float]:
    rows = session.execute("SELECT metric, value FROM corpus_stats")
    stats: Dict[str, float] = {row.metric: float(row.value) for row in rows}
    docs_count = int(stats.get("docs_count", 0.0))
    avg_doc_len = float(stats.get("avg_doc_len", 0.0))
    return docs_count, avg_doc_len


def fetch_df(session, term: str) -> int:
    row = session.execute("SELECT df FROM vocabulary WHERE term=%s", (term,)).one()
    return int(row.df) if row else 0


def fetch_postings(session, term: str):
    rows = session.execute(
        "SELECT doc_id, title, tf, doc_len FROM postings WHERE term=%s",
        (term,),
    )
    return [(row.doc_id, row.title, int(row.tf), int(row.doc_len)) for row in rows]


def bm25_score(tf: int, doc_len: int, df: int, docs_count: int, avg_doc_len: float) -> float:
    if tf <= 0 or df <= 0 or docs_count <= 0 or avg_doc_len <= 0:
        return 0.0
    # Assignment formula: log(N / df)
    idf = math.log(docs_count / df)
    norm = K1 * (1.0 - B + B * (doc_len / avg_doc_len)) + tf
    return idf * ((K1 + 1.0) * tf) / norm


def main():
    query = " ".join(sys.argv[1:]).strip() if len(sys.argv) > 1 else sys.stdin.read().strip()
    if not query:
        print("Empty query")
        return

    terms = tokenize(query)
    if not terms:
        print("No valid query terms")
        return

    spark = SparkSession.builder.appName("ranker-bm25").master("yarn").getOrCreate()
    sc = spark.sparkContext

    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("search_engine")

    docs_count, avg_doc_len = fetch_corpus_stats(session)
    if docs_count == 0:
        print("No indexed documents found")
        spark.stop()
        cluster.shutdown()
        return

    scored_rows = []
    for term in terms:
        df = fetch_df(session, term)
        if df == 0:
            continue
        postings = fetch_postings(session, term)
        for doc_id, title, tf, doc_len in postings:
            score = bm25_score(tf, doc_len, df, docs_count, avg_doc_len)
            scored_rows.append((doc_id, title, score))

    if not scored_rows:
        print("No matching documents found")
        spark.stop()
        cluster.shutdown()
        return

    top10 = (
        sc.parallelize(scored_rows)
        .map(lambda x: ((x[0], x[1]), x[2]))
        .reduceByKey(lambda a, b: a + b)
        .map(lambda x: (x[0][0], x[0][1], x[1]))
        .takeOrdered(10, key=lambda x: -x[2])
    )

    print(f"query\t{query}")
    for doc_id, title, score in top10:
        print(f"{doc_id}\t{title}\tscore={score:.6f}")

    spark.stop()
    cluster.shutdown()


if __name__ == "__main__":
    main()