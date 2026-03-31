from cassandra.cluster import Cluster
import subprocess


def hdfs_lines(path_glob: str):
    proc = subprocess.run(
        f"hdfs dfs -cat {path_glob}",
        shell=True,
        text=True,
        capture_output=True,
        check=True,
    )
    for line in proc.stdout.splitlines():
        if line.strip():
            yield line

# Connects to the cassandra server
cluster = Cluster(["cassandra-server"])

session = cluster.connect()
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
)
session.set_keyspace("search_engine")

session.execute(
    """
    CREATE TABLE IF NOT EXISTS vocabulary (
      term text PRIMARY KEY,
      df int
    )
    """
)
session.execute(
    """
    CREATE TABLE IF NOT EXISTS postings (
      term text,
      doc_id text,
      title text,
      tf int,
      doc_len int,
      PRIMARY KEY (term, doc_id)
    )
    """
)
session.execute(
    """
    CREATE TABLE IF NOT EXISTS document_stats (
      doc_id text PRIMARY KEY,
      title text,
      doc_len int,
      unique_terms int
    )
    """
)
session.execute(
    """
    CREATE TABLE IF NOT EXISTS corpus_stats (
      metric text PRIMARY KEY,
      value double
    )
    """
)

session.execute("TRUNCATE vocabulary")
session.execute("TRUNCATE postings")
session.execute("TRUNCATE document_stats")
session.execute("TRUNCATE corpus_stats")

insert_vocab = session.prepare("INSERT INTO vocabulary(term, df) VALUES (?, ?)")
insert_posting = session.prepare(
    "INSERT INTO postings(term, doc_id, title, tf, doc_len) VALUES (?, ?, ?, ?, ?)"
)
insert_doc = session.prepare(
    "INSERT INTO document_stats(doc_id, title, doc_len, unique_terms) VALUES (?, ?, ?, ?)"
)
insert_corpus = session.prepare("INSERT INTO corpus_stats(metric, value) VALUES (?, ?)")

for line in hdfs_lines("/indexer/vocabulary/part-*"):
    term, df = line.split("\t", 1)
    session.execute(insert_vocab, (term, int(df)))

for line in hdfs_lines("/indexer/index/part-*"):
    parts = line.split("\t")
    if len(parts) != 5:
        continue
    term, doc_id, title, tf, doc_len = parts
    session.execute(insert_posting, (term, doc_id, title, int(tf), int(doc_len)))

for line in hdfs_lines("/indexer/doc_stats/part-*"):
    parts = line.split("\t")
    if len(parts) != 4:
        continue
    doc_id, title, doc_len, unique_terms = parts
    session.execute(insert_doc, (doc_id, title, int(doc_len), int(unique_terms)))

for line in hdfs_lines("/indexer/corpus_stats/part-*"):
    metric, value = line.split("\t", 1)
    session.execute(insert_corpus, (metric, float(value)))

print("Index data loaded into Cassandra keyspace 'search_engine'.")