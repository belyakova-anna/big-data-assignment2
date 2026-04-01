#!/usr/bin/env python3
"""Update Cassandra index tables for one new or replaced document (no full HDFS reload)."""
from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path
from typing import List, Tuple

from cassandra.cluster import Cluster

TOKEN_RE = re.compile(r"[a-z0-9]+")
CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"


def normalize_text(text: str) -> str:
    text = text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return " ".join(text.split())


def parse_local_file(path: str) -> Tuple[str, str, str]:
    p = Path(path)
    if not p.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)
    base = p.name
    if not base.endswith(".txt"):
        print("Expected a .txt file", file=sys.stderr)
        sys.exit(1)
    name = base[:-4]
    if "_" not in name:
        print("File name must follow <doc_id>_<doc_title>.txt", file=sys.stderr)
        sys.exit(1)
    doc_id, title = name.split("_", 1)
    raw = p.read_text(encoding="utf-8")
    doc_text = normalize_text(raw).strip()
    if not doc_text:
        print("Document content is empty after normalization.", file=sys.stderr)
        sys.exit(1)
    return doc_id, title, doc_text


def tokenize(doc_text: str) -> List[str]:
    return TOKEN_RE.findall(doc_text.lower())


def remove_doc_from_index(session, doc_id: str) -> None:
    rows = list(
        session.execute(
            "SELECT term FROM postings WHERE doc_id=%s ALLOW FILTERING", (doc_id,)
        )
    )
    for r in rows:
        term = r.term
        session.execute("DELETE FROM postings WHERE term=%s AND doc_id=%s", (term, doc_id))
        v = session.execute("SELECT df FROM vocabulary WHERE term=%s", (term,)).one()
        if not v:
            continue
        new_df = int(v.df) - 1
        if new_df <= 0:
            session.execute("DELETE FROM vocabulary WHERE term=%s", (term,))
        else:
            session.execute("UPDATE vocabulary SET df=%s WHERE term=%s", (new_df, term))

    session.execute("DELETE FROM document_stats WHERE doc_id=%s", (doc_id,))


def get_corpus_stats(session) -> Tuple[int, float]:
    rows = list(session.execute("SELECT metric, value FROM corpus_stats"))
    m = {r.metric: float(r.value) for r in rows}
    return int(m.get("docs_count", 0.0)), float(m.get("avg_doc_len", 0.0))


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: incremental_index.py /path/to/<doc_id>_<doc_title>.txt", file=sys.stderr)
        sys.exit(1)

    doc_id, title, doc_text = parse_local_file(sys.argv[1])
    tokens = tokenize(doc_text)
    if not tokens:
        print("No indexable tokens in document.", file=sys.stderr)
        sys.exit(1)

    doc_len = len(tokens)
    unique_terms = len(set(tokens))
    tf_by_term = Counter(tokens)

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )
    session.set_keyspace(KEYSPACE)
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

    insert_posting = session.prepare(
        "INSERT INTO postings(term, doc_id, title, tf, doc_len) VALUES (?, ?, ?, ?, ?)"
    )
    insert_vocab = session.prepare("INSERT INTO vocabulary(term, df) VALUES (?, ?)")
    insert_doc = session.prepare(
        "INSERT INTO document_stats(doc_id, title, doc_len, unique_terms) VALUES (?, ?, ?, ?)"
    )
    insert_corpus = session.prepare("INSERT INTO corpus_stats(metric, value) VALUES (?, ?)")

    docs_count, avg_doc_len = get_corpus_stats(session)

    row_existing = session.execute(
        "SELECT doc_len FROM document_stats WHERE doc_id=%s", (doc_id,)
    ).one()
    had_doc = row_existing is not None
    old_len = int(row_existing.doc_len) if had_doc else None

    if had_doc:
        remove_doc_from_index(session, doc_id)

    for term, tf in tf_by_term.items():
        session.execute(insert_posting, (term, doc_id, title, tf, doc_len))
        v = session.execute("SELECT df FROM vocabulary WHERE term=%s", (term,)).one()
        new_df = (int(v.df) + 1) if v else 1
        session.execute(insert_vocab, (term, new_df))

    session.execute(insert_doc, (doc_id, title, doc_len, unique_terms))

    if had_doc:
        if docs_count <= 0:
            new_n = 1
            new_avg = float(doc_len)
        else:
            new_n = docs_count
            new_avg = (avg_doc_len * docs_count - old_len + doc_len) / docs_count
    else:
        new_n = docs_count + 1
        new_avg = (
            (avg_doc_len * docs_count + doc_len) / new_n if new_n > 0 else float(doc_len)
        )

    session.execute(insert_corpus, ("docs_count", float(new_n)))
    session.execute(insert_corpus, ("avg_doc_len", new_avg))

    cluster.shutdown()
    print("Cassandra index updated incrementally.")


if __name__ == "__main__":
    main()
