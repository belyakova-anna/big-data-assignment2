#!/usr/bin/env python3
import sys


docs_count = 0
doc_len_sum = 0

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 3:
        continue

    _, docs, doc_len = parts
    try:
        docs_count += int(docs)
        doc_len_sum += int(doc_len)
    except ValueError:
        continue

avg_doc_len = (doc_len_sum / docs_count) if docs_count else 0.0
print(f"docs_count\t{docs_count}")
print(f"avg_doc_len\t{avg_doc_len:.6f}")
