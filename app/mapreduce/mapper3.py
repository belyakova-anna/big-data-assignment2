#!/usr/bin/env python3
import sys


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 5:
        continue
    _, doc_id, doc_title, _, doc_len = parts
    print(f"{doc_id}\t{doc_title}\t{doc_len}\t1")
