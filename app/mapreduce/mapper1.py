#!/usr/bin/env python3
import re
import sys
from collections import Counter


TOKEN_RE = re.compile(r"[a-z0-9]+")


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, doc_title, doc_text = parts
    tokens = TOKEN_RE.findall(doc_text.lower())
    if not tokens:
        continue

    doc_len = len(tokens)
    tf_by_term = Counter(tokens)

    for term, tf in tf_by_term.items():
        print(f"{term}\t{doc_id}\t{doc_title}\t{tf}\t{doc_len}")