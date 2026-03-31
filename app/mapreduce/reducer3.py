#!/usr/bin/env python3
import sys


current_doc = None
title = ""
doc_len = 0
unique_terms = 0

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 4:
        continue

    doc_id, doc_title, doc_len_str, one = parts
    try:
        current_doc_len = int(doc_len_str)
        term_count = int(one)
    except ValueError:
        continue

    if current_doc is None:
        current_doc = doc_id
        title = doc_title
        doc_len = current_doc_len

    if doc_id != current_doc:
        print(f"{current_doc}\t{title}\t{doc_len}\t{unique_terms}")
        current_doc = doc_id
        title = doc_title
        doc_len = current_doc_len
        unique_terms = 0

    unique_terms += term_count

if current_doc is not None:
    print(f"{current_doc}\t{title}\t{doc_len}\t{unique_terms}")
