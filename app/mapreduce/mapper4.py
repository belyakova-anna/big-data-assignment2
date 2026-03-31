#!/usr/bin/env python3
import sys


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 4:
        continue
    _, _, doc_len, _ = parts
    print(f"corpus\t1\t{doc_len}")
