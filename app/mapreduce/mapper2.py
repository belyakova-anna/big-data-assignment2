#!/usr/bin/env python3
import sys


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 5:
        continue
    term = parts[0]
    print(f"{term}\t1")
