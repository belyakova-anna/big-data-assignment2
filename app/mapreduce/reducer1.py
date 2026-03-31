#!/usr/bin/env python3
import sys


# mapper1 already emits one line per (term, doc_id), keep passthrough reducer
for line in sys.stdin:
    line = line.rstrip("\n")
    if line:
        print(line)