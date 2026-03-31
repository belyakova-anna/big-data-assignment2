#!/usr/bin/env python3
import sys


current_term = None
df = 0

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 1)
    if len(parts) != 2:
        continue
    term, value = parts
    try:
        value = int(value)
    except ValueError:
        continue

    if current_term is None:
        current_term = term

    if term != current_term:
        print(f"{current_term}\t{df}")
        current_term = term
        df = 0

    df += value

if current_term is not None:
    print(f"{current_term}\t{df}")
