#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 3:
        continue

    page, pr_old_str, neighbors_str = parts
    try:
        pr_old = float(pr_old_str)
    except ValueError:
        continue

    neighbors = neighbors_str.split(',') if neighbors_str else []
    outdeg = len(neighbors)

    if outdeg > 0:
        contrib = pr_old / outdeg
        for nbr in neighbors:
            print(f"{nbr}\t{contrib}")
    else:
        print(f"__dangling__\t{pr_old}")

    print(f"{page}\tADJ|{neighbors_str}")