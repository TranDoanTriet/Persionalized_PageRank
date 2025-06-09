# import sys

# for line in sys.stdin:
#     # loại bỏ khoảng trắng, kiểm tra dòng có rỗng hay không
#     line = line.strip()
#     if not line:
#         continue

#     # tách tab và kiểm tra số lượng phần tử 1 hàng
#     # 
#     parts = line.split('\t')
#     if len(parts) != 3:
#         continue

#     node, pr_old_str, neighbors_str = parts
#     try:
#         pr_old = float(pr_old_str)
#     except ValueError:
#         continue

#     neighbors = neighbors_str.split(',') if neighbors_str else []
#     outdeg = len(neighbors)

#     if outdeg > 0:
#         contribution = pr_old / outdeg
#         for nbr in neighbors:
#             print(f"{nbr}\t{contribution}")
#     else:
#         print(f"__dangling__\t{pr_old}")

#     print("{}\t{}".format(nbr, contribution))

#     print(f"{node}\tADJ|{neighbors_str}")






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

    # Emit contribution to neighbors
    if outdeg > 0:
        contrib = pr_old / outdeg
        for nbr in neighbors:
            print(f"{nbr}\t{contrib}")
    else:
        # Dangling node: emit to global key
        print(f"__dangling__\t{pr_old}")

    # Emit adjacency list for reconstruction
    print(f"{page}\tADJ|{neighbors_str}")