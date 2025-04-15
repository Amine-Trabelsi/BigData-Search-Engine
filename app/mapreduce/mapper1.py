#!/usr/bin/env python
import sys
import re

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        print("Skipping line (unexpected format):", line, file=sys.stderr)
        continue

    doc_id, title, content = parts
    words = re.findall(r'\w+', content.lower())
    
    for word in words:
        print(f"{word}\t{doc_id}\t1")
