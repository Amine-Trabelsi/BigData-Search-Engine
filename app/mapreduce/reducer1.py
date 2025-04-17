#!/usr/bin/env python3
import sys
from collections import defaultdict
from cassandra.cluster import Cluster
import traceback

current_key = None
current_tf = 0

try:
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS indexdb WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
    session.set_keyspace('indexdb')
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term TEXT,
            doc_id TEXT,
            tf INT,
            PRIMARY KEY (term, doc_id)
        )
    """)
except Exception as e:
    print("Cassandra connection failed!", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

def emit(term, doc_id, tf):
    try:
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, tf)
        )
        print(f"Inserted: {term}\t{doc_id}\t{tf}", file=sys.stderr)
    except Exception as e:
        print(f"Failed to insert {term}\t{doc_id}: {e}", file=sys.stderr)


for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        continue

    term, doc_id, count = parts
    key = (term, doc_id)

    if current_key == key:
        current_tf += int(count)
    else:
        if current_key:
            t, d = current_key
            emit(t, d, current_tf)
        current_key = key
        current_tf = int(count)


if current_key:
    t, d = current_key
    emit(t, d, current_tf)
