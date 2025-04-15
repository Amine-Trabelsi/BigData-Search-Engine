#!/usr/bin/env python
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
