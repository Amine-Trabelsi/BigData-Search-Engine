import sys
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

query_terms = sys.argv[1].lower().split()

spark = SparkSession.builder \
    .appName("BM25 Query") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()

sc = spark.sparkContext

k1 = 1.5
b = 0.75

cluster = Cluster(['cassandra-server'])
session = cluster.connect('indexdb')

doc_count = session.execute("SELECT COUNT(*) FROM documents").one()[0]

doc_lengths = session.execute("SELECT doc_id, length, title FROM documents")
doc_data = [(row.doc_id, row.length, row.title) for row in doc_lengths]
doc_rdd = sc.parallelize(doc_data)  # (doc_id, length, title)

avgdl = doc_rdd.map(lambda x: x[1]).mean()

vocab = session.execute("SELECT term, idf FROM vocabulary")
idf_map = {row.term: row.idf for row in vocab}

query_terms_str = ", ".join(f"'{term}'" for term in query_terms)
inv_rows = session.execute(f"""
    SELECT term, doc_id, tf FROM inverted_index
    WHERE term IN ({query_terms_str})
""")
inv_rdd = sc.parallelize([(r.term, r.doc_id, r.tf) for r in inv_rows])

tf_by_doc = inv_rdd.map(lambda x: (x[1], (x[0], x[2])))  # (doc_id, (term, tf))
docs = doc_rdd.map(lambda x: (x[0], (x[1], x[2])))        # (doc_id, (length, title))

joined = tf_by_doc.join(docs)
# => (doc_id, ((term, tf), (length, title)))

def bm25_score(record):
    doc_id, ((term, tf), (dl, title)) = record
    idf = idf_map.get(term, 0)
    score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
    return (doc_id, (title, score))

scores = joined.map(bm25_score)

agg_scores = scores.reduceByKey(lambda a, b: (a[0], a[1] + b[1]))  # (doc_id, (title, total_score))

top10 = agg_scores.takeOrdered(10, key=lambda x: -x[1][1])

print("\nTop 10 documents:")
for doc_id, (title, score) in top10:
    print(f"{doc_id}\t{title}\tBM25 Score: {round(score, 3)}")

spark.stop()
