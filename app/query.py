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
avgdl = 100  # 🔸 temporary placeholder

# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect('indexdb')

# 🔸 Fetch tf entries from inverted_index
query_terms_str = ", ".join(f"'{term}'" for term in query_terms)
inv_rows = session.execute(f"""
    SELECT term, doc_id, tf FROM inverted_index
    WHERE term IN ({query_terms_str})
""")
inv_rdd = sc.parallelize([(r.term, r.doc_id, r.tf) for r in inv_rows])

# 🔸 Estimate doc lengths (fake with tf sum per doc)
doc_lengths = (
    inv_rdd
    .map(lambda x: (x[1], x[2]))  # (doc_id, tf)
    .reduceByKey(lambda a, b: a + b)  # sum tf = doc length
    .collectAsMap()
)

# 🔸 Compute placeholder IDF values (assume fixed)
idf_map = {term: 1.5 for term in query_terms}  # placeholder

# 🔸 Join tf with length
tf_by_doc = inv_rdd.map(lambda x: (x[1], (x[0], x[2])))  # (doc_id, (term, tf))

doc_info_rdd = sc.parallelize(doc_lengths.items())  # (doc_id, length)
doc_info_rdd = doc_info_rdd.map(lambda x: (x[0], (x[1], f"Document_{x[0]}")))  # add title

joined = tf_by_doc.join(doc_info_rdd)  # (doc_id, ((term, tf), (length, title)))

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
