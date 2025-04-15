#!/bin/bash

QUERY="$1"

if [ -z "$QUERY" ]; then
  echo "Usage: bash search.sh \"your query here\""
  exit 1
fi

spark-submit --master yarn \
    --deploy-mode cluster \
    --conf spark.cassandra.connection.host=cassandra-server \
    query.py "$QUERY"
