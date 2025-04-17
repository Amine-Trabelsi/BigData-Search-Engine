#!/bin/bash

source .venv/bin/activate


export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON


echo "Uploading i.parquet to HDFS root..."
hdfs dfs -put -f data/i.parquet /


echo "Running Spark script to generate text documents..."
spark-submit prepare_data.py


echo "Consolidating .txt files to one input file..."
mkdir -p data/formatted/
rm -f data/formatted/input.txt

for f in data/*.txt; do
    filename=$(basename "$f" .txt)
    doc_id="${filename%%_*}"
    title="${filename#*_}"
    title="${title//_/ }"
    text=$(cat "$f" | tr '\n' ' ' | tr '\t' ' ')
    echo -e "${doc_id}\t${title}\t${text}" >> data/formatted/input.txt
done


echo "Uploading input.txt to HDFS..."
hdfs dfs -mkdir -p /index/data
hdfs dfs -put -f data/formatted/input.txt /index/data


echo "✅ Uploaded to HDFS:"
hdfs dfs -ls /index/data
echo "✅ Preview of input:"
hdfs dfs -cat /index/data/input.txt | head -n 3

echo "✅ Data preparation complete!"
