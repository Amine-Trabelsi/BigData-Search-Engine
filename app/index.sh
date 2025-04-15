#!/bin/bash
echo "This script runs the MapReduce indexing pipeline"

INPUT=${1:-/index/data}

OUTPUT=/tmp/index/output1

# Clean previous output if exists
hdfs dfs -rm -r -f $OUTPUT

# Run MapReduce
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -input $INPUT \
    -output $OUTPUT \
    -mapper mapreduce/mapper1.py \
    -reducer mapreduce/reducer1.py \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py
