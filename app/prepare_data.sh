echo "Consolidating .txt files to one input file..."
mkdir -p data/formatted/
for f in data/*.txt; do
    filename=$(basename "$f" .txt)
    doc_id="${filename%%_*}"
    title="${filename#*_}"
    title="${title//_/ }"
    text=$(cat "$f" | tr '\n' ' ' | tr '\t' ' ')
    echo -e "${doc_id}\t${title}\t${text}" >> data/formatted/input.txt
done

echo "Uploading to HDFS..."
hdfs dfs -mkdir -p /index/data
hdfs dfs -put -f data/formatted/input.txt /index/data
