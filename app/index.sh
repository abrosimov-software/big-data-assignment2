#!/bin/bash
DEFAULT_INDEX_PATH="/index/data"
HDFS_OUTPUT_PATH="/tmp/index/output"
CASSANDRA_DEFINITION_FILE="/cassandra/schema.sql"

input_path=${1:-"$DEFAULT_INDEX_PATH"}

source .venv/bin/activate

# Run app.py to prepare the cassandra server
python app.py

# Remove everything in the output path if it exists
hdfs dfs -rm -r -skipTrash "$HDFS_OUTPUT_PATH" 2>/dev/null || true

# Run the mapreduce job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "mapreduce/mapper1.py,mapreduce/reducer1.py" \
    -archives ".venv.tar.gz#.venv" \
    -mapper ".venv/bin/python mapper1.py" \
    -reducer ".venv/bin/python reducer1.py" \
    -input "$input_path" \
    -output "$HDFS_OUTPUT_PATH"
