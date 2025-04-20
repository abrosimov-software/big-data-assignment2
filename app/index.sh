#!/bin/bash
set -e

# Get the input file/directory path from the command-line argument
INPUT_PATH=$1

if [ -z "$INPUT_PATH" ]; then
    echo "Error: Input path not provided."
    echo "Usage: ./index.sh <input_path>"
    exit 1
fi

echo "=== BM25 Document Indexing Pipeline ==="
echo "Input path: $INPUT_PATH"

# Source Python virtual environment if available
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "Activated Python virtual environment"
fi

# Load configuration from config.py
echo "Loading configuration variables from config.py"
HDFS_FINAL_OUTPUT=$(python -c 'from config import HDFS_FINAL_OUTPUT; print(HDFS_FINAL_OUTPUT)')

echo "Final output path: $HDFS_FINAL_OUTPUT"

# Step 1: Check if input path exists in HDFS
echo "=== Step 1: Checking input path in HDFS ==="
if ! hdfs dfs -test -e "$INPUT_PATH" 2>/dev/null; then
    echo "Warning: Input path $INPUT_PATH does not exist in HDFS"
    
    # Check if the path exists locally
    if [ -e "$INPUT_PATH" ]; then
        echo "Input path exists locally, uploading to HDFS..."
        hdfs dfs -mkdir -p "$(dirname "$INPUT_PATH")"
        hdfs dfs -put -f "$INPUT_PATH" "$INPUT_PATH"
    else
        echo "Error: Input path $INPUT_PATH does not exist locally or in HDFS"
        exit 1
    fi
fi

# Step 2: Clean up previous output directories if they exist
echo "=== Step 2: Cleaning up previous output directories ==="
hdfs dfs -rm -r -skipTrash "$HDFS_FINAL_OUTPUT" 2>/dev/null || true
echo "Previous output directories cleaned"

cqlsh cassandra-server -f cassandra/schema.sql

# Step 3: Run MapReduce job
echo "=== Step 3: Running MapReduce job ==="
mapred streaming \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py,config.py \
    -archives "/app/.venv.tar.gz#.venv" \
    -input "$INPUT_PATH" \
    -output "$HDFS_FINAL_OUTPUT" \
    -mapper "mapper1.py" \
    -reducer "reducer1.py" \
    -cmdenv PYTHONIOENCODING=utf8

echo "MapReduce job completed successfully"

# Step 4: Print out summary
echo "=== Step 4: Summary ==="
echo "MapReduce output stored in HDFS at: $HDFS_FINAL_OUTPUT"
echo "Sample of indexed data:"
hdfs dfs -cat "$HDFS_FINAL_OUTPUT/part-*" | head -n 10

echo "=== Indexing process completed successfully ==="
