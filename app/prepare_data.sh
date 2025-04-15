#!/bin/bash

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# Load variables from config.py
echo "Loading configuration variables from config.py"
SOURCE_FILE=$(python -c 'from config import SOURCE_FILE; print(SOURCE_FILE.lstrip("/"))')
HDFS_DATA=$(python -c 'from config import HDFS_DATA; print(HDFS_DATA)')
HDFS_INDEX=$(python -c 'from config import HDFS_INDEX; print(HDFS_INDEX)')

echo "Source file: $SOURCE_FILE"
echo "HDFS data path: $HDFS_DATA"
echo "HDFS index path: $HDFS_INDEX"

# Check if source parquet file exists locally
if [ ! -f "$SOURCE_FILE" ]; then
    echo "Error: Source file $SOURCE_FILE does not exist locally."
    exit 1
fi

# Check and clean HDFS /data directory if it exists
echo "Checking if HDFS data directory exists..."
if hdfs dfs -test -d "$HDFS_DATA" 2>/dev/null; then
    echo "Cleaning up existing HDFS data directory: $HDFS_DATA"
    hdfs dfs -rm -r -skipTrash "$HDFS_DATA"
fi

# Check and clean HDFS index directory if it exists
echo "Checking if HDFS index directory exists..."
if hdfs dfs -test -d "$HDFS_INDEX" 2>/dev/null; then
    echo "Cleaning up existing HDFS index directory: $HDFS_INDEX"
    hdfs dfs -rm -r -skipTrash "$HDFS_INDEX"
fi

# Upload the parquet file to HDFS
echo "Uploading source file to HDFS..."
hdfs dfs -put -f "$SOURCE_FILE" / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls "$HDFS_DATA" && \
    hdfs dfs -ls "$HDFS_INDEX" && \
    echo "done data preparation!"