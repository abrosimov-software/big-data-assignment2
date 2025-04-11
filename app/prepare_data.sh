#!/bin/bash
set -euo pipefail

# Configuration variables
PARQUET_FILE="a.parquet"
NUM_SAMPLES=1000
SAMPLING_SEED=0
LOCAL_DATA_PATH="data"
HDFS_DATA_PATH="/data"
HDFS_INDEX_PATH="/index/data"
METADATA_FILE="data/.metadata"
HDFS_METADATA_FILE="/data/.metadata"

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "Checking for parquet file..."
if [ ! -f "$PARQUET_FILE" ]; then
    echo "Error: Parquet file '$PARQUET_FILE' not found. Please download it first."
    exit 1
fi

# Check if metadata exists in HDFS
NEED_REPROCESS=true
if hdfs dfs -test -e "$HDFS_METADATA_FILE" 2>/dev/null; then
    echo "Metadata found in HDFS, checking if reprocessing is needed..."
    hdfs dfs -get "$HDFS_METADATA_FILE" /tmp/hdfs_metadata 2>/dev/null
    
    if [ -f "/tmp/hdfs_metadata" ]; then
        HDFS_NUM_SAMPLES=$(grep "^NUM_SAMPLES=" /tmp/hdfs_metadata | cut -d= -f2)
        HDFS_SAMPLING_SEED=$(grep "^SAMPLING_SEED=" /tmp/hdfs_metadata | cut -d= -f2)
        
        if [ "$HDFS_NUM_SAMPLES" == "$NUM_SAMPLES" ] && [ "$HDFS_SAMPLING_SEED" == "$SAMPLING_SEED" ]; then
            echo "HDFS data matches current configuration. No reprocessing needed."
            NEED_REPROCESS=false
        else
            echo "Configuration changed. HDFS data will be reprocessed."
        fi
        rm /tmp/hdfs_metadata
    fi
fi

if [ "$NEED_REPROCESS" = true ]; then
    echo "Processing parquet file and preparing data..."
    
    # Upload parquet file to HDFS
    echo "Uploading parquet file to HDFS..."
    hdfs dfs -put -f "$PARQUET_FILE" / 
    
    # Run the PySpark job with parameters
    echo "Running PySpark data preparation job..."
    spark-submit prepare_data.py --parquet-file="/$PARQUET_FILE" --num-samples="$NUM_SAMPLES" \
        --sampling-seed="$SAMPLING_SEED" --data-path="$LOCAL_DATA_PATH" \
        --index-data-path="$HDFS_INDEX_PATH"
    
    # Create metadata file
    echo "Creating metadata file..."
    echo "NUM_SAMPLES=$NUM_SAMPLES" > "$METADATA_FILE"
    echo "SAMPLING_SEED=$SAMPLING_SEED" >> "$METADATA_FILE"
    echo "PROCESSING_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "$METADATA_FILE"
    
    # Upload data and metadata to HDFS
    echo "Uploading data to HDFS..."
    hdfs dfs -rm -r -skipTrash "$HDFS_DATA_PATH" 2>/dev/null || true
    hdfs dfs -put "$LOCAL_DATA_PATH" /
    
    echo "Data preparation complete!"
else
    echo "Using existing data in HDFS."
fi

# Check if index data exists
if hdfs dfs -test -e "$HDFS_INDEX_PATH" 2>/dev/null; then
    echo "Index data already exists at $HDFS_INDEX_PATH"
else
    echo "Creating index data from documents..."
    spark-submit --master yarn prepare_index.py --data-path="$HDFS_DATA_PATH" --index-path="$HDFS_INDEX_PATH"
fi

echo "Verifying data in HDFS..."
hdfs dfs -ls "$HDFS_DATA_PATH" | head -n 5
echo "..."
hdfs dfs -ls "$HDFS_INDEX_PATH" | head -n 5
echo "Data preparation process completed successfully!"