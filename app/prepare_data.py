from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import os
import shutil
from config import *
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Prepare data from parquet file')
    parser.add_argument('--source-file', type=str, default=SOURCE_FILE,
                        help='Path to parquet file in HDFS')
    parser.add_argument('--num-samples', type=int, default=NUM_SAMPLES,
                        help='Number of documents to sample')
    parser.add_argument('--sampling-seed', type=int, default=SAMPLING_SEED,
                        help='Random seed for sampling')
    parser.add_argument('--data-path', type=str, default=LOCAL_DATA,
                        help='Local path to store document files')
    parser.add_argument("--index-path", type=str, default=HDFS_INDEX,
                        help="HDFS path for storing index data")
    return parser.parse_args()

def create_doc(row, data_path):
    filename = os.path.join(data_path, sanitize_filename(f"{row['id']}_{row['title'].replace(' ', '_')}.txt"))
    with open(filename, "w", encoding='utf-8') as f:
        f.write(row['text'])

def partitioned_create_doc(partition_iter, data_path):
    for row in partition_iter:
        create_doc(row, data_path)


def main():
    args = parse_args()
    
    print(f"Starting data preparation with the following parameters:")
    print(f"  Source file: {args.source_file}")
    print(f"  Number of samples: {args.num_samples}")
    print(f"  Sampling seed: {args.sampling_seed}")
    print(f"  Data path: {args.data_path}")
    print(f"  Index path: {args.index_path}")
    
    # Create Spark session
    spark = (SparkSession.builder \
        .appName(SPARK_PREPARE_DATA_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.parquet.enableVectorizedReader", SPARK_SQL_VECTORIZED_READER_ENABLED) \
        .config("spark.sql.parquet.filterPushdown", SPARK_SQL_PARQUET_FILTER_PUSHDOWN_ENABLED) \
        .config("spark.sql.adaptive.enabled", SPARK_SQL_ADAPTIVE_EXECUTION_ENABLED) \
        .config("spark.sql.parquet.columnarReaderBatchSize", SPARK_COLUMNAR_READER_BATCH_SIZE) \
        .config("spark.dynamicAllocation.enabled", SPARK_DYNAMIC_ALLOCATION_ENABLED) \
        .config("spark.dynamicAllocation.minExecutors", SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS) \
        .config("spark.dynamicAllocation.maxExecutors", SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS) \
        .config("spark.dynamicAllocation.executorIdleTimeout", SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .getOrCreate())
    
    try:
        if os.path.exists(args.data_path):
            print(f"Removing existing data path: {args.data_path}")
            shutil.rmtree(args.data_path)

        print(f"Creating data path: {args.data_path}")
        os.makedirs(args.data_path)
           
        # Sample the data
        print("Sampling and processing data...")
        sampled_df = spark.read.parquet(args.source_file) \
            .select(*DATA_COLUMNS) \
            .orderBy(rand(seed=args.sampling_seed)) \
            .limit(args.num_samples) \
            .cache()

        print(f"Sampled {sampled_df.count()} rows")

        print("Creating documents...")
        sampled_df.foreachPartition(lambda partition_iter: partitioned_create_doc(partition_iter, args.data_path))

        print("Documents created successfully")

        print(f"Creating index at: {args.index_path}")

        if os.path.exists(args.index_path):
            print(f"Removing existing index path: {args.index_path}")
            shutil.rmtree(args.index_path)

        print(f"Creating index path: {args.index_path}")
        os.makedirs(args.index_path)
        
        # Convert DataFrame to RDD and format
        index_rdd = sampled_df.rdd.map(lambda row: f"{row['id']}\t{row['title']}\t{row['text']}")
        
        # Save as a single partition text file
        index_rdd.saveAsTextFile(args.index_path)
        
        print(f"Index created successfully at {args.index_path}")
    except Exception as e:
        print(f"Error during data preparation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.catalog.clearCache()
        print("Stopping Spark session...")
        spark.stop()
    

if __name__ == "__main__":
    main()