from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import os
import shutil
import argparse
import json

def parse_args():
    parser = argparse.ArgumentParser(description='Prepare data from parquet file')
    parser.add_argument('--parquet-file', type=str, default='/a.parquet', 
                        help='Path to parquet file in HDFS')
    parser.add_argument('--num-samples', type=int, default=1000, 
                        help='Number of documents to sample')
    parser.add_argument('--sampling-seed', type=int, default=0, 
                        help='Random seed for sampling')
    parser.add_argument('--data-path', type=str, default='data', 
                        help='Local path to store document files')
    parser.add_argument('--index-data-path', type=str, default='/index/data', 
                        help='HDFS path for storing index data')
    return parser.parse_args()

def main():
    args = parse_args()
    
    print(f"Starting data preparation with the following parameters:")
    print(f"  Parquet file: {args.parquet_file}")
    print(f"  Number of samples: {args.num_samples}")
    print(f"  Sampling seed: {args.sampling_seed}")
    print(f"  Data path: {args.data_path}")
    print(f"  Index data path: {args.index_data_path}")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName('data preparation') \
        .master("local") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .getOrCreate()
    
    try:
        # Read and sample parquet file
        print(f"Reading parquet file from {args.parquet_file}")
        df = spark.read.parquet(args.parquet_file)
        total_records = df.count()
        print(f"Total records in parquet file: {total_records}")
        
        # Calculate fraction based on desired sample size
        fraction = min(1.0, 100 * args.num_samples / total_records)
        print(f"Sampling fraction: {fraction}")
        
        sampled_df = df.select(['id', 'title', 'text']) \
                       .sample(fraction=fraction, seed=args.sampling_seed) \
                       .limit(args.num_samples)
        
        actual_samples = sampled_df.count()
        print(f"Actual samples selected: {actual_samples}")
        
        # Create data directory if it doesn't exist, or clean it if it does
        if os.path.exists(args.data_path):
            print(f"Cleaning existing data directory: {args.data_path}")
            shutil.rmtree(args.data_path)
        
        print(f"Creating data directory: {args.data_path}")
        os.makedirs(args.data_path)
        
        # Create document files
        print("Creating document files...")
        def create_doc(row):
            filename = os.path.join(args.data_path, sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt")
            with open(filename, "w", encoding='utf-8') as f:
                f.write(row['text'])
            return filename
        
        sampled_df.foreach(create_doc)
        
        print("Data preparation completed successfully")
        
    except Exception as e:
        print(f"Error during data preparation: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()