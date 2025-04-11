from pyspark.sql import SparkSession
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Prepare index data from documents')
    parser.add_argument('--data-path', type=str, default='/data', 
                        help='HDFS path where document files are stored')
    parser.add_argument('--index-path', type=str, default='/index/data', 
                        help='HDFS path for storing index data')
    return parser.parse_args()

def main():
    args = parse_args()
    
    print(f"Starting index data preparation with the following parameters:")
    print(f"  Data path: {args.data_path}")
    print(f"  Index path: {args.index_path}")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName('index data preparation') \
        .getOrCreate()
    
    try:
        # Read all documents from the data path
        # Each file in HDFS has format: <doc_id>_<doc_title>.txt
        documents_rdd = spark.sparkContext.wholeTextFiles(f"{args.data_path}/*.txt")
        
        print(f"Loaded {documents_rdd.count()} documents")
        
        # Transform to required format: doc_id, doc_title, doc_text
        processed_rdd = documents_rdd.map(lambda x: (
            x[0].split('/')[-1].split('_')[0],                # Extract doc_id
            '_'.join(x[0].split('/')[-1].split('_')[1:]).split('.')[0],  # Extract doc_title
            x[1]                                             # doc_text
        )).map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}")
        
        # Save as single file to /index/data
        processed_rdd.coalesce(1).saveAsTextFile(args.index_path)
        
        print(f"Successfully saved index data to {args.index_path}")
        
    except Exception as e:
        print(f"Error during index data preparation: {e}")
        spark.stop()
        exit(1)
        
    print("Index data preparation completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()