from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import os
import shutil

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.parquet.columnarReaderBatchSize", 40) \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", 0) \
    .config("spark.dynamicAllocation.maxExecutors", 6) \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .getOrCreate()

n = 1000
df = spark.read.parquet("/a.parquet") \
    .select(['id', 'title', 'text']) \
    .orderBy(rand(seed=0)) \
    .limit(n) \
    .cache()


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])

def batch_create_docs(df_partition):
    for row in df_partition:
        create_doc(row)

if os.path.exists("data"):
    shutil.rmtree("data")
os.makedirs("data")

df.foreachPartition(batch_create_docs)


if os.path.exists("/index/data"):
    shutil.rmtree("/index/data")

index_rdd = df.rdd.map(lambda row: f"{row['id']}\t{row['title']}\t{row['text']}")
index_rdd.saveAsTextFile("/index/data")
