### Data Source Configuration ###
SOURCE_FILE = "/a.parquet"

### Sampling Configuration ###
NUM_SAMPLES = 1000
SAMPLING_SEED = 0

### Path Configuration ###
LOCAL_DATA = "data"
HDFS_DATA = "/data"
HDFS_INDEX = "/index/data"
HDFS_FINAL_OUTPUT = "/tmp/index/final"

### Processing Configuration ###
DATA_COLUMNS = ['id', 'title', 'text']  # Columns to extract from parquet

### Cassandra Configuration ###
CASSANDRA_HOSTS = ['cassandra-server']
CASSANDRA_KEYSPACE = 'bm25_index'

# Cassandra Tables

### MapReduce Configuration ###
# BM25 parameters
BM25_K1 = 1.0  # Controls term frequency scaling
BM25_B = 0.75  # Controls document length normalization


### Spark Configuration ###
# Application settings
SPARK_PREPARE_DATA_APP_NAME = "data preparation"
SPARK_PREPARE_INDEX_APP_NAME = "index preparation"
SPARK_MASTER = "local"

# Performance settings
SPARK_DRIVER_MEMORY = "4g"
SPARK_COLUMNAR_READER_BATCH_SIZE = 100
SPARK_DYNAMIC_ALLOCATION_ENABLED = True
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = 0
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = 6
SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT = "60s"

# Optimization flags
SPARK_SQL_VECTORIZED_READER_ENABLED = "true"
SPARK_SQL_PARQUET_FILTER_PUSHDOWN_ENABLED = "true"
SPARK_SQL_ADAPTIVE_EXECUTION_ENABLED = "true"
