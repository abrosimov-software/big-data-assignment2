from cassandra.cluster import Cluster


print("hello app")

# Connects to the cassandra server
cluster = Cluster(['cassandra-server'])

session = cluster.connect()

# Create the keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS bm25_index WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };")

# Use the keyspace
session.execute("USE bm25_index;")

# Drop the tables if they exist
session.execute("DROP TABLE IF EXISTS terms;")
session.execute("DROP TABLE IF EXISTS documents;")
session.execute("DROP TABLE IF EXISTS term_docs;")
session.execute("DROP TABLE IF EXISTS global_stats;")

# Create the tables
session.execute("CREATE TABLE IF NOT EXISTS terms (term text PRIMARY KEY, doc_frequency int);")
session.execute("CREATE TABLE IF NOT EXISTS documents (doc_id text PRIMARY KEY, doc_length int);")
session.execute("CREATE TABLE IF NOT EXISTS term_docs (term text, doc_id text, term_frequency int, PRIMARY KEY (term, doc_id));")
session.execute("CREATE TABLE IF NOT EXISTS global_stats (docs_num int, total_doc_len int, PRIMARY KEY (docs_num, total_doc_len));")


print("Schema created in cassandra server")
session.shutdown()