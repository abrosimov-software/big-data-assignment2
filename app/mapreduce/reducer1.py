
#!/usr/bin/env python3
"""
Reducer for BM25 score calculation

This reducer:
1. Reads the output from mapper1.py in format: <doc_id>\t<term>\t<term_frequency>\t<doc_length>
2. Groups data by term
3. Calculates document frequency (df) for each term
4. Calculates BM25 scores for all term-document pairs
5. Stores results in Cassandra tables for: terms, documents, term_docs, and global_stats
6. Outputs data in the format: <term>\t<doc_id>\t<bm25_score>
   (for verification and possible use in subsequent MapReduce jobs)

BM25 Formula:
BM25(q,d) = log(N/df(t)) * ((k1+1) * tf(t,d)) / (k1 * ((1-b) + b * (dl(d)/dlavg)) + tf(t,d))
"""

import sys
import os
import math
import logging
from typing import Dict, List, Tuple

sys.path.append("./python_env.zip")

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

from config import (
    CASSANDRA_HOSTS,
    CASSANDRA_PORT,
    CASSANDRA_KEYSPACE,
    BM25_K1,
    BM25_B
)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    logger.info("Connected to Cassandra cluster")
except Exception as e:
    logger.error(f"Failed to connect to Cassandra: {str(e)}")
    sys.exit(1)

insert_term = session.prepare(
    "INSERT INTO terms (term, doc_frequency, idf) VALUES (?, ?, ?)"
)

insert_document = session.prepare(
    "INSERT INTO documents (doc_id, doc_length) VALUES (?, ?)"
)

insert_term_doc = session.prepare(
    "INSERT INTO term_docs (term, doc_id, term_frequency, bm25_score) VALUES (?, ?, ?, ?)"
)

insert_global_stat = session.prepare(
    "INSERT INTO global_stats (stat_name, stat_value) VALUES (?, ?)"
)

# Data structures to hold information for processing
doc_lengths: Dict[str, int] = {}  # doc_id -> document length
terms_info: Dict[str, List[Tuple[str, int]]] = {} # term -> [(doc_id, term_frequency)]
current_doc_id = None
total_length = 0

# Process input data from mapper
for line in sys.stdin:
    parts = line.strip().split('\t')
    
    if len(parts) != 4:
        continue
        
    doc_id, term, tf, doc_length = parts

    tf = int(tf)
    doc_length = int(doc_length)

    if current_doc_id == doc_id:
        terms_info.setdefault(term, []).append((doc_id, tf))
    else:
        doc_lengths[doc_id] = doc_length
        total_length += doc_length
        current_doc_id = doc_id
        terms_info.setdefault(term, []).append((doc_id, tf))

total_docs = len(doc_lengths)
avg_length = total_length / total_docs if total_docs > 0 else 0

logger.info(f"Processed {total_docs} documents with {len(terms_info)} unique terms")

# Store global statistics
batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
batch.add(insert_global_stat, ("total_docs", float(total_docs)))
batch.add(insert_global_stat, ("avg_doc_length", avg_length))
session.execute(batch)
logger.info("Stored global statistics in Cassandra")

# Store document information
doc_batch_size = 100
doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
doc_count = 0

for doc_id, doc_length in doc_lengths.items():
    doc_batch.add(insert_document, (doc_id, doc_length))
    doc_count += 1
    
    if doc_count >= doc_batch_size:
        session.execute(doc_batch)
        doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        doc_count = 0

if doc_count > 0:
    session.execute(doc_batch)

logger.info(f"Stored {len(doc_lengths)} documents in Cassandra")

# Process terms and calculate BM25 scores
term_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
term_doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
term_count = 0
term_doc_count = 0
batch_size = 100

for term, doc_tf_map in terms_info.items():
    doc_frequency = len(doc_tf_map)
    
    # Calculate IDF for the term
    idf = math.log(total_docs / doc_frequency) if doc_frequency > 0 else 0
    
    # Store term information
    term_batch.add(insert_term, (term, doc_frequency, idf))
    term_count += 1
    
    if term_count >= batch_size:
        session.execute(term_batch)
        term_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        term_count = 0
    
    # Process each document for this term
    for doc_id, tf in doc_tf_map.items():
        doc_length = doc_lengths.get(doc_id, avg_length)
        
        # Calculate BM25 score
        numerator = (BM25_K1 + 1) * tf
        denominator = tf + BM25_K1 * (1 - BM25_B + BM25_B * doc_length / avg_length)
        bm25_score = idf * (numerator / denominator) if denominator > 0 else 0
        
        # Store term-document relationship
        term_doc_batch.add(insert_term_doc, (term, doc_id, tf, bm25_score))
        term_doc_count += 1
        
        if term_doc_count >= batch_size:
            session.execute(term_doc_batch)
            term_doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
            term_doc_count = 0
        
        # Output for verification and possible downstream processing
        print(f"{term}\t{doc_id}\t{bm25_score}")

# Execute any remaining batches
if term_count > 0:
    session.execute(term_batch)

if term_doc_count > 0:
    session.execute(term_doc_batch)

logger.info(f"Stored term and term-document relationships in Cassandra")

# Close Cassandra connection
cluster.shutdown()
logger.info("MapReduce job completed successfully")

