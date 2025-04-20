#!/usr/bin/env python3
"""
Reducer for database population

This reducer:
1. Reads the output from mapper1.py in format: <doc_id>\t<term>\t<term_frequency>\t<doc_length>
2. Groups data by term
3. Calculates document frequency (df) for each term
4. Stores results in Cassandra tables for: terms, documents, term_docs, and global_stats
5. Outputs data in the format: <term>\t<doc_id>\t<term_frequency>
   (for verification and possible use in subsequent MapReduce jobs)

"""

import sys
import os
import math
from typing import Dict, List, Tuple

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

cluster = Cluster(["cassandra-server"])
session = cluster.connect("bm25_index")

insert_term = session.prepare(
    "INSERT INTO terms (term, doc_frequency) VALUES (?, ?)"
)

insert_document = session.prepare(
    "INSERT INTO documents (doc_id, doc_length) VALUES (?, ?)"
)

insert_term_doc = session.prepare(
    "INSERT INTO term_docs (term, doc_id, term_frequency) VALUES (?, ?, ?)"
)

insert_global_stat = session.prepare(
    "INSERT INTO global_stats (docs_num, total_doc_len) VALUES (?, ?)"
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

    if current_doc_id != doc_id:
        doc_lengths[doc_id] = doc_length
        total_length += doc_length
        current_doc_id = doc_id
    
    terms_info.setdefault(term, []).append((doc_id, tf))

total_docs = len(doc_lengths)

# Store global statistics
batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
batch.add(insert_global_stat, (total_docs, total_length))
session.execute(batch)

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

# Process terms and calculate BM25 scores
term_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
term_doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
term_count = 0
term_doc_count = 0
batch_size = 100

for term, doc_tf in terms_info.items():
    doc_frequency = len(doc_tf)
    
    # Store term information
    term_batch.add(insert_term, (term, doc_frequency))
    term_count += 1
    
    if term_count >= batch_size:
        session.execute(term_batch)
        term_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        term_count = 0

    
    # Process each document for this term
    for doc_id, tf in doc_tf:
        
        # Store term-document relationship
        term_doc_batch.add(insert_term_doc, (term, doc_id, tf))
        term_doc_count += 1
        
        if term_doc_count >= batch_size:
            session.execute(term_doc_batch)
            term_doc_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
            term_doc_count = 0
        
        # Output for verification and possible downstream processing
        print(f"{term}\t{doc_id}\t{tf}")

# Execute any remaining batches
if term_count > 0:
    session.execute(term_batch)

if term_doc_count > 0:
    session.execute(term_doc_batch)

# Close Cassandra connection
cluster.shutdown()

