#!/usr/bin/env python3
"""
PySpark application for document retrieval using BM25 ranking.

This script:
1. Reads a user query from stdin
2. Tokenizes and normalizes the query terms
3. Retrieves BM25 scores for each term from Cassandra
4. Aggregates scores by document
5. Returns the top 10 relevant documents with their titles
"""

import os
import sys
import re
from typing import List, Dict, Tuple

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

# Import configuration
from config import (
    CASSANDRA_HOSTS, 
    CASSANDRA_PORT, 
    CASSANDRA_KEYSPACE,
    HDFS_DATA
)

def tokenize_and_normalize(text: str) -> List[str]:
    """
    Tokenize, normalize, and lemmatize text
    
    Args:
        text: Input text to process
        
    Returns:
        List of normalized tokens
    """
    import nltk
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import stopwords
    
    nltk.download('punkt', quiet=True)
    nltk.download('wordnet', quiet=True)
    nltk.download('stopwords', quiet=True)
    
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text).strip()
    
    tokens = nltk.word_tokenize(text)
    
    normalized_tokens = []
    for token in tokens:
        lemmatized_token = lemmatizer.lemmatize(token)
        if lemmatized_token not in stop_words and len(lemmatized_token) > 2:
            normalized_tokens.append(lemmatized_token)
    
    return normalized_tokens

def get_query_terms() -> List[str]:
    """
    Read the query from stdin or command line arguments and tokenize it
    
    Returns:
        List of normalized query terms
    """
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = input("Enter your search query: ")
    
    return tokenize_and_normalize(query)

def main():
    conf = SparkConf().setAppName("BM25 Document Search")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    print("Starting BM25 document search")
    
    query_terms = get_query_terms()
    print(f"Processed query terms: {', '.join(query_terms)}")
    
    if not query_terms:
        print("No valid search terms found. Please try again with a different query.")
        return
    
    try:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        print("Connected to Cassandra cluster")
    except Exception as e:
        print(f"Failed to connect to Cassandra: {str(e)}")
        sys.exit(1)
    
    # Load document titles from HDFS
    print(f"Loading document titles from {HDFS_DATA}")
    doc_titles_rdd = sc.textFile(HDFS_DATA).map(
        lambda line: line.split('\t')
    ).filter(
        lambda parts: len(parts) >= 2
    ).map(
        lambda parts: (parts[0], parts[1])  # (doc_id, title)
    ).collectAsMap()
    
    print(f"Loaded {len(doc_titles_rdd)} document titles")
    
    query_rdd = sc.parallelize(query_terms)
    
    def get_term_scores(term: str) -> List[Tuple[str, float]]:
        """
        Retrieve BM25 scores for a term from Cassandra
        
        Args:
            term: The query term
            
        Returns:
            List of tuples (doc_id, bm25_score)
        """
        rows = session.execute(
            "SELECT doc_id, bm25_score FROM term_docs_table WHERE term = %s", 
            (term,)
        )
        
        return [(row.doc_id, float(row.bm25_score)) for row in rows]
    
    # Map each term to its document scores
    doc_scores_rdd = query_rdd.flatMap(get_term_scores)
    
    # Group by document and sum the scores
    doc_total_scores = doc_scores_rdd.map(
        lambda x: (x[0], x[1])  # (doc_id, score)
    ).reduceByKey(
        lambda a, b: a + b  # Sum scores for the same document
    )
    
    top_docs = doc_total_scores.takeOrdered(10, key=lambda x: -x[1])  # Descending order
    
    print("\nTop 10 relevant documents:")
    print("-" * 80)
    print(f"{'Document ID':<15} | {'Score':<10} | {'Title':<50}")
    print("-" * 80)
    
    for doc_id, score in top_docs:
        title = doc_titles_rdd.get(doc_id, "Unknown Title")
        print(f"{doc_id:<15} | {score:<10.4f} | {title[:50]}")
    
    cluster.shutdown()
    
    sc.stop()

if __name__ == "__main__":
    main()