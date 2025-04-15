"""
Cassandra Schema for BM25 Index

This module defines the Cassandra tables needed for the BM25 search index:
1. terms_table: Stores vocabulary information (term, document frequency)
2. docs_table: Stores document metadata (id, title, length)
3. term_docs_table: Stores term-document relationships with BM25 scores
4. global_stats: Stores global corpus statistics needed for BM25 calculation
"""

from cassandra.cluster import Cluster
from typing import List, Optional
import logging
from config import (
    CASSANDRA_HOSTS, 
    CASSANDRA_PORT, 
    CASSANDRA_KEYSPACE,
    CASSANDRA_REPLICATION_STRATEGY,
    CASSANDRA_REPLICATION_FACTOR
)

logger = logging.getLogger(__name__)

def create_keyspace(session, keyspace: str, 
                   replication_strategy: str = "SimpleStrategy", 
                   replication_factor: int = 1) -> None:
    """
    Creates a keyspace if it doesn't exist
    
    Args:
        session: Cassandra session
        keyspace: Name of the keyspace
        replication_strategy: Replication strategy to use
        replication_factor: Replication factor
    """
    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH REPLICATION = {{ 
        'class': '{replication_strategy}', 
        'replication_factor': {replication_factor} 
    }}
    """
    session.execute(create_keyspace_query)
    logger.info(f"Keyspace {keyspace} created or already exists")

def create_tables(session) -> None:
    """
    Creates all necessary tables for the BM25 index
    
    Args:
        session: Cassandra session connected to the keyspace
    """
    # Terms table - stores vocabulary information
    create_terms_table = """
    CREATE TABLE IF NOT EXISTS terms (
        term text PRIMARY KEY,
        doc_frequency int,
        idf double
    )
    """
    
    # Documents table - stores document metadata
    create_docs_table = """
    CREATE TABLE IF NOT EXISTS documents (
        doc_id text PRIMARY KEY,
        title text,
        doc_length int
    )
    """
    
    # Term-Document relationship table - stores BM25 scores
    create_term_docs_table = """
    CREATE TABLE IF NOT EXISTS term_docs (
        term text,
        doc_id text,
        term_frequency int,
        bm25_score double,
        PRIMARY KEY (term, doc_id)
    )
    """
    
    # Global statistics table - stores corpus-wide statistics
    create_global_stats_table = """
    CREATE TABLE IF NOT EXISTS global_stats (
        stat_name text PRIMARY KEY,
        stat_value double
    )
    """
    
    tables = [
        create_terms_table,
        create_docs_table, 
        create_term_docs_table,
        create_global_stats_table
    ]
    
    for table_query in tables:
        session.execute(table_query)
        
    logger.info("All required tables have been created")

def initialize_schema(hosts: List[str] = CASSANDRA_HOSTS, 
                     port: int = CASSANDRA_PORT,
                     keyspace: str = CASSANDRA_KEYSPACE,
                     replication_strategy: str = CASSANDRA_REPLICATION_STRATEGY,
                     replication_factor: int = CASSANDRA_REPLICATION_FACTOR) -> Optional[Cluster]:
    """
    Initializes the Cassandra schema by creating keyspace and tables
    
    Args:
        hosts: List of Cassandra hosts
        port: Cassandra port
        keyspace: Name of the keyspace
        replication_strategy: Replication strategy to use
        replication_factor: Replication factor
        
    Returns:
        Cluster object if successful, None otherwise
    """
    try:
        # Connect to Cassandra
        cluster = Cluster(hosts, port=port)
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        create_keyspace(session, keyspace, replication_strategy, replication_factor)
        
        # Use the keyspace
        session.set_keyspace(keyspace)
        
        # Create tables
        create_tables(session)
        
        logger.info("Schema initialization complete")
        return cluster
    except Exception as e:
        logger.error(f"Failed to initialize schema: {str(e)}")
        return None

if __name__ == "__main__":
    # When run directly, initialize the schema
    logging.basicConfig(level=logging.INFO)
    cluster = initialize_schema()
    if cluster:
        cluster.shutdown() 