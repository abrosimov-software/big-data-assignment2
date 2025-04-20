CREATE KEYSPACE IF NOT EXISTS bm25_index WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

USE bm25_index;

CREATE TABLE IF NOT EXISTS terms (
    term text PRIMARY KEY,
    doc_frequency int,
    idf double
);

CREATE TABLE IF NOT EXISTS documents (
    doc_id text PRIMARY KEY,
    title text,
    doc_length int
);

CREATE TABLE IF NOT EXISTS term_docs (
    term text,
    doc_id text,
    term_frequency int,
    bm25_score double,
    PRIMARY KEY (term, doc_id)
);

CREATE TABLE IF NOT EXISTS global_stats (
    stat_name text PRIMARY KEY,
    stat_value double
);
