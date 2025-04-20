CREATE KEYSPACE IF NOT EXISTS bm25_index WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

USE bm25_index;

DROP TABLE IF EXISTS terms;
DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS term_docs;
DROP TABLE IF EXISTS global_stats;

CREATE TABLE IF NOT EXISTS terms (
    term text PRIMARY KEY,
    doc_frequency int
);

CREATE TABLE IF NOT EXISTS documents (
    doc_id text PRIMARY KEY,
    doc_length int
);

CREATE TABLE IF NOT EXISTS term_docs (
    term text,
    doc_id text,
    term_frequency int,
    PRIMARY KEY (term, doc_id)
);

CREATE TABLE IF NOT EXISTS global_stats (
    docs_num int,
    total_doc_len int,
    PRIMARY KEY (docs_num, total_doc_len)
);
