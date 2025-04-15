#!/usr/bin/env python3
"""
Mapper for the first MapReduce job in the document indexing pipeline.

This mapper:
1. Reads document data in the format: <doc_id>\t<doc_title>\t<doc_text>
2. Tokenizes and normalizes the combined text (title + content)
3. Counts term frequencies for each document
4. Outputs data in the format: <doc_id>\t<term>\t<term_frequency>\t<doc_length>

The output is then consumed by reducer1.py to calculate BM25 scores.
"""

import sys
import os
import re
from collections import Counter
from typing import List
import zipfile

sys.path.append("./python_env.zip")

try:
    import nltk
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import stopwords

    with zipfile.ZipFile("./python_env.zip", "r") as zip_ref:
        zip_ref.extractall("./python_env")
    nltk.data.path.append("./python_env/nltk_data")
    
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
except Exception as e:
    print(f"Error importing libraries: {str(e)}", file=sys.stderr)
    lemmatizer = None
    stop_words = set()

def tokenize_and_normalize(text: str) -> List[str]:
    """
    Tokenize, normalize, and lemmatize text
    
    Args:
        text: Input text to process
        
    Returns:
        List of normalized tokens
    """
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text).strip()
    
    if 'nltk' in sys.modules:
        tokens = nltk.word_tokenize(text)
    else:
        tokens = text.split()
    
    normalized_tokens = []
    for token in tokens:
        if lemmatizer is not None:
            lemmatized_token = lemmatizer.lemmatize(token)
        else:
            lemmatized_token = token
            
        if lemmatized_token not in stop_words and len(lemmatized_token) > 2:
            normalized_tokens.append(lemmatized_token)
    
    return normalized_tokens

for line in sys.stdin:
    parts = line.strip().split('\t')
    
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    
    full_text = f"{title} {text}"
    
    tokens = tokenize_and_normalize(full_text)

    term_counts = Counter(tokens)

    doc_length = len(tokens)
        
    for term, tf in term_counts.items():
        print(f"{doc_id}\t{term}\t{tf}\t{doc_length}")
