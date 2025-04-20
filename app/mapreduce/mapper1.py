#!/usr/bin/env python3
"""
Mapper for the first MapReduce job in the document indexing pipeline.

This mapper:
1. Reads document data in the format: <doc_id>\t<doc_title>\t<doc_text>
2. Tokenizes and normalizes the combined text (title + content)
3. Counts term frequencies for each document
4. Outputs data in the format: <doc_id>\t<term>\t<term_frequency>\t<doc_length>
"""

import os
import sys
import re
from collections import Counter
from typing import List

stop_words = {"a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with"}

def tokenize_and_normalize(text: str) -> List[str]:
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text).strip()
    
    tokens = text.split()
    
    normalized_tokens = []
    for token in tokens:
        if token not in stop_words and len(token) > 2:
            normalized_tokens.append(token)
    
    return normalized_tokens

# Process each input line
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
