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

stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

def tokenize_and_normalize(text: str) -> List[str]:
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text).strip()
    
    tokens = text.split()

    normalized_tokens = [token for token in tokens if len(token) > 2 and token not in stopwords]
    
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
