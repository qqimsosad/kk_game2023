from pyserini.index import IndexReader
from pyserini.search.lucene import LuceneSearcher
import argparse
from tqdm import tqdm
import pandas as pd

index_path = 'indexes/collection'  # Replace with the actual path to your index
index_reader = IndexReader(index_path)

parser = argparse.ArgumentParser()
parser.add_argument("--index", default="indexes/collection", type=str)
args = parser.parse_args()
searcher = LuceneSearcher(args.index)

term='fffa6006b29b0f5d835d07dcfcad5c5e'
postings_list = index_reader.get_postings_list(term)
for posting in postings_list:
    print(f'docid={posting.docid}, tf={posting.tf}, pos={posting.positions}')