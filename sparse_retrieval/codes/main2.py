import argparse
from search import *
from query import *
from miss_q import *

result_json = []  
with open('data/collection/collection.jsonl', 'r') as json_file:
    for line in json_file:
        entry = json.loads(line)
        result_json.append(entry)

file_path = 'data/collection/session_id.jsonl'
with open(file_path, 'r') as json_file:
    loaded_data = json.load(json_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", default="indexes/collection", type=str)
    parser.add_argument("--query", default='../datagame-2023/label_test_source.parquet', type=str)
    parser.add_argument("--method", default="bm25", type=str)
    parser.add_argument("--k", default=25, type=int)
    parser.add_argument("--output", default='runs/bm25.run', type=str)
    # 只跑遺漏session
    # parser.add_argument("--output", default='runs/bm25_1.run', type=str)
    
    args = parser.parse_args()

    searcher = LuceneSearcher(args.index)
    if args.method == "bm25":
        searcher.set_bm25(k1=2, b=0.75)
    query = get_query(loaded_data,result_json)
    # 遺漏session
    # query = get_miss_query(loaded_data,result_json)
    search_skip(searcher, query, args)
