import json
result_json = []  
with open('data/collection/collection.jsonl', 'r') as json_file:
    for line in json_file:
        entry = json.loads(line)
        result_json.append(entry)

file_path = 'data/collection/session_id.jsonl'
with open(file_path, 'r') as json_file:
    loaded_data = json.load(json_file)

for entry in loaded_data[:15]:
    print(entry)
#print(loaded_data)