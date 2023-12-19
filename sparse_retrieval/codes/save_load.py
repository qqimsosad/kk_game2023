import json
from tqdm import tqdm
import pandas as pd

# train_source_df = pd.read_parquet('../datagame-2023/label_train_source.parquet')
#train_target_df = pd.read_parquet('../datagame-2023/label_train_target.parquet')
test_source_df = pd.read_parquet('../datagame-2023/label_test_source.parquet')
id_dicts_list = []

for index, row in tqdm(test_source_df.iterrows(), total=len(test_source_df)):
    session_id = row['session_id']
    song_id = row['song_id']
    id_dict = {"session_id": session_id, "song_id": song_id}
    id_dicts_list.append(id_dict)
#print(id_dict)

json_data = json.dumps(id_dicts_list, indent=2)
output_json_path = 'data/collection/session_id.jsonl'   
with open(output_json_path, 'w') as json_file:
    json_file.write(json_data)


# id_dicts_list = [{"session_id": row['session_id'], "song_id": row['song_id']} for _, row in tqdm(train_target_df.iterrows(), total=len(train_target_df))]
# id_df = pd.DataFrame(id_dicts_list)
# output_parquet_path = 'data/collection/session_id.parquet'
# id_df.to_parquet(output_parquet_path, index=False)
#---------------------------------------------------------------------
# Session ID: 307, Song IDs: ['75c2aa348888f982d85e3f870e6ba5b2', '0cab8863e5440551c7b37e59635ec18e', '4d5aceee5c9731151ca69f0946ffa71f', '929b07d69451684f4f0f6e3bcc2a62d6', '12ae4e616d3e5c7bd53ec771797f596b']
# Session ID: 1504, Song IDs: ['34f1a786e245f2886ab99b0062de906c', 'd8ec0f80ee6b4457f12e74aa469335d6', 'd63dbd5214a39f50100c8d59f1c24d6a', 'c1550c264fb083b3acffe619bd02d75e', '61a3b37f326394081b95196a5eb676b8']



# loaded_data = pd.read_parquet('../datagame-2023/label_train_target.parquet')
# session_song_dict = {}
# for index, row in loaded_data.iterrows():
#     session_id = row['session_id']
#     song_id = row['song_id']
#     if session_id in session_song_dict:
#         session_song_dict[session_id].append(song_id)
#     else:
#         session_song_dict[session_id] = [song_id]
# print(session_song_dict[307])
# print("\nTop 5 entries in session_song_dict:")
# for session_id, song_ids in list(session_song_dict.items())[:5]:
#     print(f"Session ID: {session_id}, Song IDs: {song_ids}")