import json
from tqdm import tqdm
import pandas as pd

def get_miss_query(loaded_data, result_json):
    out_df = pd.read_csv('../datagame-2023/output.csv')
    sample_df = pd.read_csv('../datagame-2023/sample.csv')
    missing_session_ids = list(sample_df[~sample_df['session_id'].isin(out_df['session_id'])]['session_id'])
    
    df_loaded_data = pd.DataFrame(loaded_data)
    df_result_json = pd.DataFrame(result_json)

    merged_df = pd.merge(df_loaded_data, df_result_json, left_on='song_id', right_on='id', how='left')
    merged_df = merged_df.drop(['id', 'song_id'], axis=1)
    updated_data = merged_df.to_dict(orient='records')
    df_updated_data = pd.DataFrame(updated_data)
    grouped_data = df_updated_data.groupby('session_id')['contents'].apply(lambda x: ' '.join(x)).reset_index(name='contents')
    final_result = pd.merge(df_loaded_data[['session_id']], grouped_data, on='session_id', how='left')

    # 過濾只包含缺失 session_id 的資料
    final_result_filtered = final_result[final_result['session_id'].isin(missing_session_ids)]
    #print(final_result_filtered)
    # print(final_result_filtered.head(5))
    query = final_result_filtered.set_index('session_id')['contents'].to_dict()

    return query
