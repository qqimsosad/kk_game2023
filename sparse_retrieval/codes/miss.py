import pandas as pd

# 讀取CSV檔案
out_df = pd.read_csv('../datagame-2023/output.csv')
sample_df = pd.read_csv('../datagame-2023/sample.csv')

# 比較session_id欄位，找出在out.csv中但不在sample.csv中的session_id
missing_session_ids = list(sample_df[~sample_df['session_id'].isin(out_df['session_id'])]['session_id'])

# 顯示結果
print("在sample.csv中但不在out.csv中的session_id列表:")
print(missing_session_ids,len(missing_session_ids))