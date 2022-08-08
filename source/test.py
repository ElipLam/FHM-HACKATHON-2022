import pandas as pd
from datetime import datetime
import numpy as np
import json
import os
import glob

# now = datetime.now()
# print(int(np.rint(datetime.timestamp(now))))
# # CHECK_FILE = "check_download_match_id_.parquet"
# CHECK_FILE = "check_download_pro_players.parquet"

# match_df = pd.read_parquet(CHECK_FILE)

# # print(match_df.head())
# match_df["downloaded"] = 1
a = 5
b = 0
# for i in range(5):
#     try:
#         print(a / i)
#     except:
#         continue

# finally:
# print('hah')
# import pandas as pd
dir_path = "D:\Python\hackathon\crawled_data\matches"

# list to store files
res = []

# for (dir_path, dir_names, file_names) in os.walk(dir_path):
#     res.extend(file_names)
#     print(file_names)
# print(len(res))
match_list = glob.glob(f"{dir_path}\*\*\*\*.json")
print(len(match_list))
# with open("crawled_data/matches/2022/7/10/6654586166.json", "r") as f:
#     data = json.load(f)
#     print(data)

# df = pd.read_json("crawled_data/matches/2022/7/10/6654586166.json")
# df = pd.DataFrame(
#     {
#         "num_legs": [2, 4, 8, 3],
#         "num_wings": [2, 0, 0, 0],
#         "num_specimen_seen": [10, 2, 1, 8],
#     },
#     index=["falcon", "dog", "spider", "fish"],
# )
# df2 = pd.DataFrame(
#     {
#         "num_legs": [2, 4, 8, 3],
#         "num_wings": [2, 0, 5, 3],
#         "num_specimen_seen": [10, 2, 1, 3],
#     },
#     index=["falcon", "dog", "spider", "fish"],
# )
# df_new = pd.concat([df2, df])
# df_new = df_new.drop_duplicates(subset=["num_legs", "num_wings"], keep="last")
# print(df_new)
# print(match_df.head())
# match_df.to_parquet(CHECK_FILE, index=False)

# df = match_df.sort_values("match_id", ascending=False)
# df = df[:8]  # get 50000 last matches
# for i, row in df.iterrows():
#     print(i)
#     print(row)

# print(df.head(10))
# match_df.loc[6659362883]["downloaded"] = 1
# match_df.loc[match_df["match_id"] == 6659362883, "downloaded"] = 1
# print(match_df.loc[match_df["match_id"] == 6659362883])
# print(match_df.loc[match_df["match_id"] == 6659358087]["downloaded"])

"""
dùng 2 ứng dụng AWS để CI/CD
nhận 200 để trả về giá trị pass, kiểm tra phản hồi dữ liệu, độ chính xác của dữ liệu
kiểm soát thời gian chạy của ứng dụng - time out,
quality control
cloud formation
làm sao để chạy app trên môi trường khác?
tách riêng E T L ra, độc lập

GLUE ETL: 
5 job
mỗi job 4 phút -> 20 phút = 0.34 giờ
2 worker
region: virginia - 0.44USD/h
=> 2 DPUs x 0.34 hours x 0.44 USD per DPU-Hour = 0.30 USD (Apache Spark ETL job cost)

Crawler: 
2 crawler(1 new_lake, 1 redshift)
mỗi crawler chạy 4 phút = 0.07 giờ
mỗi crawler dùng 1 DPU

=> 2 crawlers x 0.07 hours x 0.44 USD per DPU-hour = 0.06 USD

Data Catalog storage requests: (million/month)
=> dùng ít nên xem như là bỏ qua chi phí. 

Kết luận: mỗi lần chạy Jobs và Crawler sẽ tốn 0.36 USD

Redshift instances: 1 instance - dc2.large
region: virginia
Thời gian sử dụng: 1h
1 instance(s) x 0.25 USD hourly x 1 = 0.25 USD 
Redshift Data Transfer:
There is no charge for data transferred between Amazon Redshift and Amazon S3 within the same AWS Region for backup, restore, load, and unload operations. 


2 GB x 0.02 Price for data transfers in to = 0.04 USD (Redshift Data Transfer In To cost

"""
