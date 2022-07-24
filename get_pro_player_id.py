"""
Get pro player id from crawled_data/pro_player.json to save to check_download_pro_players_matches.csv
"""


import json
import pandas as pd

INPUT_FILE_PATH = "crawled_data/pro_player.json"
OUTPUT_FILE_PATH = "check_download_pro_players_matches.csv"

f = open(INPUT_FILE_PATH, encoding="utf8")
data = json.load(f)
lis = []
for dat in data:
    lis.append(dat["account_id"])
print("Have", len(lis), "pro players.")
df = pd.DataFrame(lis, columns=["pro_player_id"])
df.sort_values("pro_player_id", inplace=True)
df["downloaded"] = 0

# create check download pro player matches id, default downloaded = 0
df.to_csv(OUTPUT_FILE_PATH, index=False)
print(df.head())
