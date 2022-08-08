"""
Get pro player id from crawled_data/pro_player.json to save to check_download_pro_players_matches.parquet
"""


import json
import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE_PATH = "crawled_data/pro_player.json"
OUTPUT_FILE_PATH = "check_download_pro_players.parquet"


def get_pro_player_id():
    now = datetime.now()
    crawl_time = int(np.rint(datetime.timestamp(now)))
    f = open(INPUT_FILE_PATH, encoding="utf8")
    data = json.load(f)
    lis = []
    for dat in data:
        lis.append(dat["account_id"])
    print("Have", len(lis), "pro players.")
    df = pd.DataFrame(lis, columns=["pro_player_id"])
    df["downloaded"] = 0
    df["time"] = crawl_time
    if os.path.isfile(OUTPUT_FILE_PATH):
        old_df = pd.read_parquet(OUTPUT_FILE_PATH)
        df = pd.concat([old_df, df])
        df.drop_duplicates(subset=["pro_player_id"], keep="last")

    df.sort_values("pro_player_id", inplace=True)

    # create check download pro player matches id, default downloaded = 0
    df.to_parquet(OUTPUT_FILE_PATH, index=False)
    print(df.head())


if __name__ == "__main__":
    get_pro_player_id()
    pass
