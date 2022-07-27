"""
Get match id from crawled_data/players_matches folder to save to check_download_match_id.parquet
"""
import os
from pathlib import Path
import json
import pandas as pd
from progress.bar import IncrementalBar

ROOT = Path("__file__").resolve().parents[1]
BASE_PATH = Path("_file__").resolve().parents[0]
MATCHES_PATH = f"{ROOT}/crawled_data/players_matches"
# OUTPUT_PATH = "check_download_match_id.csv"
OUTPUT_PATH = "check_download_match_id.parquet"

# get list player-matches files by player id
match_id = []
matches_path = os.listdir(MATCHES_PATH)
player_json_files = [match for match in matches_path if match.split(".")[-1] == "json"]
num_player = len(player_json_files)
print("Have", num_player, "players.")

# loop list to get match_id
match_id_set = set()
bar = IncrementalBar("Geting match id...", max=num_player)
for player in player_json_files:
    f = open(str(Path(MATCHES_PATH, player)), encoding="utf-8")
    data = json.load(f)
    # print("PLAYER", player)
    for dat in data:
        match_id_set.add((dat["match_id"], dat["start_time"]))

    f.close()
    bar.next()
bar.finish()
print("Have", len(match_id_set), "matches.")


# save to check_download_match_id.csv
print("Saving to check_download_match_id.parquet...")
df = pd.DataFrame(match_id_set, columns=["match_id", "start_time"])
df.sort_values("match_id", inplace=True)
df["downloaded"] = 0
# df.to_parquet(OUTPUT_PATH, index=False)
print(df.head())
print("Done!")
