"""
Extract new_lake/total_files_v2.csv to new_lake/player_match.csv

output_df = pd.DataFrame(
    columns=[
        "match_id",
        "game_mode",
        "account_id",
        "hero_id",
        "isRadiant",
        "radiant_win",
        "leagueid",
        "region",
        "patch",
        "start_time",
    ]
)
"""

import pandas as pd
from progress.bar import IncrementalBar
import time
from datetime import datetime

INPUT_FILENAME = "new_lake/total_files_v2.csv"
OUTPUT_FILENAME = "new_lake/player_match.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Start Time =", current_time)
start_time = time.time()


input_df = pd.read_csv(INPUT_FILENAME, encoding="utf8")
list_df = []


bar = IncrementalBar(
    "Extracting data...",
    max=len(input_df),
    suffix="%(index)d/%(max)d | %(elapsed_td)s",
)
for index, row in input_df.iterrows():

    tudien = {
        "match_id": [row["match_id"] for i in range(10)],
        "game_mode": [row["game_mode"] for i in range(10)],
        "account_id": [
            row["account_id01"],
            row["account_id02"],
            row["account_id03"],
            row["account_id04"],
            row["account_id05"],
            row["account_id06"],
            row["account_id07"],
            row["account_id08"],
            row["account_id09"],
            row["account_id10"],
        ],
        "hero_id": [
            row["hero_id01"],
            row["hero_id02"],
            row["hero_id03"],
            row["hero_id04"],
            row["hero_id05"],
            row["hero_id06"],
            row["hero_id07"],
            row["hero_id08"],
            row["hero_id09"],
            row["hero_id10"],
        ],
        "isRadiant": [
            row["isRadiant01"],
            row["isRadiant02"],
            row["isRadiant03"],
            row["isRadiant04"],
            row["isRadiant05"],
            row["isRadiant06"],
            row["isRadiant07"],
            row["isRadiant08"],
            row["isRadiant09"],
            row["isRadiant10"],
        ],
        "radiant_win": [row["radiant_win"] for i in range(10)],
        "leagueid": [row["leagueid"] for i in range(10)],
        "region": [row["region"] for i in range(10)],
        "patch": [row["patch"] for i in range(10)],
        "start_time": [row["start_time"] for i in range(10)],
    }
    df = pd.DataFrame(tudien)
    list_df.append(df)
    bar.next()
output_df = pd.concat(list_df, ignore_index=True)
output_df["account_id"] = output_df["account_id"].astype("Int64")
bar.finish()
output_df.to_csv(OUTPUT_FILENAME, index=False)
print(output_df.head())


end_time = time.time()
elapsed = end_time - start_time
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("End Time =", current_time)
print("Elapsed =", time.strftime("%H:%M:%S", time.gmtime(elapsed)))
