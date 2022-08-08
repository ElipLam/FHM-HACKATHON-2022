"""
Extract crawled_data/matches to new_lake/50k_matches.parquet and create check_download_player_id.parquet
"""
import pandas as pd
import json
import time
from pathlib import Path
import glob
from progress.bar import IncrementalBar
from datetime import datetime
import numpy as np
import sys
import requests


ROOT = Path(__file__).resolve().parents[1]
BASE_PATH = Path(__file__).resolve().parents[0]
SUB_FOLDER = "crawled_data/matches"
OUTPUT_FILE = "new_lake/50k_matches.parquet"
CHECK_FILE = "check_download_player_id.parquet"
# add my source
SOUCRE_PATH = str(Path(ROOT, "source"))
sys.path.append(SOUCRE_PATH)
from common import *


def etl_matches():
    columns = [
        "match_id",
        "game_mode",
        "account_id01",
        "hero_id01",
        "isRadiant01",
        "account_id02",
        "hero_id02",
        "isRadiant02",
        "account_id03",
        "hero_id03",
        "isRadiant03",
        "account_id04",
        "hero_id04",
        "isRadiant04",
        "account_id05",
        "hero_id05",
        "isRadiant05",
        "account_id06",
        "hero_id06",
        "isRadiant06",
        "account_id07",
        "hero_id07",
        "isRadiant07",
        "account_id08",
        "hero_id08",
        "isRadiant08",
        "account_id09",
        "hero_id09",
        "isRadiant09",
        "account_id10",
        "hero_id10",
        "isRadiant10",
        "radiant_win",
        "leagueid",
        "region",
        "patch",
        "start_time",
    ]
    matches_path = str(Path(ROOT, SUB_FOLDER))
    # print(matches_path)

    match_list = glob.glob(f"{matches_path}\*\*\*\*.json")
    # for (dir_path, dir_names, file_names) in os.walk(matches_path):
    #     match_list.extend(file_names)
    # print(len(match_list))

    bar = IncrementalBar(
        "Extracting matches and players id...",
        max=len(match_list),
        suffix="%(index)d/%(max)d | %(elapsed_td)s",
    )

    data = {}
    for col in columns:
        data[col] = []
    account_list = []
    now = datetime.now()
    crawl_time = int(np.rint(datetime.timestamp(now)))
    error_files = []
    for file in match_list:
        with open(file, "rb") as f:
            try:
                data_json = json.load(f)
                # ignore the match if this match dont have players.
                if "players" in data_json.keys():
                    players_data = data_json["players"]
                    for i in range(10):
                        num = str(i + 1).zfill(2)
                        if i < len(players_data):
                            data["account_id" + num].append(
                                players_data[i]["account_id"]
                            )
                            data["hero_id" + num].append(players_data[i]["hero_id"])
                            data["isRadiant" + num].append(players_data[i]["isRadiant"])
                            account_list.append(
                                players_data[i]["account_id"]
                            )  # get player id to save
                        else:
                            data["account_id" + num].append(None)
                            data["hero_id" + num].append(None)
                            data["isRadiant" + num].append(None)
                    for col in [
                        "match_id",
                        "game_mode",
                        "radiant_win",
                        "leagueid",
                        "region",
                        "patch",
                        "start_time",
                    ]:
                        if col in data_json.keys():
                            data[col].append(data_json[col])
                        else:
                            data[col].append(None)
                else:
                    bar.next()
                    error_files.append(file)
                    continue
                bar.next()
            except:
                error_files.append(file)
                bar.next()
                continue
    bar.finish()
    account_list1 = set(account_list)
    account_list1.discard(None)
    accounts = [*account_list1]
    # print(len(accounts))
    account_df = pd.DataFrame(accounts, columns=["player_id"])
    account_df["time"] = crawl_time
    account_df["downloaded"] = 0
    print("Error files:", error_files)

    # reset check downloaded of error_files= 0
    out_put = str(Path(ROOT, "check_download_match_id.parquet"))
    change_multi_rows_values_downloaded(
        out_put,
        error_files,
        "match_id",
        0,
        "Reseting check downloaded of error files...",
    )

    match_df = pd.DataFrame(data)
    account_df.to_parquet(CHECK_FILE, index=False)
    match_df.to_parquet(OUTPUT_FILE, index=False)
    print("Done.")


if __name__ == "__main__":
    etl_matches()
    pass
