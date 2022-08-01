"""
Extract s3://huongami-s3-demo/hackathon/[players_info.csv, player_wl.csv] to new_lake/players.csv
"""

import pandas as pd
import requests
import json
import time
from progress.bar import IncrementalBar
import boto3
import json
import ast
import time
import sys
from pathlib import Path
import glob

# my module
from common import *

ROOT = Path(__file__).resolve().parents[1]
PLAYERS_HEROES_PATH = str(Path(ROOT, "crawled_data", "players_heroes"))
OUTPUT_PATH = str(Path(ROOT, "new_lake/players_heroes.parquet"))


# def show_mmr_estimate(x):
#     data_parsed = ast.literal_eval(x)
#     if "estimate" in data_parsed.keys():
#         return data_parsed["estimate"]
#     else:
#         return None
def players_heroes_json_to_dataframe(input_path, columns, message):
    """Return a pandas dataframe and list of error file path."""
    error_files = []
    data_dict = {col: [] for col in columns}
    index = []
    path_list = glob.glob(input_path)
    bar = IncrementalBar(
        message,
        max=len(path_list),
        suffix="%(index)d/%(max)d | %(elapsed_td)s",
    )
    for json_path in path_list:
        try:
            idx, _ = get_filename_extension(json_path)
            with open(json_path, "rb") as f:
                data = json.load(f)
                # print(data)
                # print(len(data))
                for player_hero in data:
                    for col in columns:
                        data_dict[col].append(player_hero[col])
                    index.append(idx)
        except Exception as e:
            error_files.append(json_path)
        finally:
            bar.next()
    df = pd.DataFrame(data_dict, index=index)
    if error_files == []:
        flag = True
    else:
        flag = False
    return flag, df, error_files


def etl_players_heroes():

    # extract
    players_heroes_columns = [
        "hero_id",
        "with_win",
        "win",
        "against_games",
        "last_played",
        "against_win",
        "games",
        "with_games",
    ]
    (
        players_heroes_flag,
        players_heroes_df,
        players_heroes_erros_list,
    ) = players_heroes_json_to_dataframe(
        f"{PLAYERS_HEROES_PATH}\*.json",
        players_heroes_columns,
        "Extracting players heroes...",
    )
    df_players_heroes = players_heroes_df  # debug: skip error files
    df_players_heroes = df_players_heroes.reset_index()  # debug: skip error files
    df_players_heroes.rename(columns={"index": "player_id"}, inplace=True)
    if players_info_flag == True:
        # df_players_info_draw = players_info_df
        pass
    else:
        change_multi_rows_values_downloaded(
            str(Path(ROOT, "check_download_player_id.parquet")),
            players_heroes_erros_list,
            "player_id",
            0,
            "Reseting check dowfload player id of error files",
        )
    #     raise Exception("Incomplete players information data.")

    df_players_heroes.to_parquet(OUTPUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    etl_players_heroes()
    pass
