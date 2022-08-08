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
PLAYERS_INFO_PATH = str(Path(ROOT, "crawled_data", "players_info"))
PLAYERS_WL_PATH = str(Path(ROOT, "crawled_data", "players_wl"))
OUTPUT_PATH = str(Path(ROOT, "new_lake/players.parquet"))


def show_mmr_estimate(x):
    data_parsed = ast.literal_eval(x)
    if "estimate" in data_parsed.keys():
        return data_parsed["estimate"]
    else:
        return None


def etl_players():

    # extract
    players_info_columns = [
        "rank_tier",
        "leaderboard_rank",
        "profile",
        "competitive_rank",
        "mmr_estimate",
        "solo_competitive_rank",
    ]
    players_info_flag, players_info_df, players_info_erros_list = json_to_dataframe(
        f"{PLAYERS_INFO_PATH}\*.json",
        players_info_columns,
        "Extracting players info...",
    )
    df_players_info_draw = players_info_df  # debug: skip error files
    if players_info_flag == True:
        # df_players_info_draw = players_info_df
        pass
    else:
        change_multi_rows_values_downloaded(
            str(Path(ROOT, "check_download_player_id.parquet")),
            players_info_erros_list,
            "player_id",
            0,
            "Reseting check dowfload player id of error files",
        )
        # raise Exception("Incomplete players information data.")

    players_wl_columns = ["win", "lose"]
    players_wl_flag, players_wl_df, players_wl_erros_list = json_to_dataframe(
        f"{PLAYERS_WL_PATH}/*.json",
        players_wl_columns,
        "Extracting players win lose...",
    )
    df_players_wl = players_wl_df  # debug: skip error files
    if players_wl_flag == True:
        # df_players_wl = players_wl_df
        pass
    else:
        change_multi_rows_values_downloaded(
            str(Path(ROOT, "check_download_player_id.parquet")),
            players_wl_erros_list,
            "player_id",
            0,
            "Reseting check dowfload player id of error files",
        )
    #     raise Exception("Elip - Incomplete players win lose data.")

    # transform
    df_players_info = df_players_info_draw.drop(
        ["leaderboard_rank", "solo_competitive_rank", "competitive_rank"],
        axis=1,
    )
    # df_players_info["rank_tier"] = df_players_info["rank_tier"].astype(
    #     "Int32",
    #     # errors="ignore",
    # )
    df_players_info.dropna(subset=["profile"], inplace=True)

    start_time = time.time()
    df_players_info["profile"] = df_players_info["profile"].astype(str)
    df_players_info["profile"] = df_players_info["profile"].transform(
        lambda x: ast.literal_eval(x)["account_id"]
    )
    df_players_info.rename(columns={"profile": "account_id"}, inplace=True)
    df_players_info["account_id"] = df_players_info["account_id"].astype("Int64")

    df_players_info["mmr_estimate"] = df_players_info["mmr_estimate"].astype(str)
    df_players_info["mmr_estimate"] = df_players_info["mmr_estimate"].transform(
        show_mmr_estimate
    )
    end_time = time.time()
    print("Time expose values", (end_time - start_time))
    df_players_wl = df_players_wl.reset_index()
    df_players_wl.rename(columns={"index": "player_id"}, inplace=True)
    df_players_wl["player_id"] = df_players_wl["player_id"].astype(int)
    # df_players_wl["Winrate"] = df_players_wl["win"] / (
    #     df_players_wl["win"] + df_players_wl["lose"]
    # )

    df_players = df_players_info.merge(
        df_players_wl,
        how="outer",
        left_on="account_id",
        right_on="player_id",
    )
    df_players.drop(columns=["account_id"], inplace=True)
    df_players.rename(
        columns={
            "player_id": "PlayerId",
            "win": "Win",
            "lose": "Lose",
            "rank_tier": "Rank",
            "mmr_estimate": "MmrEstimate",
        },
        inplace=True,
    )
    print(df_players.head())
    print(df_players.info())
    # load
    df_players.to_parquet(OUTPUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    etl_players()
    pass

# -------------------------------------------
#  player_info.csv :: rank_tier,leaderboard_rank,profile[str],competitive_rank,mmr_estimate,solo_competitive_rank

#  player_wl.csv :: player_id,win,lose

# player_heros.csv :: win,games,with_games,hero_id,against_games,last_played,against_win,with_win

# player_heros table:: PlayerId: , HeroId:, Profiency:, TimePlay:, Winrate:, WinrateTeam:, WinrateAnemy:

# player table :: player_id: player_info.csv(profile['account_id']) - win: player_wl.csv(win) - lose: player_wl.csv(lose) - winrate: win/(win+lose) - rank:player_info.csv(rank_tier) - mms_estimate: player_info.csv(mmr_estimate)
