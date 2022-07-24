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


def show_mmr_estimate(x):
    data_parsed = ast.literal_eval(x)
    if "estimate" in data_parsed.keys():
        return data_parsed["estimate"]
    else:
        return None


df_play_info_draw = pd.read_csv(
    "lake/players_info.csv", encoding="utf-8"
)  # 'Body' is a key word
df_play_info = df_play_info_draw.drop(
    ["leaderboard_rank", "solo_competitive_rank", "competitive_rank"],
    axis=1,
)
df_play_info["rank_tier"] = df_play_info["rank_tier"].astype(
    "Int32",
    # errors="ignore",
)
# print(df_play_info["profile"].isna().sum())
df_play_info.dropna(subset=["profile"], inplace=True)
# print(df_play_info["mmr_estimate"].isna().sum())


start_time = time.time()
df_play_info["profile"] = df_play_info["profile"].transform(
    lambda x: ast.literal_eval(x)["account_id"]
)
df_play_info.rename(columns={"profile": "account_id"}, inplace=True)
df_play_info["account_id"] = df_play_info["account_id"].astype("Int64")

df_play_info["mmr_estimate"] = df_play_info["mmr_estimate"].transform(show_mmr_estimate)
end_time = time.time()
print("Time expose values", (end_time - start_time))

df_player_wl = pd.read_csv("lake/player_wl.csv", encoding="utf8")
df_player_wl["Winrate"] = df_player_wl["win"] / (
    df_player_wl["win"] + df_player_wl["lose"]
)
df_player_wl.drop(columns=["Unnamed: 0"], inplace=True)

df_player = df_play_info.merge(
    df_player_wl,
    how="outer",
    left_on="account_id",
    right_on="player_id",
)
df_player.drop(columns=["account_id"], inplace=True)
df_player.rename(
    columns={
        "player_id": "PlayerId",
        "win": "Win",
        "lose": "Lose",
        "rank_tier": "Rank",
        "mmr_estimate": "MmrEstimate",
    },
    inplace=True,
)
print(df_player.head())
print(df_player.info())
df_player.to_csv("new_lake/players.csv", index=False)

# -------------------------------------------
#  player_info.csv :: rank_tier,leaderboard_rank,profile[str],competitive_rank,mmr_estimate,solo_competitive_rank

#  player_wl.csv :: player_id,win,lose

# player_heros.csv :: win,games,with_games,hero_id,against_games,last_played,against_win,with_win

# player_heros table:: PlayerId: , HeroId:, Profiency:, TimePlay:, Winrate:, WinrateTeam:, WinrateAnemy:

# player table :: player_id: player_info.csv(profile['account_id']) - win: player_wl.csv(win) - lose: player_wl.csv(lose) - winrate: win/(win+lose) - rank:player_info.csv(rank_tier) - mms_estimate: player_info.csv(mmr_estimate)
