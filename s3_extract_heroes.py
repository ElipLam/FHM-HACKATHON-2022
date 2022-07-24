"""
Extract raw lake/heroes.csv to new_lake/eroes.csv
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


df_heroes_raw = pd.read_csv("lake/heroes.csv")
df_heroes = df_heroes_raw[["id", "name", "pro_win", "pro_pick"]]
df_heroes.loc[:, "name"] = df_heroes["name"].apply(
    lambda name: " ".join(name.split("_")[3:])
)
# df_heros["WinrateAll"] = df_heros["pro_win"].divide(df_heros["pro_pick"])
# df_heros["WinrateAll"].fillna(0, inplace=True)
df_heroes.rename(
    columns={
        "id": "HeroId",
        "name": "HeroName",
        "pro_win": "ProWin",
        "pro_pick": "ProPick",
    },
    inplace=True,
)
print(df_heroes.tail())
df_heroes.to_csv("new_lake/heroes.csv", index=False)

# heros.csv: id,name,localized_name,primary_attr,attack_type,roles,img,icon,base_health,base_health_regen,base_mana,base_mana_regen,base_armor,base_mr,base_attack_min,base_attack_max,base_str,base_agi,base_int,str_gain,agi_gain,int_gain,attack_range,projectile_speed,attack_rate,move_speed,turn_rate,cm_enabled,legs,hero_id,turbo_picks,turbo_wins,pro_ban,pro_win,pro_pick,1_pick,1_win,2_pick,2_win,3_pick,3_win,4_pick,4_win,5_pick,5_win,6_pick,6_win,7_pick,7_win,8_pick,8_win,null_pick,null_win

#  hero table :: hero_id:heros.csv(id) - hero_name:heros.csv(name) - winrateAll:heros.csv(pro_wins)/heros.csv(pro_pick)
