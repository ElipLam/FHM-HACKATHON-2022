import pandas as pd
from pathlib import Path
import requests
from progress.bar import IncrementalBar
import sys
from common import *
import time

ROOT = Path(__file__).resolve().parents[1]
BASE = Path(__file__).resolve().parents[0]
PLAYERS_INFO_FOLDER = str(Path(ROOT, "crawled_data/players_info"))
PLAYERS_WL_FOLDER = str(Path(ROOT, "crawled_data/players_wl"))
PLAYERS_HEROES_FOLDER = str(Path(ROOT, "crawled_data/players_heroes"))
CHECK_FILE = str(Path(ROOT, "check_download_player_id.parquet"))
# add my source
# SOUCRE_PATH = str(Path(ROOT, "source"))
# sys.path.append(SOUCRE_PATH)


def crawl_all_players():
    df = pd.read_parquet(CHECK_FILE)
    new_df = df[df["downloaded"] == 0]
    bar = IncrementalBar(
        "Downloading players...",
        max=len(new_df),
        suffix="%(index)d/%(max)d | %(elapsed_td)s",
    )
    try:
        for index, row in new_df.iterrows():
            player_id = row["player_id"]
            player_info_url = f"https://api.opendota.com/api/players/{player_id}"
            player_wl_url = f"https://api.opendota.com/api/players/{player_id}/wl"
            player_heroes_url = (
                f"https://api.opendota.com/api/players/{player_id}/heroes"
            )
            player_info_output = f"{PLAYERS_INFO_FOLDER}/{player_id}.json"
            player_wl_output = f"{PLAYERS_WL_FOLDER}/{player_id}.json"
            player_heroes_output = f"{PLAYERS_HEROES_FOLDER}/{player_id}.json"
            crawl_data(player_info_url, player_info_output)
            crawl_data(player_wl_url, player_wl_output)
            crawl_data(player_heroes_url, player_heroes_output)
            df.loc[index, "downloaded"] = 1
            bar.next()
    except Exception as e:
        print(e)
    finally:
        bar.finish()
        print("Saving file to parquet file, please wait...")
        df.to_parquet(CHECK_FILE, index=False)
        print("Done.")


if __name__ == "__main__":
    crawl_all_players()
    pass
