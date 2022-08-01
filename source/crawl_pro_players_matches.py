"""
Crawl player_matchs via API with check_download_pro_players_matches.csv to crawled_data/players_matchs.json
"""

import pandas as pd
import requests
import json
import time
from progress.bar import IncrementalBar
import boto3

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECK_FILE = "check_download_pro_players.parquet"


def crawl_pro_players_matches(interval=1):
    df = pd.read_parquet(CHECK_FILE)
    bar = IncrementalBar(
        "Downloading players-matches", max=len(df[df["downloaded"] == 0])
    )
    try:
        for index, row in df.iterrows():

            if row["downloaded"] == 0:
                account_id = row["pro_player_id"]
                players_matches_url = (
                    f"https://api.opendota.com/api/players/{account_id}/matches"
                )
                download_start_time = time.time()
                resp = requests.get(players_matches_url, headers=None)
                f = open(f"crawled_data/players_matches/{account_id}.json", "wb")
                f.write(resp.content)
                f.close()
                df.iloc[index]["downloaded"] = 1
                bar.next()
                download_end_time = time.time()
                elapsed_time = download_end_time - download_start_time
                if elapsed_time < interval:
                    time.sleep(interval - elapsed_time)

        bar.finish()
    except Exception as e:
        print(e)

    finally:
        df.to_parquet(CHECK_FILE, index=False)


if __name__ == "__main__":
    crawl_pro_players_matches()
    pass
