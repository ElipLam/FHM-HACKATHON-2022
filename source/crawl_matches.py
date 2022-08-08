"""
Crawl matches via API to crawled_data/matches folder
"""

import pandas as pd
import requests
import json
import time
from progress.bar import IncrementalBar
import boto3

import sys
import os
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
BASE_PATH = Path(__file__).resolve().parents[0]
OUTPUT_FOLDER = str(Path(ROOT, "crawled_data", "matches"))
CHECK_FILE = "check_download_match_id.parquet"

# add my source
SOUCRE_PATH = str(Path(ROOT, "source"))
sys.path.append(SOUCRE_PATH)
from common import *


def crawl_match(match_id, start_time, output=OUTPUT_FOLDER):
    matches_url = f"https://api.opendota.com/api/matches/{match_id}"
    resp = requests.get(matches_url, headers=None)
    year, month, day = extract_timestamp(start_time)
    file_path = create_folder(OUTPUT_FOLDER, str(year), str(month), str(day))
    f = open(str(Path(file_path, f"{match_id}.json")), "wb")
    f.write(resp.content)
    f.close()


def create_folder(base: str, year: str, month: str, day: str):
    file_path = str(Path(base, year, month, day))
    if os.path.isdir(file_path):
        pass
    else:
        os.makedirs(file_path)
    return file_path


def crawl_matches():
    match_df = pd.read_parquet(CHECK_FILE)
    df = match_df.sort_values("match_id", ascending=False)
    df = df[:50000]  # get 50000 last matches

    # print(df.head())

    bar = IncrementalBar(
        "Downloading players-matches",
        max=len(df[df["downloaded"] == 0]),
        suffix="%(index)d/%(max)d | %(elapsed_td)s",
    )
    try:
        for index, row in df.iterrows():
            if row["downloaded"] == 0:
                match_id = row["match_id"]
                start_time = row["start_time"]
                crawl_match(match_id, start_time)
                match_df.loc[index, "downloaded"] = 1
                bar.next()
                # time.sleep(0.2)
    except Exception as e:
        print(e)
    finally:
        bar.finish()
        print("Saving file to parquet file, please wait...")
        match_df.to_parquet(CHECK_FILE, index=False)
        print("Done.")


if __name__ == "__main__":
    crawl_matches()
    pass
