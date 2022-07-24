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

# IaC
aws_section = "aws.amazon.com"
ROOT = Path(__file__).resolve().parents[0]
BASE_PATH = Path(__file__).resolve().parents[0]
IAC_FILE_PATH = str(Path(ROOT, "IaC/credentials.ini"))
sys.path.append(str(Path(ROOT, "IaC")))
from credentials import get_credentials


# # boto3
# aws_credentails = get_credentials(IAC_FILE_PATH, aws_section)
# session = boto3.session.Session(
#     aws_access_key_id=aws_credentails["aws_access_key_id"],
#     aws_secret_access_key=aws_credentails["aws_secret_access_key"],
#     region_name=aws_credentails["region_name"],
# )
# s3 = session.resource("s3")
# my_bucket = s3.Bucket("anltd-bucket")
# # my_bucket.put_object(Key="kiemtra.json", Body=(resp.content))


FILE_PATH = "check_download_pro_players_matches.csv"
df = pd.read_csv(FILE_PATH)
bar = IncrementalBar("Downloading players-matches", max=len(df[df["downloaded"] == 0]))

try:
    for index, row in df.iterrows():

        if row["downloaded"] == 0:
            account_id = row["pro_player_id"]
            players_matches_url = (
                f"https://api.opendota.com/api/players/{account_id}/matches"
            )
            resp = requests.get(players_matches_url, headers=None)
            f = open(f"crawled_data/players_matches/{account_id}.json", "wb")
            f.write(resp.content)
            f.close()
            df.iloc[index]["downloaded"] = 1
            bar.next()
            time.sleep(1)
    bar.finish()

except Exception as e:
    print(e)

finally:
    df.to_csv(FILE_PATH, index=False)
