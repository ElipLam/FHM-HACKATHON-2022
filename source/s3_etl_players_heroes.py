"""
Extract s3://huongami-s3-demo/hackathon/player_heroes/ to new_lake/player_heros.csv
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
from datetime import datetime
import sys
from pathlib import Path
from io import StringIO


now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Start Time =", current_time)
start_time = time.time()


# IaC
aws_section = "aws.amazon.com"
ROOT = Path(__file__).resolve().parents[1]
BASE_PATH = Path(__file__).resolve().parents[0]
IAC_FILE_PATH = str(Path(ROOT, "IaC/credentials.ini"))
sys.path.append(str(Path(ROOT, "IaC")))
from credentials import get_credentials


# boto3
aws_credentails = get_credentials(IAC_FILE_PATH, aws_section)
session = boto3.session.Session(
    aws_access_key_id=aws_credentails["aws_access_key_id"],
    aws_secret_access_key=aws_credentails["aws_secret_access_key"],
    region_name=aws_credentails["region_name"],
)


BUCKET = "huongami-s3-demo"
PREFIX = "hackathon/player_heroes/"
CHECK_FILE = "check_download_player_heroes.parquet"
OUTPUT_FILE = "new_lake/players_heroes.parquet"
list_keys = []

resource = session.resource("s3")
my_bucket = resource.Bucket(BUCKET)


# get list objects
list_objects = my_bucket.objects.filter(Prefix=PREFIX)
# list_objects = my_bucket.objects.filter(Prefix="hackathon/new_lake/")
bar_objects = IncrementalBar("Getting object...", max=len([*list_objects]))
for s3_object in list_objects:
    list_keys.append(s3_object.key)
    bar_objects.next()
bar_objects.finish()

# save list objects to check download file
df_check = pd.DataFrame(list_keys, columns=["key"])
df_check["downloaded"] = 0
df_check.to_parquet(CHECK_FILE, index=False)

# read list objects from check download file
df_read_check_download = pd.read_parquet(CHECK_FILE)
list_keys = df_read_check_download["key"].tolist()


if Path(OUTPUT_FILE).is_file():
    global_df = pd.read_parquet(OUTPUT_FILE)
else:
    global_df = pd.DataFrame(
        columns=[
            "win",
            "games",
            "with_games",
            "hero_id",
            "against_games",
            "last_played",
            "against_win",
            "with_win",
        ],
    )
list_df = [global_df]
bar_data = IncrementalBar(
    "Appending data...",
    max=len(df_read_check_download[df_read_check_download["downloaded"] == 0]),
    suffix="%(index)d/%(max)d | %(elapsed_td)s",
)
try:
    for index, row in df_read_check_download.iterrows():
        if row["downloaded"] == 0:
            key = row["key"]
            player_id_files = key.split("/")[-1]
            player_id = player_id_files.split(".")[0]
            raw_data = resource.Object(BUCKET, key)
            data = raw_data.get()["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(data))
            df["player_id"] = player_id
            df_read_check_download.loc[index, "downloaded"] = 1
            list_df.append(df)
            bar_data.next()
except Exception as e:
    print("ERROR:", e)
finally:
    bar_data.finish()
    # print(df_read_check_download.head())
    df_read_check_download.to_parquet(CHECK_FILE, index=False)
    result_df = pd.concat(list_df)
    result_df.to_parquet(OUTPUT_FILE, index=False)

    end_time = time.time()
    elapsed = end_time - start_time
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("End Time =", current_time)
    print("Elapsed =", time.strftime("%H:%M:%S", time.gmtime(elapsed)))


if __name__ == "__main__":

    pass
# ----------------------------------------------
# player_heros.csv: win,games,with_games,hero_id,against_games,last_played,against_win,with_win

#  player heros table ::
#   PlayerId:ten file,
#   HeroId:player_heros.csv(hero_id),
#   Profiency,
#   TimePlay,
#   Winrateplayer_heros.csv(win/games),
#   WinrateTeam:player_heros.csv(with_win/with_games),
#   WinrateEnemy:player_heros.csv(against_win/against_games)
