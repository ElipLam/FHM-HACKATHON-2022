"""
Get list player_hero from s3 bucket to save to check_download_player_heroes.csv
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
from io import StringIO


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

resource = session.resource("s3")
my_bucket = resource.Bucket(BUCKET)
list_keys = []
list_objects = my_bucket.objects.filter(Prefix=PREFIX)
# list_objects = my_bucket.objects.filter(Prefix="hackathon/new_lake/")
bar_objects = IncrementalBar("Getting object...", max=len([*list_objects]))
for s3_object in list_objects:
    list_keys.append(s3_object.key)
    bar_objects.next()
bar_objects.finish()
df = pd.DataFrame(list_keys, columns=["key"])
df["downloaded"] = 0
df.to_csv("check_download_player_heroes.csv", index=False)
print(df.head())
