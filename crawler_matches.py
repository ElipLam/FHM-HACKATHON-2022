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


# boto3
aws_credentails = get_credentials(IAC_FILE_PATH, aws_section)
session = boto3.session.Session(
    aws_access_key_id=aws_credentails["aws_access_key_id"],
    aws_secret_access_key=aws_credentails["aws_secret_access_key"],
    region_name=aws_credentails["region_name"],
)

# heros_url = "https://api.opendota.com/api/heroes"
# resp = requests.get(heros_url, headers=None)
# s3 = session.resource("s3")
# my_bucket = s3.Bucket("anltd-bucket")
# year = 2022
# my_bucket.put_object(Key=f"{year}/kiemtra.json", Body=(resp.content))


# s3 = session.resource("s3")
# my_bucket = s3.Bucket("huongami-s3-demo/hackathon/players_info.csv")
# obj = s3.Object("huongami-s3-demo", "hackathon/players_info.csv")
# # print(obj)
# # import pyspark
# print(type(obj.get()["Body"].read()))
