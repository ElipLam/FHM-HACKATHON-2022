"""
Upload Script to Bucket via API.
"""
import requests

FILE_NAME = "glue_altd_hero.py"
BUCKKET = "py-script-file"
URL = (
    f"https://ol4g7ofrh1.execute-api.us-east-1.amazonaws.com/dev/{BUCKKET}/{FILE_NAME}"
)
headers = {"Content-type": "text/plain"}
response = requests.put(URL, data=open(FILE_NAME, "rb"), headers=headers)
print(f'Put "{FILE_NAME}" to "{BUCKKET}" bucket')
print("Status code response:", response.status_code)
