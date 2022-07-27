"""
Crawl proplayer via API to crawled_data/pro_player.json
"""
import requests
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
start_time = time.time()
proPlayer_url = "https://api.opendota.com/api/proPlayers"
resp = requests.get(proPlayer_url, headers=None)


f = open("crawled_data/pro_player.json", "wb")
f.write(resp.content)
f.close()

num_pro_player = len(json.loads(resp.content))
print("Crawled", num_pro_player, "pro players.")
