"""
Crawl heros via API to crawled_data/heros.json
"""
import requests
import json
import time


start_time = time.time()
heros_url = "https://api.opendota.com/api/heroes"
resp = requests.get(heros_url, headers=None)


f = open("crawled_data/heros.json", "wb")
f.write(resp.content)
f.close()

num_heros = len(json.loads(resp.content))
print("Crawled", num_heros, "heros.")

"""
[
    {
        "id": 0,
        "name": "string",
        "localized_name": "string",
        "primary_attr": "string",
        "attack_type": "string",
        "roles": [],
    }
]

{
    "id": 1,
    "name": "npc_dota_hero_antimage",
    "localized_name": "Anti-Mage",
    "primary_attr": "agi",
    "attack_type": "Melee",
    "roles": ["Carry", "Escape", "Nuker"],
    "legs": 2,
}
"""
