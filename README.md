# FHM-HACKATHON-2022 - Connecting the world
## Predict the winning team before the match starts in Dota 2
### Steps to crawl data

Run file following:
- `crawl_heroes.py` save to crawled_data/heroes.json.
- `crawl_pro_players.py` save to crawled_data/pro_player.json.
- `get_pro_player_id.py` save to check_download_pro_players.parquet .
- `crawl_player_matches.py` save to check_download_pro_player_matches.parquet.
- `crawl_player_matches.py` save to match file in crawled_data/players_matches/{player_id}.json.
- `get_match_id.py` save to check_download_match_id.parquet.
- `crawl_matches.py` save to match file in crawled_data/matches/{match_id}.json 
- `s3_extract_matches.py` save to new_lake/50k_matches.parquet and create check_download_player_id.parquet
- `crawl_all_players.py` save to crawled_data/players_info/{player_id}.json and crawled_data/players_wl/{player_id}.json


### Steps to ETL data
Run file following:
- `s3_extract_heroes.py` save to new_lake/heroes.parquet.
- `s3_extract_players_heroes.py` save to new_lake/players_heroes.parquet.

s3_extract_players.py
s3_extract_players_matches.py



### API support 
https://docs.opendota.com/

|               | Free Tier                           | Premium Tier                          |
|---------------|-------------------------------------|---------------------------------------|
| Price         | Free                                | $0.01 per 100 calls                   |
| Key Required? | No                                  | Yes -- requires payment method        |
| Call Limit    | 50000 per month                     | Unlimited                             |
| Rate Limit    | 60 calls per minute                 | 1200 calls per minute                 |
| Support       | Community support via Discord group | Priority support from core developers |