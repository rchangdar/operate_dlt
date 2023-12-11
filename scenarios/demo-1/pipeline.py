import dlt
from dlt.sources.helpers import requests

# Create a dlt pipeline that will load

# chess player data to the DuckDB destination

pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data'
)

# Grab some player data from Chess.com API

data = []

for player in ['magnuscarlsen', 'rpragchess']:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    data.append(response.json())

for i in data:
    print(i)