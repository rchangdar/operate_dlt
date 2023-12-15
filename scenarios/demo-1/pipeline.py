import dlt
from dlt.sources.helpers import requests

import duckdb

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

load_info = pipeline.run(data, table_name='player')

#print(load_info)

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# this lets us query data without adding schema prefix to table names
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# list all tables
#print(conn.sql("DESCRIBE"))

stats_table = conn.sql("SELECT * FROM player").df()
print(stats_table)

















