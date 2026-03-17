import pandas as pd
import duckdb

print('Now trying to transform...')

def transform_coin_charts(COINS):

    # Isolate one specific coin and use it as base table
    first_coin = COINS[0]
    REST_OF_COINS = COINS.copy()
    REST_OF_COINS.pop(0)

    first_coin_chart = duckdb.read_parquet(f'data/{first_coin}_chart.parquet')

    for token in REST_OF_COINS:

        #Pick up a new coin, add it to the table, and leave out the duplicate date during the JOIN
        token_chart = duckdb.read_parquet(f'data/{token}_chart.parquet')
        old_cols = [f"first_coin_chart.{col}" for col in first_coin_chart.columns if col != "date"]
        new_cols = [f"token_chart.{col}" for col in token_chart.columns if col != "date"]

        select_clause = "first_coin_chart.date, " + ", ".join(old_cols + new_cols)

        first_coin_chart = duckdb.sql(f'SELECT {select_clause} FROM first_coin_chart FULL JOIN token_chart ON first_coin_chart.date = token_chart.date')
    
    # Once the transformation is done, write it as a staging table
    first_coin_chart.write_parquet('data/staging_table.parquet')
    first_coin_chart.show()

print('...transformed.')