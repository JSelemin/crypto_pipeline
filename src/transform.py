import pandas as pd
import duckdb

def create_staging_table(COINS):

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

def create_daily_returns(COINS):

    staging_table = duckdb.read_parquet('data/staging_table.parquet')

    select_cols = []
    final_table = duckdb.sql("SELECT date FROM staging_table")
    for token in COINS:
        temp_table = duckdb.sql(
            f'WITH tempCTE AS (SELECT date, {token}_price, LAG({token}_price) OVER (ORDER BY date) AS {token}_day_before FROM staging_table) ' \
            f'SELECT date, {token}_price, (({token}_price - {token}_day_before) / {token}_day_before) * 100 AS {token}_percentage_change FROM tempCTE')
        
        token_staging_table = duckdb.sql(f'SELECT date, {token}_price, ROUND({token}_percentage_change, 2) AS {token}_daily_returns FROM temp_table')
        final_table = duckdb.sql(f'SELECT * FROM final_table LEFT JOIN token_staging_table ON final_table.date = token_staging_table.date')
        select_cols.extend([f"{token}_price", f"{token}_daily_returns"])

    select_clause = "date, " + ", ".join(select_cols)
    daily_returns_table = duckdb.sql(f"SELECT {select_clause} FROM final_table")

    daily_returns_table.write_parquet('data/analyses/daily_returns.parquet')
    daily_returns_table.show()

def create_rolling_averages(COINS):

    staging_table = duckdb.read_parquet('data/staging_table.parquet')

    select_cols = []
    final_table = duckdb.sql("SELECT date FROM staging_table")

    for token in COINS:
        token_staging_table = duckdb.sql(f"SELECT date, {token}_price, " \
                                f"ROUND(AVG({token}_price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as {token}_weekly_average, " \
                                f"ROUND(AVG({token}_price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as {token}_monthly_average " \
                                "FROM staging_table")
        final_table = duckdb.sql("SELECT * FROM final_table LEFT JOIN token_staging_table ON final_table.date = token_staging_table.date")
        select_cols.extend([f"{token}_price", f"{token}_weekly_average", f"{token}_monthly_average"])
    
    select_clause = "date, " + ", ".join(select_cols)
    rolling_averages_table = duckdb.sql(f"SELECT {select_clause} FROM final_table")
    
    rolling_averages_table.write_parquet('data/analyses/rolling_averages.parquet')    
    rolling_averages_table.show()