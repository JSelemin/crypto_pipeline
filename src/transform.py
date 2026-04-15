import pandas as pd
import duckdb

def create_staging_table(COINS):

    first_coin = COINS[0]
    REST_OF_COINS = COINS.copy()
    REST_OF_COINS.pop(0)

    first_coin_chart = duckdb.read_parquet(f'data/{first_coin}_chart.parquet')

    for asset in REST_OF_COINS:
        token_chart = duckdb.read_parquet(f'data/{asset}_chart.parquet')
        old_cols = [f"first_coin_chart.{col}" for col in first_coin_chart.columns if col != "date"]
        new_cols = [f"token_chart.{col}" for col in token_chart.columns if col != "date"]

        select_clause = "first_coin_chart.date, " + ", ".join(old_cols + new_cols)

        first_coin_chart = duckdb.sql(f'SELECT {select_clause} FROM first_coin_chart FULL JOIN token_chart ON first_coin_chart.date = token_chart.date')
    
    first_coin_chart.write_parquet('data/staging_table.parquet')
    first_coin_chart.show()

def create_daily_returns(COINS):

    staging_table = duckdb.read_parquet('data/staging_table.parquet')

    select_cols = []
    final_table = duckdb.sql("SELECT date FROM staging_table")
    for asset in COINS:
        temp_table = duckdb.sql(
            f'WITH tempCTE AS (SELECT date, {asset}_price, LAG({asset}_price) OVER (ORDER BY date) AS {asset}_day_before FROM staging_table) ' \
            f'SELECT date, {asset}_price, (({asset}_price - {asset}_day_before) / {asset}_day_before) * 100 AS {asset}_percentage_change FROM tempCTE')
        
        token_staging_table = duckdb.sql(f'SELECT date, {asset}_price, ROUND({asset}_percentage_change, 2) AS {asset}_daily_returns FROM temp_table')
        final_table = duckdb.sql(f'SELECT * FROM final_table LEFT JOIN token_staging_table ON final_table.date = token_staging_table.date')
        select_cols.extend([f"{asset}_price", f"{asset}_daily_returns"])

    select_clause = "date, " + ", ".join(select_cols)
    daily_returns_table = duckdb.sql(f"SELECT {select_clause} FROM final_table")

    daily_returns_table.write_parquet('data/analyses/daily_returns.parquet')
    daily_returns_table.show()

def create_rolling_averages(COINS):

    staging_table = duckdb.read_parquet('data/staging_table.parquet')

    select_cols = []
    final_table = duckdb.sql("SELECT date FROM staging_table")

    for asset in COINS:
        token_staging_table = duckdb.sql(f"SELECT date, {asset}_price, " \
                                f"ROUND(AVG({asset}_price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as {asset}_weekly_average, " \
                                f"ROUND(AVG({asset}_price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as {asset}_monthly_average " \
                                "FROM staging_table")
        final_table = duckdb.sql("SELECT * FROM final_table LEFT JOIN token_staging_table ON final_table.date = token_staging_table.date")
        select_cols.extend([f"{asset}_price", f"{asset}_weekly_average", f"{asset}_monthly_average"])
    
    select_clause = "date, " + ", ".join(select_cols)
    rolling_averages_table = duckdb.sql(f"SELECT {select_clause} FROM final_table")
    
    rolling_averages_table.write_parquet('data/analyses/rolling_averages.parquet')    
    rolling_averages_table.show()

def create_volatility(COINS):

    staging_table = duckdb.read_parquet('data/analyses/daily_returns.parquet')

    select_cols = []
    final_table = duckdb.sql("SELECT date FROM staging_table")

    for asset in COINS:
        token_staging_table = duckdb.sql(f"SELECT date, {asset}_price, {asset}_daily_returns, ROUND(STDDEV({asset}_daily_returns) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as {asset}_monthly_volatility FROM staging_table")
        final_table = duckdb.sql("SELECT * FROM final_table LEFT JOIN token_staging_table ON final_table.date = token_staging_table.date")
        select_cols.extend([f"{asset}_price", f"{asset}_daily_returns", f"{asset}_monthly_volatility"])
    
    select_clause = "date, " + ", ".join(select_cols)
    volatility_table = duckdb.sql(f"SELECT {select_clause} FROM final_table")
    
    volatility_table.write_parquet('data/analyses/volatility.parquet')    
    volatility_table.show()

def create_market_dominance(COINS):

    staging_table = duckdb.read_parquet('data/staging_table.parquet')

    columns_array = []
    for asset in COINS:
        columns_array.extend([f"{asset}_market_cap"])

    columns_clause = "date, " + ", ".join(columns_array)
    sum_clause = " + ".join(columns_array)
    middle_table = duckdb.sql(f'SELECT {columns_clause}, {sum_clause} AS total_market_cap FROM staging_table')
    middle_table.show()

    select_cols = []
    final_table = duckdb.sql("SELECT date FROM staging_table")

    for asset in COINS:
        token_staging_table = duckdb.sql(f"SELECT date, total_market_cap, {asset}_market_cap, ROUND(({asset}_market_cap / total_market_cap) * 100, 2) AS {asset}_dominance FROM middle_table")
        final_table = duckdb.sql("SELECT * FROM final_table LEFT JOIN token_staging_table ON final_table.date = token_staging_table.date")
        select_cols.extend([f"{asset}_market_cap", f"{asset}_dominance"])
    
    select_clause = "date, " + "total_market_cap, " + ", ".join(select_cols)
    market_dominance_table = duckdb.sql(f"SELECT {select_clause} FROM final_table")
    
    market_dominance_table.write_parquet('data/analyses/market_dominance.parquet')    
    market_dominance_table.show()

def create_top_movers(COINS):

    staging_table = duckdb.read_parquet('data/analyses/daily_returns.parquet')

    columns_array = []
    absolutes_array = []

    for asset in COINS:
        columns_array.extend([f"{asset}_daily_returns"])
        absolutes_array.extend([f"ABS({asset}_daily_returns)"])

    absolutes = ", ".join(absolutes_array)
    case_array = []
    naming_case_array = []

    for asset in COINS:
        case_array.extend([f"WHEN ABS({asset}_daily_returns) = GREATEST({absolutes}) THEN {asset}_daily_returns"])
        naming_case_array.extend([f"WHEN ABS({asset}_daily_returns) = GREATEST({absolutes}) THEN '{asset}'"])


    columns  = ", ".join(columns_array)
    cases = " ".join(case_array)
    naming_cases = " ".join(naming_case_array)

    helper_table = duckdb.sql(f"SELECT *, YEARWEEK(date) AS yearwk, CASE {cases} END AS top_mover, CASE {naming_cases} END AS coin_name FROM staging_table")
    helper_second_table = duckdb.sql(f"SELECT MAX(ABS(top_mover)) AS week_mover, yearwk FROM helper_table GROUP BY yearwk")
    top_movers_table = duckdb.sql(f"SELECT f.yearwk AS year_week, s.week_mover AS max_daily_movement, f.coin_name AS coin, f.date AS date_of_move FROM helper_second_table s LEFT JOIN helper_table f ON s.week_mover = ABS(top_mover) AND s.yearwk = f.yearwk")
    top_movers_table.show()
    top_movers_table.write_parquet('data/analyses/top_movers.parquet')

def create_coin_correlation(COINS):

    staging_table = duckdb.read_parquet('data/analyses/daily_returns.parquet')

    first_coin = COINS[0]

    select_cols = []
    final_table = duckdb.sql(f"SELECT * FROM staging_table")

    for asset in COINS:
            token_list = [f"'{asset}' AS coin_name"]
            for second in COINS:
                token_list.extend([f"ROUND(CORR({asset}_daily_returns, {second}_daily_returns), 2) AS corr_with_{second}"])
            row_query = [f"SELECT {", ".join(token_list)} FROM staging_table"]
            select_cols.extend(row_query)

    
    select_clause = " UNION ALL ".join(select_cols)
    coin_correlation_table = duckdb.sql(select_clause)
    coin_correlation_table.show()
    coin_correlation_table.write_parquet('data/analyses/coin_correlation.parquet')