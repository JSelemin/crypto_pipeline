from src.extract import fetch_coin_charts
from src.transform import create_staging_table, create_daily_returns, create_rolling_averages, create_volatility, create_market_dominance, create_top_movers, create_coin_correlation

coins_to_fetch = ["bitcoin", 
                "ethereum", 
                "solana", 
                "tron", 
                "dogecoin", 
                "ripple", 
                "binancecoin", 
                "monero", 
                "litecoin", 
                "zcash"]

def main():
        fetch_coin_charts(coins_to_fetch)
        print("Everything captured correctly.")

        create_staging_table(coins_to_fetch)
        print('Staging table done.')

        create_daily_returns(coins_to_fetch)
        print('Daily returns done.')

        create_rolling_averages(coins_to_fetch)
        print('Rolling averages done.')

        create_volatility(coins_to_fetch)
        print('Volatility done.')

        create_market_dominance(coins_to_fetch)
        print('Market dominance done.')

        create_top_movers(coins_to_fetch)
        print('Top movers done.')

        create_coin_correlation(coins_to_fetch)
        print("Coin correlation done.")



if __name__ == "__main__":
        main()