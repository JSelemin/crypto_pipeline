from src.extract import fetch_coin_charts
from src.transform import create_staging_table, create_daily_returns

coins_to_fetch = ["bitcoin", "ethereum", "solana", "tron", "dogecoin"]

def main():
        #fetch_coin_charts(coins_to_fetch)
        print("Everything captured correctly.")

        #create_staging_table(coins_to_fetch)
        print('Staging table done.')

        create_daily_returns(coins_to_fetch)
        print('Daily returns done.')



if __name__ == "__main__":
        main()