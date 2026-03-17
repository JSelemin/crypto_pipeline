from src.extract import fetch_coin_charts
from src.transform import transform_coin_charts

coins_to_fetch = ["bitcoin", "ethereum", "solana", "tron", "dogecoin"]

def main():
        fetch_coin_charts(coins_to_fetch)
        print("Everything captured correctly.")

        transform_coin_charts(coins_to_fetch)
        print('Transformation correct')



if __name__ == "__main__":
        main()