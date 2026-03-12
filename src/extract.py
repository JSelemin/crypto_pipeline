import requests
import os
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv

# Get keys
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Base URL and endpoints
BASE_URL = "https://api.coingecko.com/api/v3"

PING = "/ping"
COIN_PRICE_BY_ID = "/simple/price"

KEY_HANDLER = f"?x_cg_demo_api_key={API_KEY}"

# Params
payload_test = {"vs_currencies": "ars",
            "ids": "ethereum"}

date = datetime.now()
month_before = date + timedelta(weeks=-16)
date = date.timestamp()
month_before = month_before.timestamp()

payload = {"vs_currency": "usd",
            "from": f"{month_before}",
            "to": f"{date}",
            "precision": 2}

# Test ping
print("Starting API connection test...")
r_ping = requests.get(BASE_URL + PING + KEY_HANDLER)
print(r_ping.json())

# Test single coin price
r_id = requests.get(BASE_URL + COIN_PRICE_BY_ID + KEY_HANDLER, params=payload_test)
print(r_id.json())
print("End of API connection test")


def fetch_coin_chart(COIN):

    HISTORICAL_CHART_DATA = f"/coins/{COIN}/market_chart/range"

    # Get prices for one coin
    r_chart = requests.get(BASE_URL + HISTORICAL_CHART_DATA + KEY_HANDLER, params=payload)

    # Transform into a df and basic transformations
    df = pd.DataFrame(r_chart.json())

    df["date"] = df["prices"].apply(lambda x: x[0])
    df["price"] = df["prices"].apply(lambda x: x[1])
    df["market_cap"] = df["market_caps"].apply(lambda x: x[1])
    df["total_volume"] = df["total_volumes"].apply(lambda x: x[1])

    df = df.drop(["prices", "market_caps", "total_volumes"], axis=1)
    df["date"] = df["date"].apply(lambda x: datetime.fromtimestamp(x / 1000))
    print(df.head())

    # Save to parquet file
    df.to_parquet(f'data/{COIN}_chart.parquet')



coins_to_fetch = ["bitcoin", "ethereum", "solana", "tron", "dogecoin"]

for token in coins_to_fetch:
    fetch_coin_chart(token)
    print(f"Token {token} captured correctly.")