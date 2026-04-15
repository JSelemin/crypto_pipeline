import requests
import os
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
os.makedirs('data/output', exist_ok=True)

BASE_URL = "https://api.coingecko.com/api/v3"

PING = "/ping"
COIN_PRICE_BY_ID = "/simple/price"

KEY_HANDLER = f"?x_cg_demo_api_key={API_KEY}"


def fetch_coin_charts(COINS):

    payload_test = {"vs_currencies": "ars",
                "ids": "ethereum"}

    date = datetime.now()
    months_before = date + timedelta(weeks=-52)
    date = date.timestamp()
    months_before = months_before.timestamp()

    payload = {"vs_currency": "usd",
                "from": f"{months_before}",
                "to": f"{date}",
                "precision": 2}

    print("Starting API connection test...")
    r_ping = requests.get(BASE_URL + PING + KEY_HANDLER)
    print(r_ping.json())

    r_id = requests.get(BASE_URL + COIN_PRICE_BY_ID + KEY_HANDLER, params=payload_test)
    print(r_id.json())
    print("...End of API connection test")

    for token in COINS:
        HISTORICAL_CHART_DATA = f"/coins/{token}/market_chart/range"

        r_chart = requests.get(BASE_URL + HISTORICAL_CHART_DATA + KEY_HANDLER, params=payload)

        df = pd.DataFrame(r_chart.json())

        df["date"] = df["prices"].apply(lambda x: x[0])
        df[f"{token}_price"] = df["prices"].apply(lambda x: x[1])
        df[f"{token}_market_cap"] = df["market_caps"].apply(lambda x: x[1])
        df[f"{token}_total_volume"] = df["total_volumes"].apply(lambda x: x[1])

        df = df.drop(["prices", "market_caps", "total_volumes"], axis=1)
        df["date"] = df["date"].apply(lambda x: datetime.fromtimestamp(x / 1000))
        print(df.head())

        df.to_parquet(f'data/{token}_chart.parquet')
        print(f"Token {token} captured correctly.")
