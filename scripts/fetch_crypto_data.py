import requests
import pandas as pd
from datetime import datetime, timezone
import os

# data/raw at the repo root, wherever Airflow runs the task from
RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

def fetch_crypto_data(symbols=["bitcoin", "ethereum"], currency="usd"):
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(symbols),
        "vs_currencies": currency,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    rows = []
    for symbol in symbols:
        info = data[symbol]
        rows.append({
            "symbol": symbol,
            "price": info[currency],
            "market_cap": info[f"{currency}_market_cap"],
            "volume_24h": info[f"{currency}_24h_vol"],
            "change_24h": info[f"{currency}_24h_change"],
            "timestamp": datetime.fromtimestamp(info["last_updated_at"], tz=timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)
    return df

def save_to_csv(df, folder=RAW_DIR):
    os.makedirs(folder, exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False)
    print(f"Saved data to {filepath}")
    return filepath

if __name__ == "__main__":
    df = fetch_crypto_data()
    save_to_csv(df)