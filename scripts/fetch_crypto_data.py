import requests
import pandas as pd
from datetime import datetime
import os

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

    response = requests.get(url, params=params)
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
            "timestamp": datetime.utcfromtimestamp(info["last_updated_at"]).isoformat()
        })

    df = pd.DataFrame(rows)
    return df

def save_to_csv(df, folder="data/raw/"):
    os.makedirs(folder, exist_ok=True)
    filename = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False)
    print(f"Saved data to {filepath}")
    return filepath

if __name__ == "__main__":
    df = fetch_crypto_data()
    save_to_csv(df)