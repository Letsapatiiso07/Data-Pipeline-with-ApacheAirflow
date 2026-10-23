# Crypto Data Pipeline with Apache Airflow

An Airflow DAG that runs every hour, pulls Bitcoin and Ethereum market data
from the free [CoinGecko API](https://www.coingecko.com/en/api) and saves
each pull as a timestamped CSV file. It is a small, working example of
scheduling an extract-and-load job with Airflow.

## How it works

```
start  ->  fetch_and_store_crypto_data  ->  end
```

- **`dags/crypto_pipeline.py`** defines the DAG `crypto_data_pipeline`:
  hourly schedule, no catch-up, one retry after 2 minutes. The start and
  end tasks are empty markers around the one real task.
- **`scripts/fetch_crypto_data.py`** does the work:
  - `fetch_crypto_data()` calls CoinGecko's `/simple/price` endpoint for
    price, market cap, 24-hour volume, 24-hour change and the time
    CoinGecko last updated each coin, and returns a pandas DataFrame;
  - `save_to_csv()` writes it to `data/raw/<UTC time>.csv` at the repo
    root.

A real run on 23 October 2026 wrote:

```
symbol,price,market_cap,volume_24h,change_24h,timestamp
bitcoin,81481.0,1637934930357.2898,24702072143.159286,0.04003172669493962,2026-10-23T15:13:59+00:00
ethereum,2399.12,293082471503.37555,11778912410.016125,0.21188947940363007,2026-10-23T15:13:49+00:00
```

## How to run it

Airflow runs on Linux and macOS; on Windows, use WSL.

```bash
git clone https://github.com/Letsapatiiso07/Data-Pipeline-with-ApacheAirflow.git
cd Data-Pipeline-with-ApacheAirflow
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt"
```

The constraints file pins Airflow's dependencies; change `3.11` to your
Python version (3.8 to 3.12).

To try the fetch step on its own, without Airflow:

```bash
python scripts/fetch_crypto_data.py
```

To run it under Airflow, point Airflow at this repo's `dags` folder and
start it:

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
airflow standalone
```

Then open http://localhost:8080, sign in with the user and password that
`airflow standalone` prints, and switch on `crypto_data_pipeline`.

## Limits and next steps

- **Two coins, one endpoint.** The coin list is a function argument;
  adding coins or currencies is a one-line change.
- **One CSV per run.** A next step is to load each pull into a database
  table (SQLite or PostgreSQL) so the history can be queried, and to add a
  data check task before the load.
- **No tests yet.** The fetch and save functions are small enough to test
  with a mocked API response.
- CoinGecko's free API limits how often it can be called; hourly is well
  within that.

## Author

Tiiso Letsapa, Letsapamyron07@gmail.com
