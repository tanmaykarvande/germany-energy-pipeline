import os
import datetime
import requests
import pandas as pd
from supabase import create_client

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SECRET_KEY"])

FILTER, REGION, WEEKS = 4169, "DE", 8  # 4169 = day-ahead market price
BASE = f"https://www.smard.de/app/chart_data/{FILTER}/{REGION}"

def fetch_prices():
    idx = requests.get(f"{BASE}/index_hour.json", timeout=30)
    idx.raise_for_status()
    chunks = idx.json()["timestamps"][-WEEKS:]  # each chunk = one week
    rows = []
    for ts in chunks:
        r = requests.get(f"{BASE}/{FILTER}_{REGION}_hour_{ts}.json", timeout=30)
        r.raise_for_status()
        for t, price in r.json()["series"]:
            if price is None:
                continue
            dt = datetime.datetime.fromtimestamp(t / 1000, tz=datetime.timezone.utc)
            rows.append({"timestamp": dt.isoformat(), "price_eur_mwh": price, "region": "DE-LU"})
    return rows

def upsert(table, rows):
    for i in range(0, len(rows), 500):
        sb.table(table).upsert(rows[i:i + 500], on_conflict="timestamp").execute()
    print(f"{table}: {len(rows)} rows upserted")

def baseline_forecast(rows, horizon=48):
    s = pd.Series(
        {pd.Timestamp(r["timestamp"]): r["price_eur_mwh"] for r in rows}
    ).sort_index()
    future = pd.date_range(s.index.max() + pd.Timedelta(hours=1), periods=horizon, freq="h")
    idx = s.index.union(future)
    lags = pd.concat(
        [s.reindex(idx - pd.Timedelta(weeks=k)).set_axis(idx) for k in (1, 2, 3, 4)],
        axis=1,
    )
    pred = lags.mean(axis=1).dropna()
    return [{"timestamp": t.isoformat(), "predicted_price": round(float(v), 2)} for t, v in pred.items()]

if __name__ == "__main__":
    print("Fetching from SMARD...")
    prices = fetch_prices()
    print(f"Got {len(prices)} hourly prices")
    upsert("energy_prices", prices)
    upsert("price_predictions", baseline_forecast(prices))
    print("Done.")