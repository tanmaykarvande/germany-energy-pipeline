import os
import requests
import pandas as pd
from supabase import create_client, Client
import datetime

# --- YOUR PROJECT KEYS ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_sync():
    print("Step 1: Fetching live data from German SMARD API...")
    # This URL gets the latest available hourly indices for German electricity prices
    index_url = "https://www.smard.de/app/chart_data/410/DE/index_hour.json"
    
    try:
        response = requests.get(index_url)
        timestamps = response.json()['timestamps']
        latest_ts = timestamps[-1]  # Get the most recent batch of data
        
        # Get actual price data for that batch
        data_url = f"https://www.smard.de/app/chart_data/410/DE/410_DE_hour_{latest_ts}.json"
        data_response = requests.get(data_url)
        price_data = data_response.json()['series']
        
        # Format data for your Supabase table
        records = []
        for entry in price_data:
            dt = datetime.datetime.fromtimestamp(entry[0] / 1000.0, tz=datetime.timezone.utc)
            records.append({
                "timestamp": dt.isoformat(),
                "price_eur_mwh": entry[1],
                "region": "DE-LU"
            })
        
        print(f"Step 2: Pushing {len(records)} records to Supabase...")
        # .upsert() ensures we don't get duplicates if we run this again
        supabase.table("energy_prices").upsert(records).execute()
        print("SUCCESS: Data is now in your cloud database!")

    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    run_sync()
