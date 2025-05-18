import requests
import pandas as pd
from datetime import datetime, timedelta
import config
import time

def get_historical_data(symbol, interval, start_date_str, end_date_str, api_key):
    """
    Fetches historical OHLCV data from Twelve Data.
    
    Args:
        symbol (str): The trading symbol (e.g., "EUR/USD").
        interval (str): The time interval (e.g., "1h", "5min").
        start_date_str (str): Start date in "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" format.
        end_date_str (str): End date in "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" format.
        api_key (str): Your Twelve Data API key.

    Returns:
        pandas.DataFrame: DataFrame with OHLC data, indexed by datetime, or None if error.
                          Columns: ['open', 'high', 'low', 'close', 'volume']
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "apikey": api_key,
        "outputsize": 5000, # Max supported by Twelve Data
        "format": "JSON",
        "timezone": "UTC" # Explicitly request UTC data
    }
    
    # Twelve Data API uses 'start_date' and 'end_date' for range queries.
    # It expects "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
    if start_date_str:
        params["start_date"] = start_date_str
    if end_date_str:
        params["end_date"] = end_date_str

    print(f"[DataFetcher] Requesting data for {symbol} ({interval}) from {start_date_str} to {end_date_str}")

    try:
        response = requests.get(f"{config.BASE_URL}/time_series", params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        data = response.json()

        if data.get("status") == "error":
            print(f"[DataFetcher] Error from Twelve Data API: {data.get('message')}")
            return None

        if "values" not in data or not data["values"]:
            print(f"[DataFetcher] No data returned for {symbol} ({interval}) in the given range.")
            # Ensure columns match what's expected later, even if empty
            cols = ['datetime', 'open', 'high', 'low', 'close']
            if 'volume' in (data.get("values", [{}])[0] if data.get("values") else {}): # Check if volume might exist based on first record
                 cols.append('volume')
            return pd.DataFrame(columns=cols).set_index('datetime')


        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
        
        # Select available columns
        available_cols = []
        if "open" in df.columns: available_cols.append("open")
        if "high" in df.columns: available_cols.append("high")
        if "low" in df.columns: available_cols.append("low")
        if "close" in df.columns: available_cols.append("close")
        if "volume" in df.columns: available_cols.append("volume")
        
        if not all(col in df.columns for col in ["open", "high", "low", "close"]):
            print(f"[DataFetcher] Critical OHLC data missing in response for {symbol} ({interval}). Columns: {df.columns.tolist()}")
            return None

        df = df[available_cols].astype(float)
        df = df.sort_index() # Ensure data is chronological
        
        # Twelve Data returns data in reverse chronological order if no start/end date.
        # If start/end_date are provided, it seems to be chronological.
        # Sorting by index ensures it's always chronological.

        print(f"[DataFetcher] Successfully fetched {len(df)} records for {symbol} ({interval})")
        return df

    except requests.exceptions.RequestException as e:
        print(f"[DataFetcher] Request failed: {e}")
        return None
    except Exception as e:
        print(f"[DataFetcher] Error processing data: {e}")
        return None

if __name__ == '__main__':
    # Example usage:
    # Ensure you have your API key in config.py
    if config.TWELVE_DATA_API_KEY == "YOUR_API_KEY_HERE":
        print("Please set your TWELVE_DATA_API_KEY in config.py before running this example.")
    else:
        symbol = "EUR/USD"
        
        # For fetching a range
        # start_fetch_date = "2024-05-13 00:00:00"
        # end_fetch_date = "2024-05-15 00:00:00"

        # For fetching most recent data up to a point (Twelve Data free plan might be delayed)
        # To get data up to "now" for backtesting, you'd typically specify a historical end_date.
        end_fetch_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        start_fetch_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S") # Fetch last 30 days for example

        print(f"Attempting to fetch H1 data for {symbol} from {start_fetch_date} to {end_fetch_date}")
        h1_data = get_historical_data(symbol, "1h", start_fetch_date, end_fetch_date, config.TWELVE_DATA_API_KEY)
        if h1_data is not None and not h1_data.empty:
            print("\nH1 Data:")
            print(h1_data.head())
            print(h1_data.tail())
        else:
            print("\nFailed to fetch H1 data or no data returned.")

        # Be mindful of API rate limits if you make many requests quickly.
        time.sleep(1) # Small delay if making multiple calls, especially for different symbols/intervals

        print(f"\nAttempting to fetch M5 data for {symbol} from {start_fetch_date} to {end_fetch_date}")
        m5_data = get_historical_data(symbol, "5min", start_fetch_date, end_fetch_date, config.TWELVE_DATA_API_KEY)
        if m5_data is not None and not m5_data.empty:
            print("\nM5 Data:")
            print(m5_data.head())
            print(m5_data.tail())
        else:
            print("\nFailed to fetch M5 data or no data returned.") 