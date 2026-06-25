# utils.py
# Shared utility functions used across the entire project
# Samuel Grana - Stocktwits News Sentiment Dashboard

from datetime import datetime, timezone, timedelta
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

# Read at import time as defaults; fetch_tickers_from_finviz re-reads on each
# call so a token written to .env by app.py is picked up without a restart.
FINVIZ_API_TOKEN  = os.getenv("FINVIZ_API_TOKEN",  "")
FINVIZ_EXPORT_URL = os.getenv("FINVIZ_EXPORT_URL", "")


def get_timestamp() -> str:
    """
    Returns the current UTC time as a string in the format:
    DD:MM:YYYY:HH:MM:SS
    This is the primary index format used in MongoDB.
    """
    now = datetime.now(timezone.utc)
    return now.strftime("%d:%m:%Y:%H:%M:%S")


def get_minute_bucket() -> str:
    """
    Returns the current UTC time bucketed to the current minute.
    Seconds are always forced to 00.
    Used as the index key for the message_density collection.
    """
    now = datetime.now(timezone.utc)
    return now.strftime("%d:%m:%Y:%H:%M:00")


def get_utc_iso() -> str:
    """
    Returns current UTC time in standard ISO format.
    Example: 2026-05-27T14:32:07Z
    """
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_window_start_iso(rolling_window_minutes: int) -> str:
    """
    Returns the ISO timestamp for the start of the rolling window.
    Used to filter MongoDB queries to only return recent messages.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=rolling_window_minutes)
    return window_start.strftime("%Y-%m-%dT%H:%M:%SZ")


def filter_by_window(messages: list, rolling_window_minutes: int) -> list:
    """
    Filters a list of message documents to only include messages
    within the rolling window.
    """
    window_start = get_window_start_iso(rolling_window_minutes)
    filtered = [
        msg for msg in messages
        if msg.get("created_at_utc", "") >= window_start
    ]
    return filtered


def load_tickers_from_finviz() -> list:
    """
    Fetches a fresh ticker list directly from Finviz Elite API.
    Uses the export URL and API token from .env file.
    Returns a clean list of uppercase ticker strings.
    Falls back to local CSV if the fetch fails.
    """

    # Re-read from .env on every call so a refreshed token written by app.py
    # is picked up without restarting the process.
    load_dotenv(override=True)
    token      = os.getenv("FINVIZ_API_TOKEN",  "") or FINVIZ_API_TOKEN
    export_url = os.getenv("FINVIZ_EXPORT_URL", "") or FINVIZ_EXPORT_URL

    if not export_url or not token:
        print("[TICKERS] Finviz credentials missing. Falling back to local CSV.")
        return []

    try:
        url = export_url + "&auth=" + token

        print("[TICKERS] Fetching ticker list from Finviz...")

        response = requests.get(url, timeout=15)

        if response.status_code != 200:
            print("[TICKERS] Finviz returned error " + str(response.status_code) + ". Falling back to local CSV.")
            return []

        # Parse CSV from response
        df = pd.read_csv(StringIO(response.text))

        if "Ticker" not in df.columns:
            print("[TICKERS] No Ticker column in Finviz response. Falling back to local CSV.")
            return []

        tickers = (
            df["Ticker"]
            .dropna()
            .str.strip()
            .str.upper()
            .unique()
            .tolist()
        )

        print("[TICKERS] Loaded " + str(len(tickers)) + " tickers from Finviz.")
        return tickers

    except Exception as e:
        print("[TICKERS] Finviz fetch failed: " + str(e) + ". Falling back to local CSV.")
        return []


def load_tickers_from_csv(csv_path: str) -> list:
    """
    Loads and validates a local Finviz CSV screener export.
    Used as fallback if Finviz API fetch fails.
    """

    if not os.path.exists(csv_path):
        print("[TICKERS] File not found: " + csv_path)
        return []

    try:
        df = pd.read_csv(csv_path)

        if "Ticker" not in df.columns:
            print("[TICKERS] No Ticker column found in CSV.")
            return []

        tickers = (
            df["Ticker"]
            .dropna()
            .str.strip()
            .str.upper()
            .unique()
            .tolist()
        )

        print("[TICKERS] Loaded " + str(len(tickers)) + " tickers from local CSV.")
        return tickers

    except Exception as e:
        print("[TICKERS] Failed to load CSV: " + str(e))
        return []


_last_good_tickers: list = []

def load_tickers() -> list:
    """
    Main ticker loader. Tries Finviz API first.
    On failure, returns the last successful Finviz fetch so stale CSV
    tickers never pollute the pipeline.
    """
    global _last_good_tickers
    tickers = load_tickers_from_finviz()
    if tickers:
        _last_good_tickers = tickers
        return tickers
    if _last_good_tickers:
        print("[TICKERS] Finviz fetch failed — reusing last known ticker list (" +
              str(len(_last_good_tickers)) + " tickers).")
        return _last_good_tickers
    print("[TICKERS] ERROR — Finviz fetch failed and no cached list available. " +
          "Check your FINVIZ_API_TOKEN and FINVIZ_EXPORT_URL in .env.")
    return []


# Quick test - only runs when you execute this file directly
if __name__ == "__main__":
    print("--- Timestamp Tests ---")
    print("Timestamp (primary index): " + get_timestamp())
    print("Minute bucket (density):   " + get_minute_bucket())
    print("UTC ISO (date arithmetic): " + get_utc_iso())

    print("")
    print("--- Rolling Window Tests ---")
    for window in [15, 60, 240]:
        start = get_window_start_iso(window)
        print(str(window) + " min window starts at: " + start)

    print("")
    print("--- Ticker Loader Test ---")
    tickers = load_tickers()
    print("Tickers found: " + str(tickers))