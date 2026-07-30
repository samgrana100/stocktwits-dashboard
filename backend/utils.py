# utils.py
# Shared utility functions used across the entire project
# Samuel Grana - Stocktwits News Sentiment Dashboard

from datetime import datetime, timezone, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO
from curl_cffi import requests as cffi_requests

load_dotenv()

# ── CREDENTIALS ──
# Read at import time as defaults; load_tickers_from_finviz re-reads from os.environ on each
# call so a token hot-swapped by app.py is picked up without restarting the process.
FINVIZ_API_TOKEN  = os.getenv("FINVIZ_API_TOKEN",  "")
FINVIZ_EXPORT_URL = os.getenv("FINVIZ_EXPORT_URL", "")


# ── TIMESTAMP HELPERS ──

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


def is_regular_market_hours() -> bool:
    """
    True during NYSE regular session (9:30am-4:00pm ET, Mon-Fri).
    The Finviz screener's Price column only reflects the regular-session quote —
    it freezes at the close and doesn't track pre/post-market trades. Callers use
    this to decide when to trust that price vs. falling back to a live source.
    """
    now_utc   = datetime.now(timezone.utc)
    et_offset = -4 if 3 <= now_utc.month <= 11 else -5  # rough EDT/EST split
    now_et    = now_utc + timedelta(hours=et_offset)
    if now_et.weekday() >= 5:
        return False
    market_open  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now_et < market_close


# ── TICKER LOADER ──

def load_tickers_from_finviz() -> list:
    """
    Fetches a fresh ticker list directly from Finviz Elite API.
    Uses the export URL and API token from .env file.
    Returns a clean list of uppercase ticker strings.
    Falls back to local CSV if the fetch fails.
    """

    # Read from environment — app.py writes the refreshed token to os.environ,
    # so os.getenv picks up the latest value without re-reading the file.
    token      = os.getenv("FINVIZ_API_TOKEN",  "") or FINVIZ_API_TOKEN
    export_url = os.getenv("FINVIZ_EXPORT_URL", "") or FINVIZ_EXPORT_URL

    if not export_url or not token:
        print("[TICKERS] Finviz credentials missing. Falling back to local CSV.")
        return []

    try:
        url = export_url + "&auth=" + token

        print("[TICKERS] Fetching ticker list from Finviz...")

        response = cffi_requests.get(url, impersonate="chrome120", timeout=15)

        if response.status_code == 401:
            print("[TICKERS] Finviz 401 — attempting token refresh...")
            try:
                from app import refresh_finviz_token
                if refresh_finviz_token():
                    token = os.getenv("FINVIZ_API_TOKEN", "") or FINVIZ_API_TOKEN
                    url   = export_url + "&auth=" + token
                    response = cffi_requests.get(url, impersonate="chrome120", timeout=15)
                    if response.status_code != 200:
                        print("[TICKERS] Retry after refresh still failed (" + str(response.status_code) + ").")
                        return []
                else:
                    return []
            except Exception as e:
                print("[TICKERS] Token refresh error: " + str(e))
                return []

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

        if "Price" in df.columns:
            global _last_good_prices
            prices = {}
            for _, row in df.iterrows():
                t = str(row.get("Ticker", "")).strip().upper()
                if not t:
                    continue
                try:
                    prices[t] = float(str(row.get("Price", "")).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            if prices:
                _last_good_prices = prices
                print("[TICKERS] Captured prices for " + str(len(prices)) + "/" + str(len(tickers)) + " tickers from Finviz Price column.")
            else:
                print("[TICKERS] Price column present but 0 values parsed — check Price format in Finviz export.")
        else:
            print("[TICKERS] No Price column in Finviz export (columns: " + ", ".join(df.columns) + ") — correlation price_history will not be written.")

        print("[TICKERS] Loaded " + str(len(tickers)) + " tickers from Finviz.")
        return tickers

    except Exception as e:
        print("[TICKERS] Finviz fetch failed: " + str(e) + ". Falling back to local CSV.")
        return []


# Stale-cache guard: if Finviz is temporarily unreachable, the pipeline
# re-uses the most recent good list instead of stopping cold.
_last_good_tickers: list = []
_last_good_prices:  dict = {}

def get_last_prices() -> dict:
    """Returns the most recently fetched {ticker: price} map from the Finviz screener CSV."""
    return _last_good_prices


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