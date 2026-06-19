# fetcher.py
# Fetches raw messages from Stocktwits by impersonating a real browser
# Uses curl-cffi to bypass bot detection - no API token required
# Samuel Grana - Stocktwits News Sentiment Dashboard

import time
from datetime import datetime, timezone, timedelta
from curl_cffi import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"


def fetch_messages(ticker: str, limit: int = 30) -> list:
    """
    Fetches the most recent messages for a given ticker from Stocktwits.
    Impersonates Chrome browser to bypass bot detection.
    Returns a list of raw message dictionaries, or empty list if it fails.
    """

    url = BASE_URL.format(ticker=ticker.upper())
    params = {"limit": limit}

    try:
        response = requests.get(
            url,
            params=params,
            impersonate="chrome",
            timeout=10
        )

        if response.status_code == 429:
            print("[FETCHER] Rate limited on " + ticker + ". Slowing down.")
            return []

        if response.status_code != 200:
            print("[FETCHER] Error " + str(response.status_code) + " for " + ticker + ".")
            return []

        data = response.json()

        if "messages" not in data:
            print("[FETCHER] No messages found for " + ticker + ".")
            return []

        messages = data["messages"]
        print("[FETCHER] " + ticker + ": fetched " + str(len(messages)) + " messages.")
        return messages

    except Exception as e:
        print("[FETCHER] Request failed for " + ticker + ": " + str(e))
        return []


def fetch_messages_paginated(ticker: str, hours_back: int = 24, page_delay: float = 0.6) -> list:
    """
    Paginates backward through the Stocktwits stream to backfill history.
    Uses cursor.max from each response to walk to progressively older pages.
    Stops when messages are older than hours_back or Stocktwits signals no more pages.
    Returns all collected messages — DB deduplication handles any overlap.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    url    = BASE_URL.format(ticker=ticker.upper())

    all_messages = []
    max_id       = None
    page         = 0

    while True:
        page += 1
        params = {"limit": 30}
        if max_id:
            params["max"] = max_id

        try:
            response = requests.get(url, params=params, impersonate="chrome", timeout=10)

            if response.status_code == 429:
                print("[FETCHER] Rate limited on " + ticker + " during backfill. Stopping.")
                break

            if response.status_code != 200:
                print("[FETCHER] Error " + str(response.status_code) + " on " + ticker + " page " + str(page + 1) + ".")
                break

            data     = response.json()
            messages = data.get("messages", [])
            cursor   = data.get("cursor", {})

            if not messages:
                break

            all_messages.extend(messages)

            oldest_ts = messages[-1].get("created_at", "")
            print("[FETCHER] Page " + str(page + 1) + " | msgs=" + str(len(messages)) +
                  " | oldest=" + oldest_ts + " | cursor=" + str(cursor))

            # Stop once the oldest message on this page predates the cutoff
            if oldest_ts and oldest_ts < cutoff:
                print("[FETCHER] Cutoff reached.")
                break

            # Stop if Stocktwits signals no more pages
            if not cursor.get("more", False):
                print("[FETCHER] Stocktwits returned more=False — no further pages.")
                break

            max_id = cursor.get("max")
            if not max_id:
                print("[FETCHER] Stocktwits returned no cursor.max — cannot paginate further.")
                break

            time.sleep(page_delay)

        except Exception as e:
            print("[FETCHER] Backfill request failed for " + ticker + ": " + str(e))
            break

    print("[FETCHER] " + ticker + ": backfilled " + str(len(all_messages)) + " messages over " + str(page + 1) + " page(s).")
    return all_messages


# Quick test - only runs when you execute this file directly
if __name__ == "__main__":
    import json

    ticker = "AAPL"
    print("[TEST] Fetching messages for " + ticker + "...")

    messages = fetch_messages(ticker)

    if messages:
        print("[TEST] First message raw output:")
        print(json.dumps(messages[0], indent=2))
    else:
        print("[TEST] No messages returned.")