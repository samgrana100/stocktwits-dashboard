# pipeline.py
# Main data ingestion loop - runs continuously until stopped
# Fetches messages for every ticker and stores them in MongoDB
# Samuel Grana - Stocktwits News Sentiment Dashboard

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from fetcher import fetch_messages, fetch_messages_paginated
from utils import load_tickers, get_timestamp, get_minute_bucket, get_utc_iso
from db import scored_messages, message_density, price_history

# --- Configuration ---
LOOP_INTERVAL   = 90
PIPELINE_WORKERS = 5   # concurrent ticker threads — raise cautiously to avoid Stocktwits 429s
CSV_PATH = "data/screener.csv"

_seen_lock = threading.Lock()


def _process_ticker(ticker: str, seen_tickers: set, rolling_window: int, stop_flag: list):
    if stop_flag[0]:
        return

    with _seen_lock:
        is_new = ticker not in seen_tickers
        if is_new:
            seen_tickers.add(ticker)

    if is_new:
        print("[PIPELINE] New ticker " + ticker + " — backfilling 24h history...")
        historical = fetch_messages_paginated(ticker, hours_back=24)
        if historical:
            store_messages(ticker, historical, rolling_window)

    messages = fetch_messages(ticker)
    if messages:
        store_messages(ticker, messages, rolling_window)
        update_density(ticker, len(messages), rolling_window)
        store_price_snapshot(ticker, messages)


def run_pipeline(rolling_window: int = 60, stop_flag: list = [False]):
    """
    Main ingestion loop. Runs continuously until stop_flag[0] is set to True.
    Fetches PIPELINE_WORKERS tickers concurrently to reduce loop cycle time.
    """

    print("[PIPELINE] Starting ingestion loop.")
    print("[PIPELINE] Rolling window: " + str(rolling_window) + " minutes.")
    print("[PIPELINE] Workers: " + str(PIPELINE_WORKERS) + " concurrent.\n")

    seen_tickers = set()

    while not stop_flag[0]:
        loop_start = time.time()

        tickers = load_tickers()

        if not tickers:
            print("[PIPELINE] No tickers found. Check your screener filters.")
            time.sleep(LOOP_INTERVAL)
            continue

        print("[PIPELINE] Loop started at " + get_timestamp())
        print("[PIPELINE] Tickers: " + str(len(tickers)) + " | Workers: " + str(PIPELINE_WORKERS) + "\n")

        with ThreadPoolExecutor(max_workers=PIPELINE_WORKERS) as executor:
            futures = {
                executor.submit(_process_ticker, t, seen_tickers, rolling_window, stop_flag): t
                for t in tickers
            }
            for future in as_completed(futures):
                if stop_flag[0]:
                    break
                try:
                    future.result()
                except Exception as e:
                    t = futures[future]
                    print("[PIPELINE] Error on " + t + ": " + str(e))

        elapsed = time.time() - loop_start
        wait_time = max(0, LOOP_INTERVAL - elapsed)

        print("\n[PIPELINE] Loop finished in " + str(round(elapsed, 1)) + "s.")

        if wait_time > 0:
            print("[PIPELINE] Waiting " + str(round(wait_time, 1)) + "s before next loop.\n")
            time.sleep(wait_time)


def store_messages(ticker: str, messages: list, rolling_window: int):
    """
    Stores fetched messages into MongoDB scored_messages collection.
    Skips duplicates automatically using the unique message_id index.
    """

    inserted = 0
    skipped = 0

    for msg in messages:
        document = {
            "timestamp": get_timestamp(),
            "ticker": ticker,
            "message_id": msg.get("id"),
            "body": msg.get("body", ""),
            "sentiment_score": None,
            "trust_score": None,
            "impact_score": None,
            "composite_score": None,
            "rolling_window_minutes": rolling_window,
            "stocktwits_sentiment": msg.get("entities", {}).get("sentiment"),
            "created_at_utc": msg.get("created_at", ""),
            "symbols": msg.get("symbols", []),
            "user": {
                "id": msg.get("user", {}).get("id"),
                "username": msg.get("user", {}).get("username"),
                "followers": msg.get("user", {}).get("followers", 0),
                "following": msg.get("user", {}).get("following", 0),
                "ideas": msg.get("user", {}).get("ideas", 0),
                "join_date": msg.get("user", {}).get("join_date", ""),
                "official": msg.get("user", {}).get("official", False),
                "classification": msg.get("user", {}).get("classification", [])
            }
        }

        try:
            scored_messages.insert_one(document)
            inserted += 1
        except Exception:
            skipped += 1

    print("[STORE] " + ticker + ": " + str(inserted) + " new, " + str(skipped) + " duplicate.")


def update_density(ticker: str, message_count: int, rolling_window: int):
    """
    Updates the message density counter for this ticker in the current minute bucket.
    """

    bucket = get_minute_bucket()

    try:
        message_density.update_one(
            {
                "ticker": ticker,
                "timestamp": bucket
            },
            {
                "$inc": {"message_count": message_count},
                "$set": {
                    "rolling_window_minutes": rolling_window,
                    "last_updated": get_timestamp()
                }
            },
            upsert=True
        )
    except Exception as e:
        print("[DENSITY] Failed to update density for " + ticker + ": " + str(e))


def store_price_snapshot(ticker: str, messages: list):
    """
    Extracts the current price from the first message that has price data
    and stores it as a snapshot in the price_history collection.
    Called once per ticker per loop.
    """

    price = None

    for msg in messages:
        prices = msg.get("prices", [])
        for p in prices:
            if p.get("symbol") == ticker:
                try:
                    price = float(p.get("price", 0))
                    break
                except Exception:
                    continue
        if price:
            break

    if not price:
        return

    bucket = get_minute_bucket()
    try:
        price_history.update_one(
            {"ticker": ticker, "minute_bucket": bucket},
            {"$set": {
                "price":          price,
                "timestamp":      get_timestamp(),
                "created_at_utc": get_utc_iso(),
                "ts":             datetime.now(timezone.utc),
            }},
            upsert=True
        )
    except Exception as e:
        print("[PRICE] Failed to store price for " + ticker + ": " + str(e))


# Quick test - only runs when you execute this file directly
if __name__ == "__main__":
    run_pipeline(rolling_window=60)