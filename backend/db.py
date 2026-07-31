# db.py
# MongoDB connection and collection setup
# Samuel Grana - Stocktwits News Sentiment Dashboard

from pymongo import MongoClient, ASCENDING, DESCENDING
from dotenv import load_dotenv
import os

load_dotenv()

# ── CONNECTION CONFIG ──
# MONGO_URI supports both local and Atlas URIs; falls back to localhost for dev.
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGO_URL") or "mongodb://localhost:27017"
DB_NAME   = os.getenv("MONGO_DB_NAME", "stocktwits_dashboard")

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

# ── COLLECTION HANDLES ──
# Each variable is a live PyMongo collection object — pass it to find/insert/aggregate.
# Collection handles
scored_messages       = db["scored_messages"]
message_density       = db["message_density"]
price_history         = db["price_history"]
upcoming_catalysts    = db["upcoming_catalysts"]
auto_trades           = db["auto_trades"]
completed_auto_trades = db["completed_auto_trades"]
screener_snapshots    = db["screener_snapshots"]
pipeline_events       = db["pipeline_events"]


def ensure_indexes():
    """
    Creates all required MongoDB indexes.
    Called once at app startup.
    """

    # ── scored_messages ──
    # Compound (ticker, timestamp) supports fast per-ticker sorted queries.
    # message_id unique index silently rejects duplicate Stocktwits messages.
    scored_messages.create_index("timestamp")
    scored_messages.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    scored_messages.create_index("message_id", unique=True)

    # ── message_density ──
    # Unique (ticker, timestamp) compound index prevents double-counting
    # when the pipeline loops and calls update_density twice for the same minute.
    message_density.create_index("timestamp")
    message_density.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    message_density.create_index(
        [("ticker", ASCENDING), ("timestamp", ASCENDING)],
        unique=True
    )

    # ── price_history ──
    # TTL index on ts auto-deletes price snapshots after 7 days — keeps collection small.
    price_history.create_index("timestamp")
    price_history.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    price_history.create_index("ts", expireAfterSeconds=604800)  # 7-day TTL

    # ── upcoming_catalysts ──
    # TTL on fetched_at purges stale catalyst data after 30 days.
    upcoming_catalysts.create_index([("ticker", ASCENDING), ("event_type", ASCENDING)])
    upcoming_catalysts.create_index("date")
    upcoming_catalysts.create_index("fetched_at", expireAfterSeconds=2592000)  # 30-day TTL

    # ── auto_trades ──
    # (ticker, status) compound index supports fast active-trade lookup in _check_auto_trades.
    auto_trades.create_index([("ticker", ASCENDING), ("status", ASCENDING)])
    auto_trades.create_index("entered_at")

    # ── completed_auto_trades ──
    completed_auto_trades.create_index("exited_at")
    completed_auto_trades.create_index("ticker")

    # ── screener_snapshots ──
    # TTL on ts auto-deletes bubble trail points after 24 hours — intraday only.
    screener_snapshots.create_index([("ticker", ASCENDING), ("ts", DESCENDING)])
    screener_snapshots.create_index("ts", expireAfterSeconds=86400)  # 1-day TTL

    # ── pipeline_events ──
    # Durable lifecycle log (started/crashed/watchdog_restart/stopped_manually) so
    # pipeline history survives process restarts and redeploys — unlike the in-memory
    # pipeline_health dict and Railway's own ephemeral log stream, both of which get
    # wiped by the exact events we need to diagnose. TTL keeps 14 days of history.
    pipeline_events.create_index("ts", expireAfterSeconds=1209600)  # 14-day TTL

    print("[DB] All indexes verified successfully.")


if __name__ == "__main__":
    ensure_indexes()
    print("[DB] Connected to:", DB_NAME)
    print("[DB] Collections:", db.list_collection_names())