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
    # TTL on ts (a real BSON date — "timestamp"/"created_at_utc" are formatted strings,
    # which Mongo's TTL monitor can't act on) auto-deletes messages after 3 days. Nothing
    # anywhere reads scored_messages further back than 24h (the longest chart window), so
    # this is pure cleanup of an otherwise-unbounded collection.
    scored_messages.create_index("ts", expireAfterSeconds=259200)  # 3-day TTL

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
    # TTL on ts — same reasoning as scored_messages above. Only the most recent bucket
    # per ticker is ever read (get_density_count), so nothing depends on old buckets.
    message_density.create_index("ts", expireAfterSeconds=259200)  # 3-day TTL

    # ── price_history ──
    # TTL index on ts auto-deletes price snapshots after 7 days — keeps collection small.
    price_history.create_index("timestamp")
    price_history.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    # (ticker, ts) — supports the SMA exit-watch's per-ticker trailing-window query
    # (ts is the datetime field; the compound index above is on the string timestamp).
    price_history.create_index(
        [("ticker", ASCENDING), ("ts", DESCENDING)]
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
    auto_trades.create_index("source")
    auto_trades.create_index([("source", ASCENDING), ("status", ASCENDING)])

    # ── completed_auto_trades ──
    # exited_at is already a real BSON date (set from datetime.now(timezone.utc) at
    # exit time), so no new field is needed here, unlike scored_messages/message_density
    # above. This index used to be a plain (non-expiring) index — migrate it to a 3-day
    # TTL by dropping the old version first if its options don't already match, since
    # create_index errors on a key pattern that already exists with different options.
    existing_idx = completed_auto_trades.index_information()
    if "exited_at_1" in existing_idx and existing_idx["exited_at_1"].get("expireAfterSeconds") != 259200:
        completed_auto_trades.drop_index("exited_at_1")
    completed_auto_trades.create_index("exited_at", expireAfterSeconds=259200)  # 3-day TTL
    completed_auto_trades.create_index("ticker")
    completed_auto_trades.create_index([("source", ASCENDING), ("exited_at", DESCENDING)])
    completed_auto_trades.create_index("exited_at_et")
    completed_auto_trades.create_index("market_cap_bucket")

    # Backfill: every trade doc created before the manual-trade feature shipped has no
    # "source" field. They're unambiguously auto trades (manual trades never persisted
    # server-side before now), so tag them once — otherwise the source-scoped clear
    # endpoints (added alongside manual trades) silently stop matching old history.
    auto_trades.update_many({"source": {"$exists": False}}, {"$set": {"source": "auto"}})
    completed_auto_trades.update_many({"source": {"$exists": False}}, {"$set": {"source": "auto"}})

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