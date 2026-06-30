# db.py
# MongoDB connection and collection setup
# Samuel Grana - Stocktwits News Sentiment Dashboard

from pymongo import MongoClient, ASCENDING, DESCENDING
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGO_URL") or "mongodb://localhost:27017"
DB_NAME   = os.getenv("MONGO_DB_NAME", "stocktwits_dashboard")

print(f"[DB] MONGO_URI={MONGO_URI[:40]}...")
print(f"[DB] ENV CHECK: MONGO_URI_RAW={os.getenv('MONGO_URI','MISSING')} MONGO_URL_RAW={os.getenv('MONGO_URL','MISSING')} RAILWAY_ENV={os.getenv('RAILWAY_ENVIRONMENT_NAME','MISSING')}")

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

# Collection handles
scored_messages    = db["scored_messages"]
message_density    = db["message_density"]
price_history      = db["price_history"]
upcoming_catalysts = db["upcoming_catalysts"]


def ensure_indexes():
    """
    Creates all required MongoDB indexes.
    Called once at app startup.
    """

    # scored_messages indexes
    scored_messages.create_index("timestamp")
    scored_messages.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    scored_messages.create_index("message_id", unique=True)

    # message_density indexes
    message_density.create_index("timestamp")
    message_density.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    message_density.create_index(
        [("ticker", ASCENDING), ("timestamp", ASCENDING)],
        unique=True
    )

    # price_history indexes
    price_history.create_index("timestamp")
    price_history.create_index(
        [("ticker", ASCENDING), ("timestamp", DESCENDING)]
    )
    price_history.create_index("ts", expireAfterSeconds=604800)  # 7-day TTL

    # upcoming_catalysts indexes
    upcoming_catalysts.create_index([("ticker", ASCENDING), ("event_type", ASCENDING)])
    upcoming_catalysts.create_index("date")
    upcoming_catalysts.create_index("fetched_at", expireAfterSeconds=2592000)  # 30-day TTL

    print("[DB] All indexes verified successfully.")


if __name__ == "__main__":
    ensure_indexes()
    print("[DB] Connected to:", DB_NAME)
    print("[DB] Collections:", db.list_collection_names())