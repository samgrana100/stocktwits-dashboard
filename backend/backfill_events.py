# backfill_events.py
# Retroactively runs event detection on historical MongoDB messages
# that were scored before event_detector was added (event_type is null/missing).
#
# Usage:
#   python backfill_events.py               # backfill all untagged messages
#   python backfill_events.py --dry-run     # preview counts, no writes
#   python backfill_events.py --summary     # show event counts in the whole DB
#   python backfill_events.py --search "FDA Approval"   # print matching messages

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Windows terminals default to cp1252 — force UTF-8 so pill symbols print cleanly
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pymongo import UpdateOne
from db import scored_messages
from event_detector import detect_event, EVENT_META


BATCH_SIZE = 500


def backfill(dry_run=False):
    """Runs detect_event on every message that has no event_type tag yet."""

    query = {
        "body": {"$exists": True, "$ne": ""},
        "$or": [
            {"event_type": {"$exists": False}},
            {"event_type": None},
        ],
    }

    total = scored_messages.count_documents(query)
    print(f"[BACKFILL] {total:,} messages to process")
    if total == 0:
        print("[BACKFILL] Nothing to do.")
        return

    found     = 0
    processed = 0
    ops       = []
    t0        = time.time()

    cursor = scored_messages.find(query, {"body": 1, "_id": 1}).batch_size(BATCH_SIZE)

    for doc in cursor:
        body       = (doc.get("body") or "").strip()
        event_type = detect_event(body)

        if event_type:
            found += 1

        ops.append(UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"event_type": event_type}}
        ))

        processed += 1

        if len(ops) >= BATCH_SIZE:
            if not dry_run:
                scored_messages.bulk_write(ops, ordered=False)
            ops = []
            elapsed = time.time() - t0
            rate    = processed / elapsed
            eta     = (total - processed) / rate if rate > 0 else 0
            print(f"  {processed:,} / {total:,}  |  {rate:.0f} docs/s  |  ETA {eta:.0f}s")

    if ops and not dry_run:
        scored_messages.bulk_write(ops, ordered=False)

    elapsed = time.time() - t0
    print(f"\n[BACKFILL] Finished in {elapsed:.1f}s")
    print(f"  Events detected : {found:,}")
    print(f"  No event        : {total - found:,}")
    if dry_run:
        print("  (dry-run — nothing written)")


def summary():
    """Shows a count of each event_type currently tagged in the DB."""

    pipeline = [
        {"$match": {"event_type": {"$ne": None}}},
        {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    results = list(scored_messages.aggregate(pipeline))

    if not results:
        print("[SUMMARY] No event-tagged messages found.")
        return

    print(f"\n{'Event':<22}  {'Count':>6}  Pill")
    print("-" * 40)
    for r in results:
        ev   = r["_id"]
        cnt  = r["count"]
        meta = EVENT_META.get(ev, (None, "?"))
        pill = meta[1] if isinstance(meta, tuple) else meta.get("pill", "?")
        print(f"{ev:<22}  {cnt:>6,}  {pill}")

    total = scored_messages.count_documents({"event_type": {"$ne": None}})
    print(f"\nTotal tagged messages: {total:,}")


def search(event_type, limit=20):
    """Prints the most recent messages tagged with a specific event type."""

    docs = list(
        scored_messages.find(
            {"event_type": event_type, "body": {"$exists": True, "$ne": ""}},
            {"ticker": 1, "body": 1, "created_at_utc": 1, "composite_score": 1, "_id": 0}
        )
        .sort("created_at_utc", -1)
        .limit(limit)
    )

    if not docs:
        print(f"[SEARCH] No messages found for event_type='{event_type}'")
        return

    print(f"\n[SEARCH] {len(docs)} most recent '{event_type}' messages:\n")
    for d in docs:
        score     = d.get("composite_score")
        score_str = f"{score:+.4f}" if score is not None else "  n/a "
        ts        = d.get("created_at_utc", "")[:16]
        body      = (d.get("body") or "").replace("\n", " ")[:120]
        print(f"  [{d.get('ticker','?'):6}]  {ts}  score={score_str}  {body}")


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--summary" in args:
        summary()

    elif "--search" in args:
        idx = args.index("--search")
        if idx + 1 >= len(args):
            print('Usage: python backfill_events.py --search "Event Type"')
            sys.exit(1)
        search(args[idx + 1])

    else:
        dry_run = "--dry-run" in args
        if dry_run:
            print("[BACKFILL] Dry-run mode — no writes will happen")
        backfill(dry_run=dry_run)
        print()
        summary()
