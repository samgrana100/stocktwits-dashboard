# score_calculator.py
# Reads unscored messages from MongoDB, runs all three agents,
# applies the composite formula, and writes scores back to the database
# Also aggregates scores per ticker for the dashboard ranked table
# Samuel Grana - Stocktwits News Sentiment Dashboard

import sys
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import scored_messages, message_density, price_history
from agents.trust_agent import calculate_trust_score
from agents.impact_agent import calculate_impact_score
from agents.sentiment_agent import calculate_sentiment_scores_batch
from utils import get_timestamp, get_window_start_iso
from event_detector import detect_event


def get_density_count(ticker: str) -> int:
    """Gets the most recent message count for a ticker from the density collection."""
    try:
        result = message_density.find_one(
            {"ticker": ticker},
            sort=[("timestamp", -1)]
        )
        if result:
            return result.get("message_count", 0)
        return 0
    except Exception:
        return 0


def score_unscored_messages(batch_size: int = 50):
    """
    Finds messages in MongoDB where composite_score is null,
    runs them through all three agents, and writes the scores back.
    Prioritizes newest messages first.
    """

    unscored = list(
        scored_messages.find(
            {"composite_score": None}
        ).sort("created_at_utc", -1).limit(batch_size)
    )

    if not unscored:
        print("[SCORER] No unscored messages found.")
        return

    # Separate scoreable messages from empty-body ones before hitting FinBERT
    scoreable_idx = []
    for i, msg in enumerate(unscored):
        body = msg.get("body") or ""
        if body.strip():
            scoreable_idx.append(i)
        else:
            # No text to analyze — clear from queue with zero scores
            scored_messages.update_one(
                {"_id": msg["_id"]},
                {"$set": {"trust_score": 0.0, "sentiment_score": 0.0,
                           "impact_score": 0.0, "composite_score": 0.0,
                           "scored_at": get_timestamp()}}
            )

    scoreable_msgs = [unscored[i] for i in scoreable_idx]
    print("[SCORER] Scoring " + str(len(scoreable_msgs)) + " messages (" +
          str(len(unscored) - len(scoreable_msgs)) + " empty skipped)...")

    if not scoreable_msgs:
        return

    # Single FinBERT forward pass for all scoreable messages
    bodies           = [unscored[i].get("body") or "" for i in scoreable_idx]
    sentiment_scores = calculate_sentiment_scores_batch(bodies)

    scored_count = 0
    error_count  = 0

    for j, msg in enumerate(scoreable_msgs):
        try:
            ticker = msg.get("ticker", "")
            user   = msg.get("user", {})

            trust_score     = calculate_trust_score(user)
            sentiment_score = sentiment_scores[j]
            density_count   = get_density_count(ticker)
            impact_score    = calculate_impact_score(msg, density_count)
            quality         = 0.40 + (trust_score * 0.35) + (impact_score * 0.25)
            composite_score = round(sentiment_score * quality, 4)

            event_type = detect_event(msg.get("body", ""))

            scored_messages.update_one(
                {"_id": msg["_id"]},
                {
                    "$set": {
                        "trust_score":     trust_score,
                        "sentiment_score": sentiment_score,
                        "impact_score":    impact_score,
                        "composite_score": composite_score,
                        "event_type":      event_type,
                        "scored_at":       get_timestamp()
                    }
                }
            )
            scored_count += 1

        except Exception as e:
            print("[SCORER] Error scoring message: " + str(e) + " — marking as unscoreable.")
            scored_messages.update_one(
                {"_id": msg["_id"]},
                {"$set": {"trust_score": 0.0, "sentiment_score": 0.0,
                           "impact_score": 0.0, "composite_score": 0.0,
                           "scored_at": get_timestamp()}}
            )
            error_count += 1
            continue

    print("[SCORER] Done. Scored: " + str(scored_count) + " | Errors: " + str(error_count))


DENSITY_WINDOW_MIN = 60   # rolling window used to compute density at each point
CORR_LOOKBACK_MIN  = 30   # how many minutes back to look for price snapshots
CORR_FETCH_MIN     = CORR_LOOKBACK_MIN + DENSITY_WINDOW_MIN  # total message history needed
CORR_MIN_POINTS    = 5    # minimum price points required to compute correlation

def _pearson_r(x, y):
    n = len(x)
    if n < CORR_MIN_POINTS:
        return None
    mx, my = sum(x) / n, sum(y) / n
    num  = sum((a - mx) * (b - my) for a, b in zip(x, y))
    denx = sum((a - mx) ** 2 for a in x) ** 0.5
    deny = sum((b - my) ** 2 for b in y) ** 0.5
    if denx == 0 or deny == 0:
        return None
    return round(num / (denx * deny), 4)


def _bucket_to_minute_key(bucket):
    """'DD:MM:YYYY:HH:MM:00' → 'YYYY-MM-DDTHH:MM'  (aligns with created_at_utc prefix)."""
    p = bucket.split(":")
    return f"{p[2]}-{p[1]}-{p[0]}T{p[3]}:{p[4]}"


def aggregate_ticker_scores(rolling_window_minutes: int = 60) -> list:
    """
    Aggregates composite scores per ticker within the rolling window.
    Uses simple averages across all scored messages in the window.
    Also computes per-ticker Pearson correlation between message density and price.
    """

    window_start = get_window_start_iso(rolling_window_minutes)

    scored_pipeline = [
        {
            "$match": {
                "created_at_utc":  {"$gte": window_start},
                "composite_score": {"$ne": None}
            }
        },
        {
            "$project": {
                "ticker":          1,
                "composite_score": 1,
                "sentiment_score": 1,
                "trust_score":     1,
                "impact_score":    1
            }
        }
    ]

    # Total message count per ticker (scored and unscored)
    total_pipeline = [
        {"$match": {"created_at_utc": {"$gte": window_start}}},
        {"$group": {"_id": "$ticker", "total_messages": {"$sum": 1}}}
    ]

    raw_msgs      = list(scored_messages.aggregate(scored_pipeline))
    total_results = list(scored_messages.aggregate(total_pipeline))
    total_map     = {r["_id"]: r["total_messages"] for r in total_results}

    # Catalyst events per ticker in window
    catalyst_results = list(scored_messages.aggregate([
        {"$match": {"created_at_utc": {"$gte": window_start}, "event_type": {"$ne": None}}},
        {"$group": {"_id": "$ticker", "events": {"$addToSet": "$event_type"}}}
    ]))
    catalyst_map = {r["_id"]: r["events"] for r in catalyst_results}

    # ── Surge: last 5m vs last 1h
    surge_5m_map = {
        r["_id"]: r["count"]
        for r in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": get_window_start_iso(5)}}},
            {"$group": {"_id": "$ticker", "count": {"$sum": 1}}}
        ])
    }
    surge_1h_map = {
        r["_id"]: r["count"]
        for r in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": get_window_start_iso(60)}}},
            {"$group": {"_id": "$ticker", "count": {"$sum": 1}}}
        ])
    }

    # ── Density-price correlation
    # Fetch CORR_FETCH_MIN of message history so each price point can have a
    # full DENSITY_WINDOW_MIN rolling count computed behind it.
    corr_start_iso      = get_window_start_iso(CORR_FETCH_MIN)
    corr_price_start_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=CORR_LOOKBACK_MIN)

    corr_density_docs = list(scored_messages.aggregate([
        {"$match": {"created_at_utc": {"$gte": corr_start_iso}}},
        {"$group": {
            "_id": {"ticker": "$ticker", "minute": {"$substr": ["$created_at_utc", 0, 16]}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.minute": 1}}
    ]))
    corr_price_docs = list(price_history.find(
        {"ts": {"$gte": corr_price_start_dt}},
        {"ticker": 1, "price": 1, "minute_bucket": 1}
    ))

    corr_density_by_ticker = defaultdict(dict)
    for doc in corr_density_docs:
        corr_density_by_ticker[doc["_id"]["ticker"]][doc["_id"]["minute"]] = doc["count"]

    corr_price_by_ticker = defaultdict(dict)
    for doc in corr_price_docs:
        mk = _bucket_to_minute_key(doc["minute_bucket"])
        corr_price_by_ticker[doc["ticker"]][mk] = doc["price"]

    def _compute_correlation(ticker):
        d_map         = corr_density_by_ticker.get(ticker, {})
        p_map         = corr_price_by_ticker.get(ticker, {})
        price_minutes = sorted(p_map.keys())[-10:]  # last 10 price snapshots
        if len(price_minutes) < CORR_MIN_POINTS:
            return None, 0.0, False, False

        d_vec, p_vec = [], []
        for m in price_minutes:
            m_dt    = datetime.strptime(m, "%Y-%m-%dT%H:%M")
            rolling = sum(
                d_map.get((m_dt - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M"), 0)
                for i in range(DENSITY_WINDOW_MIN)
            )
            d_vec.append(rolling)
            p_vec.append(p_map[m])

        r       = _pearson_r(d_vec, p_vec)
        peak    = max(d_vec)
        current = d_vec[-1]
        pct_from_peak = round((peak - current) / peak, 4) if peak > 0 else 0.0

        # 3-point average trend check — smooths single outlier minutes
        early_d = sum(d_vec[:3]) / 3
        late_d  = sum(d_vec[-3:]) / 3
        early_p = sum(p_vec[:3]) / 3
        late_p  = sum(p_vec[-3:]) / 3
        density_rising = late_d > early_d
        price_rising   = late_p > early_p

        return r, pct_from_peak, price_rising, density_rising

    # Group by ticker
    ticker_msgs = defaultdict(list)
    for msg in raw_msgs:
        ticker_msgs[msg["ticker"]].append(msg)

    ticker_scores  = []
    scored_tickers = set(ticker_msgs.keys())

    for ticker, msgs in ticker_msgs.items():
        n = len(msgs)

        avg_composite = sum(m["composite_score"]            for m in msgs) / n
        avg_sentiment = sum((m.get("sentiment_score") or 0) for m in msgs) / n
        avg_trust     = sum((m.get("trust_score")     or 0) for m in msgs) / n
        avg_impact    = sum((m.get("impact_score")    or 0) for m in msgs) / n

        bullish_count = sum(1 for m in msgs if m["composite_score"] >  0.05)
        bearish_count = sum(1 for m in msgs if m["composite_score"] < -0.05)
        sig_count     = bullish_count + bearish_count
        bull_pct      = round(bullish_count / sig_count * 100, 1) if sig_count > 0 else 0.0
        bear_pct      = round(bearish_count / sig_count * 100, 1) if sig_count > 0 else 0.0

        count_5m    = surge_5m_map.get(ticker, 0)
        count_1h    = surge_1h_map.get(ticker, 0)
        expected_5m = count_1h / 12
        surge_ratio = round(count_5m / expected_5m, 1) if expected_5m > 0 and count_5m >= 3 else 0.0

        corr, pct_from_peak, price_rising, density_rising = _compute_correlation(ticker)

        ticker_scores.append({
            "ticker":                ticker,
            "avg_composite_score":   round(avg_composite, 4),
            "avg_sentiment_score":   round(avg_sentiment, 4),
            "avg_trust_score":       round(avg_trust,     4),
            "avg_impact_score":      round(avg_impact,    4),
            "message_count":         len(msgs),
            "scored_messages":       len(msgs),
            "total_messages":        total_map.get(ticker, len(msgs)),
            "bullish_count":         bullish_count,
            "bearish_count":         bearish_count,
            "bull_pct":              bull_pct,
            "bear_pct":              bear_pct,
            "surge_ratio":           surge_ratio,
            "catalyst_events":       catalyst_map.get(ticker, []),
            "rolling_window_minutes": rolling_window_minutes,
            "signal":                None,
            "correlation":           corr,
            "density_pct_from_peak": pct_from_peak,
            "price_rising":          price_rising,
            "density_rising":        density_rising,
        })

    # Add tickers with messages but no scored messages yet
    for ticker in (set(total_map.keys()) - scored_tickers):
        count_5m    = surge_5m_map.get(ticker, 0)
        count_1h    = surge_1h_map.get(ticker, 0)
        expected_5m = count_1h / 12
        surge_ratio = round(count_5m / expected_5m, 1) if expected_5m > 0 and count_5m >= 3 else 0.0

        corr, pct_from_peak, price_rising, density_rising = _compute_correlation(ticker)

        ticker_scores.append({
            "ticker":                ticker,
            "avg_composite_score":   0.0,
            "avg_sentiment_score":   0.0,
            "avg_trust_score":       0.0,
            "avg_impact_score":      0.0,
            "message_count":         0,
            "scored_messages":       0,
            "total_messages":        total_map.get(ticker, 0),
            "bullish_count":         0,
            "bearish_count":         0,
            "bull_pct":              0.0,
            "bear_pct":              0.0,
            "surge_ratio":           surge_ratio,
            "catalyst_events":       catalyst_map.get(ticker, []),
            "rolling_window_minutes": rolling_window_minutes,
            "signal":                None,
            "correlation":           corr,
            "density_pct_from_peak": pct_from_peak,
            "price_rising":          price_rising,
            "density_rising":        density_rising,
        })

    # Scored tickers first, ranked by weighted composite score; unscored at bottom
    ticker_scores.sort(key=lambda x: (x["scored_messages"] == 0, -x["avg_composite_score"]))

    return ticker_scores


# Runs when you execute this file directly
if __name__ == "__main__":
    import time as t

    print("[AGGREGATOR] Testing ticker score aggregation...")
    scores = aggregate_ticker_scores(rolling_window_minutes=60)
    if scores:
        print("[AGGREGATOR] Top 5 tickers by composite score:")
        for ticker in scores[:5]:
            print("  " + ticker["ticker"] +
                  " | composite: " + str(ticker["avg_composite_score"]) +
                  " | scored: "    + str(ticker["scored_messages"]) +
                  " | total: "     + str(ticker["total_messages"]))
    else:
        print("[AGGREGATOR] No messages found in window.")

    print("")
    print("[SCORER] Starting continuous scoring loop...")
    while True:
        score_unscored_messages(batch_size=50)
        t.sleep(10)