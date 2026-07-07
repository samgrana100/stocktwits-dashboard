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


def aggregate_ticker_scores(rolling_window_minutes: int = 60) -> list:
    """
    Aggregates composite scores per ticker within the rolling window.
    Uses simple averages across all scored messages in the window.
    Also returns bull/bear signal breakdown.
    """

    window_start     = get_window_start_iso(rolling_window_minutes)
    short_window     = max(1, rolling_window_minutes // 12)
    short_start      = get_window_start_iso(short_window)
    rolling_start_dt = datetime.now(timezone.utc) - timedelta(minutes=rolling_window_minutes)

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

    # Threshold for "high density" tickers (used by peak_fade signal)
    total_counts           = list(total_map.values())
    mean_messages          = sum(total_counts) / len(total_counts) if total_counts else 1
    high_density_threshold = max(10, mean_messages * 1.25)

    # Catalyst events per ticker in window
    catalyst_results = list(scored_messages.aggregate([
        {"$match": {"created_at_utc": {"$gte": window_start}, "event_type": {"$ne": None}}},
        {"$group": {"_id": "$ticker", "events": {"$addToSet": "$event_type"}}}
    ]))
    catalyst_map = {r["_id"]: r["events"] for r in catalyst_results}

    # ── Short-term counts per ticker (for density direction)
    short_count_map = {
        r["_id"]: r["count"]
        for r in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": short_start}}},
            {"$group": {"_id": "$ticker", "count": {"$sum": 1}}}
        ])
    }

    # ── Previous short window [2x ago → 1x ago] — confirms trend direction
    # If recent < previous, density is already rolling over; don't fire "up"
    prev_short_start = get_window_start_iso(2 * short_window)
    prev_count_map = {
        r["_id"]: r["count"]
        for r in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": prev_short_start, "$lt": short_start}}},
            {"$group": {"_id": "$ticker", "count": {"$sum": 1}}}
        ])
    }

    # ── Short-term sentiment averages per ticker (for sentiment direction)
    short_sent_map = {
        r["_id"]: {"avg": r["avg_score"], "n": r["n"]}
        for r in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": short_start}, "composite_score": {"$ne": None}}},
            {"$group": {"_id": "$ticker", "avg_score": {"$avg": "$composite_score"}, "n": {"$sum": 1}}}
        ])
    }

    # ── Intraday price trend from MongoDB price_history (for price direction)
    price_docs = list(price_history.find(
        {"ts": {"$gte": rolling_start_dt}},
        {"ticker": 1, "price": 1, "ts": 1}
    ).sort("ts", 1))
    price_by_ticker = defaultdict(list)
    for doc in price_docs:
        price_by_ticker[doc["ticker"]].append(doc["price"])
    price_trend_map = {}
    for t, prices in price_by_ticker.items():
        if len(prices) >= 2:
            oldest, newest = prices[0], prices[-1]
            price_trend_map[t] = round((newest - oldest) / oldest * 100, 2) if oldest > 0 else 0.0

    # ── Short-window price direction: recent short_window vs previous short_window
    # Used by peak_fade so it catches active pullbacks even on tickers up big on the day
    short_price_start_dt = datetime.now(timezone.utc) - timedelta(minutes=short_window)
    prev_price_start_dt  = datetime.now(timezone.utc) - timedelta(minutes=2 * short_window)
    short_price_by_ticker = defaultdict(list)
    prev_price_by_ticker  = defaultdict(list)
    for doc in price_docs:
        if doc["ts"] >= short_price_start_dt:
            short_price_by_ticker[doc["ticker"]].append(doc["price"])
        elif doc["ts"] >= prev_price_start_dt:
            prev_price_by_ticker[doc["ticker"]].append(doc["price"])
    short_price_trend_map = {}
    for t in set(list(short_price_by_ticker.keys()) + list(prev_price_by_ticker.keys())):
        recent = short_price_by_ticker.get(t, [])
        prev   = prev_price_by_ticker.get(t, [])
        if recent and prev:
            recent_avg = sum(recent) / len(recent)
            prev_avg   = sum(prev)   / len(prev)
            if prev_avg > 0:
                short_price_trend_map[t] = round((recent_avg - prev_avg) / prev_avg * 100, 2)

    # ── Surge signal: always compare last 5m vs last 1h (raw counts, no scoring required)
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

    def _compute_signal(ticker, total_cnt, avg_composite):
        # Density direction: rate threshold + trend confirmation
        # short_cnt = messages in last short_window min
        # prev_cnt  = messages in the previous short_window min (one period back)
        # Requiring short_cnt >= prev_cnt for "up" prevents firing on a peak that
        # has already rolled over (e.g. density burst at open but falling by midday)
        short_cnt    = short_count_map.get(ticker, 0)
        prev_cnt     = prev_count_map.get(ticker, 0)
        rolling_rate = (total_cnt * short_window / rolling_window_minutes) if rolling_window_minutes > 0 else 0
        if total_cnt >= 5 and short_cnt >= max(3, rolling_rate * 1.5) and short_cnt >= prev_cnt:
            density_dir = "up"
        elif total_cnt >= 10 and rolling_rate > 0 and short_cnt <= rolling_rate * 0.5 and short_cnt <= prev_cnt:
            density_dir = "down"
        else:
            density_dir = "flat"

        # Sentiment direction: compare short-term avg score vs rolling avg
        short_sent   = short_sent_map.get(ticker, {})
        short_avg    = short_sent.get("avg")
        short_sent_n = short_sent.get("n", 0)
        if short_avg is not None and short_sent_n >= 3:
            if short_avg > avg_composite + 0.05:
                sentiment_dir = "up"
            elif short_avg < avg_composite - 0.05:
                sentiment_dir = "down"
            else:
                sentiment_dir = "flat"
        else:
            sentiment_dir = "flat"

        # Price direction: intraday % change over rolling window from MongoDB
        price_pct = price_trend_map.get(ticker)
        if price_pct is None:
            price_dir = "unknown"
        elif price_pct > 0.5:
            price_dir = "up"
        elif price_pct < -0.5:
            price_dir = "down"
        else:
            price_dir = "flat"

        # Short-window price direction: recent short_window vs previous short_window
        # Used only by peak_fade so it fires on active pullbacks even when full-window price is still up
        short_price_pct = short_price_trend_map.get(ticker)
        if short_price_pct is not None and short_price_pct < -0.3:
            short_price_dir = "down"
        elif short_price_pct is not None and short_price_pct > 0.3:
            short_price_dir = "up"
        else:
            short_price_dir = "flat"

        # Signal classification — density + sentiment must agree (with sentiment confirmation);
        # peak_fade uses short-window price direction to catch active pullbacks on big movers
        signal = None
        if density_dir == "up" and sentiment_dir == "up":
            signal = "bullish_corr" if price_dir == "up" else "early_long"
        elif density_dir == "down" and sentiment_dir == "down":
            signal = "bearish_corr" if price_dir == "down" else "early_short"
        elif total_cnt >= high_density_threshold and density_dir == "down" and short_price_dir == "down":
            signal = "peak_fade"

        return signal, density_dir, sentiment_dir, price_dir, price_pct

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

        signal, density_dir, sentiment_dir, price_dir, price_pct = \
            _compute_signal(ticker, total_map.get(ticker, len(msgs)), avg_composite)

        ticker_scores.append({
            "ticker":              ticker,
            "avg_composite_score": round(avg_composite, 4),
            "avg_sentiment_score": round(avg_sentiment, 4),
            "avg_trust_score":     round(avg_trust,     4),
            "avg_impact_score":    round(avg_impact,    4),
            "message_count":       len(msgs),
            "scored_messages":     len(msgs),
            "total_messages":      total_map.get(ticker, len(msgs)),
            "bullish_count":       bullish_count,
            "bearish_count":       bearish_count,
            "bull_pct":            bull_pct,
            "bear_pct":            bear_pct,
            "surge_ratio":         surge_ratio,
            "catalyst_events":     catalyst_map.get(ticker, []),
            "rolling_window_minutes": rolling_window_minutes,
            "signal":              signal,
            "density_dir":         density_dir,
            "sentiment_dir":       sentiment_dir,
            "price_dir":           price_dir,
            "price_trend_pct":     price_pct,
        })

    # Add tickers with messages but no scored messages yet
    for ticker in (set(total_map.keys()) - scored_tickers):
        count_5m    = surge_5m_map.get(ticker, 0)
        count_1h    = surge_1h_map.get(ticker, 0)
        expected_5m = count_1h / 12
        surge_ratio = round(count_5m / expected_5m, 1) if expected_5m > 0 and count_5m >= 3 else 0.0

        _, density_dir, _, price_dir, price_pct = _compute_signal(ticker, total_map.get(ticker, 0), 0.0)

        ticker_scores.append({
            "ticker":              ticker,
            "avg_composite_score": 0.0,
            "avg_sentiment_score": 0.0,
            "avg_trust_score":     0.0,
            "avg_impact_score":    0.0,
            "message_count":       0,
            "scored_messages":     0,
            "total_messages":      total_map.get(ticker, 0),
            "bullish_count":       0,
            "bearish_count":       0,
            "bull_pct":            0.0,
            "bear_pct":            0.0,
            "surge_ratio":         surge_ratio,
            "catalyst_events":     catalyst_map.get(ticker, []),
            "rolling_window_minutes": rolling_window_minutes,
            "signal":              None,
            "density_dir":         density_dir,
            "sentiment_dir":       "flat",
            "price_dir":           price_dir,
            "price_trend_pct":     price_pct,
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