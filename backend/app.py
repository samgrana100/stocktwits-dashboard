# app.py
# Flask web server for the Stocktwits News Sentiment Dashboard
# Serves the frontend and provides API endpoints for live data
# Runs pipeline and score calculator as background threads
# Samuel Grana - Stocktwits News Sentiment Dashboard

import sys
import os
import re
import threading
import time
import requests
from io import StringIO
from datetime import datetime, timezone, timedelta
import pandas as pd
from curl_cffi import requests as cffi_requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, request, render_template
from db import scored_messages, message_density, ensure_indexes
from score_calculator import aggregate_ticker_scores, score_unscored_messages
from utils import get_window_start_iso, get_timestamp
from event_detector import detect_event
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, template_folder="../frontend")

FINVIZ_API_TOKEN  = os.getenv("FINVIZ_API_TOKEN", "")
FINVIZ_EXPORT_URL = os.getenv("FINVIZ_EXPORT_URL", "")

# Pipeline control
pipeline_thread    = None
pipeline_stop_flag = [False]

# Scorer control
scorer_thread    = None
scorer_stop_flag = [False]

# Finviz screener data cache
finviz_cache      = {}
finviz_cache_time = 0
_finviz_lock      = threading.Lock()

# Per-ticker price cache: avoids hitting Finviz on every popup open
price_cache     = {}
PRICE_CACHE_TTL = 60  # seconds
_price_lock     = threading.Lock()

# Finviz Stocks News cache (v=3 + v=4) — keyed by ticker
finviz_news_cache      = {}
finviz_news_cache_time = 0
_finviz_news_lock      = threading.Lock()

WINDOW_CONFIG = {
    "1m":  {"minutes": 1,     "finviz_param": "i1"},
    "3m":  {"minutes": 3,     "finviz_param": "i1"},
    "5m":  {"minutes": 5,     "finviz_param": "i1"},
    "15m": {"minutes": 15,    "finviz_param": "i1"},
    "30m": {"minutes": 30,    "finviz_param": "i1"},
    "1h":  {"minutes": 60,    "finviz_param": "i1"},
    "4h":  {"minutes": 240,   "finviz_param": "i1"},
    "1d":  {"minutes": 1440,  "finviz_param": "i1"},
    "1w":  {"minutes": 10080, "finviz_param": "i1"},
    "1mo": {"minutes": 43200, "finviz_param": "i1"},
}


# ─── TIMEZONE HELPERS ───

def utc_to_et(utc_str: str) -> str:
    """Converts a UTC ISO string to Eastern Time string."""
    try:
        if utc_str.endswith("Z"):
            utc_str = utc_str[:-1] + "+00:00"
        dt_utc = datetime.fromisoformat(utc_str)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        month = dt_utc.month
        if 3 <= month <= 11:
            et_offset = timedelta(hours=-4)
            tz_label  = "EDT"
        else:
            et_offset = timedelta(hours=-5)
            tz_label  = "EST"
        dt_et = dt_utc + et_offset
        return dt_et.strftime("%Y-%m-%dT%H:%M:%S") + " " + tz_label
    except Exception:
        return utc_str


def get_et_now() -> datetime:
    """Returns current datetime in Eastern Time (timezone-aware)."""
    now_utc = datetime.now(timezone.utc)
    month   = now_utc.month
    if 3 <= month <= 11:
        et_offset = timedelta(hours=-4)
    else:
        et_offset = timedelta(hours=-5)
    return now_utc + et_offset


def get_et_timestamp() -> str:
    """Returns current Eastern Time timestamp as DD:MM:YYYY:HH:MM:SS string."""
    now_et = get_et_now()
    month  = datetime.now(timezone.utc).month
    label  = "EDT" if 3 <= month <= 11 else "EST"
    return now_et.strftime("%d:%m:%Y:%H:%M:%S") + " " + label


# ─── FINVIZ SCREENER CACHE ───

def get_finviz_data() -> dict:
    """Fetches Overview (v=111) + Technical (v=151) exports from Finviz, merges by ticker.
    Overview provides Market Cap / P/E; Technical provides Avg Volume / Rel Volume.
    Caches the merged result for 5 minutes."""
    global finviz_cache, finviz_cache_time

    with _finviz_lock:
        if finviz_cache and (time.time() - finviz_cache_time) < 300:
            return finviz_cache

    if not FINVIZ_EXPORT_URL or not FINVIZ_API_TOKEN:
        return {}

    def clean(val):
        s = str(val).strip()
        return "--" if s in ("nan", "None", "NaN", "", "inf") else s

    def fetch_df(v):
        url = re.sub(r'v=\d+', f'v={v}', FINVIZ_EXPORT_URL) + "&auth=" + FINVIZ_API_TOKEN
        try:
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                print(f"[FINVIZ] HTTP {r.status_code} for v={v}")
                return None
            df = pd.read_csv(StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:
            print(f"[FINVIZ] Failed to fetch v={v}: {e}")
            return None

    try:
        df_ov  = fetch_df(111)   # Overview: Market Cap, P/E, Price, Change, Volume
        df_tec = fetch_df(151)   # Technical: Avg Volume, Rel Volume

        result = {}

        if df_ov is not None:
            for _, row in df_ov.iterrows():
                ticker = str(row.get("Ticker", "")).strip().upper()
                if ticker:
                    result[ticker] = {
                        "company":    clean(row.get("Company",    "--")),
                        "sector":     clean(row.get("Sector",     "--")),
                        "industry":   clean(row.get("Industry",   "--")),
                        "country":    clean(row.get("Country",    "--")),
                        "market_cap": clean(row.get("Market Cap", "--")),
                        "pe":         clean(row.get("P/E",        "--")),
                        "price":      clean(row.get("Price",      "--")),
                        "change":     clean(row.get("Change",     "--")),
                        "volume":     clean(row.get("Volume",     "--")),
                        "avg_volume": "--",
                        "rel_volume": "--",
                    }

        if df_tec is not None:
            for _, row in df_tec.iterrows():
                ticker = str(row.get("Ticker", "")).strip().upper()
                if not ticker:
                    continue
                avg_vol = clean(row.get("Avg Volume") or row.get("Average Volume") or "--")
                rel_vol = clean(row.get("Rel Volume") or row.get("Relative Volume") or "--")
                if ticker in result:
                    result[ticker]["avg_volume"] = avg_vol
                    result[ticker]["rel_volume"]  = rel_vol
                else:
                    result[ticker] = {
                        "company":    clean(row.get("Company",  "--")),
                        "sector": "--", "industry": "--", "country": "--",
                        "market_cap": "--", "pe": "--",
                        "price":      clean(row.get("Price",   "--")),
                        "change":     clean(row.get("Change",  "--")),
                        "volume":     clean(row.get("Volume",  "--")),
                        "avg_volume": avg_vol,
                        "rel_volume": rel_vol,
                    }

        if df_tec is None and finviz_cache:
            for ticker in result:
                if ticker in finviz_cache:
                    result[ticker]["avg_volume"] = finviz_cache[ticker].get("avg_volume", "--")
                    result[ticker]["rel_volume"]  = finviz_cache[ticker].get("rel_volume", "--")

        with _finviz_lock:
            finviz_cache      = result
            finviz_cache_time = time.time()
        return result

    except Exception as e:
        print("[FINVIZ SCREENER] Failed: " + str(e))
        with _finviz_lock:
            return finviz_cache


# ─── FINVIZ PRICE FETCHER ───

def get_finviz_price_history(ticker: str, finviz_param: str, window_minutes: int) -> list:
    """
    Fetches price bars from Finviz quote_export (p=i1, 1-min bars).
    Results are cached per ticker for PRICE_CACHE_TTL seconds so repeated
    popup opens don't hammer the Finviz API.

    CSV format: "MM/DD/YYYY HH:MM AM/PM"  e.g. "05/21/2026 04:00 AM"
    """
    cache_key = f"{ticker}_{finviz_param}_{window_minutes}"
    now_ts    = time.time()
    with _price_lock:
        cached = price_cache.get(cache_key)
        if cached and (now_ts - cached["ts"]) < PRICE_CACHE_TTL:
            return cached["data"]

    url = (
        "https://elite.finviz.com/quote_export"
        f"?t={ticker}&p={finviz_param}&auth={FINVIZ_API_TOKEN}"
    )

    try:
        response = cffi_requests.get(url, impersonate="chrome", timeout=15)

        if response.status_code != 200:
            print(f"[FINVIZ PRICE] HTTP {response.status_code} for {ticker}")
            return []

        df = pd.read_csv(StringIO(response.text))
        df.columns = [c.strip() for c in df.columns]

        if "Date" not in df.columns or "Close" not in df.columns:
            print(f"[FINVIZ PRICE] Bad columns for {ticker}: {list(df.columns)}")
            return []

        df["Date"]  = df["Date"].astype(str).str.strip()
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df          = df.dropna(subset=["Close"])
        df          = df[df["Date"] != ""]

        def parse_finviz_date(date_str):
            s = str(date_str).strip()
            # Finviz hybrid format: hours 0-12 use "HH:MM AM/PM" (12-hour),
            # hours 13-23 use "HH:MM PM" (24-hour number but still appends PM).
            # %I:%M %p rejects hour>=13, so we strip the suffix and use %H:%M instead.
            upper = s.upper()
            if upper.endswith(" AM") or upper.endswith(" PM"):
                parts = s.rsplit(" ", 2)
                if len(parts) == 3:
                    date_part, time_part, _ = parts
                    try:
                        hour = int(time_part.split(":")[0])
                    except ValueError:
                        hour = 0
                    if hour >= 13:
                        try:
                            return datetime.strptime(f"{date_part} {time_part}", "%m/%d/%Y %H:%M")
                        except ValueError:
                            pass
                    else:
                        try:
                            return datetime.strptime(s, "%m/%d/%Y %I:%M %p")
                        except ValueError:
                            pass
            try:
                return datetime.strptime(s, "%m/%d/%Y")
            except ValueError:
                return None

        df["parsed_dt"] = df["Date"].apply(parse_finviz_date)
        df = df.dropna(subset=["parsed_dt"])

        if df.empty:
            print(f"[FINVIZ PRICE] No parseable dates for {ticker}")
            return []

        # Anchor the rolling window to the data's own end timestamp (not now()),
        # so the window selector works correctly even when the market is closed.
        # For r=d1 this covers the full current session; for r=d5/m1 it spans
        # multiple days and the window_minutes cutoff selects the right slice.
        df          = df.sort_values("parsed_dt")
        session_end = df["parsed_dt"].max()
        cutoff      = session_end - timedelta(minutes=window_minutes)
        df          = df[df["parsed_dt"] >= cutoff]

        month    = datetime.now(timezone.utc).month
        tz_label = "EDT" if 3 <= month <= 11 else "EST"

        price_hist = []
        for _, row in df.iterrows():
            try:
                dt          = row["parsed_dt"]
                close_price = round(float(row["Close"]), 2)
                price_hist.append({
                    "time":     dt.strftime("%Y-%m-%dT%H:%M:%S") + " " + tz_label,
                    "price":    close_price,
                    "sort_key": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                })
            except Exception:
                continue

        print(f"[FINVIZ PRICE] {ticker}: {len(price_hist)} points, session_end={session_end} (param={finviz_param})")
        with _price_lock:
            stale = [k for k, v in price_cache.items() if (now_ts - v["ts"]) >= PRICE_CACHE_TTL]
            for k in stale:
                del price_cache[k]
            price_cache[cache_key] = {"data": price_hist, "ts": now_ts}
        return price_hist

    except Exception as e:
        print(f"[FINVIZ PRICE] Failed for {ticker}: {e}")
        return []


# ─── FINVIZ STOCKS NEWS CACHE ───

def get_finviz_stocks_news() -> dict:
    """Fetches Stocks News from Finviz (v=3 + v=4), returns {ticker: [articles]}.
    Runs detect_event on each headline so confirmed_event_types can be computed.
    Caches for 10 minutes."""
    global finviz_news_cache, finviz_news_cache_time

    with _finviz_news_lock:
        if finviz_news_cache and (time.time() - finviz_news_cache_time) < 600:
            return finviz_news_cache

    if not FINVIZ_API_TOKEN:
        return {}

    result = {}
    for v in (3, 4):
        url = f"https://elite.finviz.com/news_export?v={v}&auth={FINVIZ_API_TOKEN}"
        try:
            r = cffi_requests.get(url, impersonate="chrome120", timeout=15)
            if r.status_code != 200:
                print(f"[FINVIZ NEWS] HTTP {r.status_code} for v={v}")
                continue
            df = pd.read_csv(StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]
            if "Ticker" not in df.columns:
                continue
            for _, row in df.iterrows():
                tickers_raw = str(row.get("Ticker", "") or "")
                for t in tickers_raw.split(","):
                    t = t.strip().upper()
                    if not t:
                        continue
                    article = {
                        "title":      str(row.get("Title",  "")).strip(),
                        "source":     str(row.get("Source", "")).strip(),
                        "date":       str(row.get("Date",   "")).strip(),
                        "url":        str(row.get("Url",    "")).strip(),
                        "event_type": detect_event(str(row.get("Title", ""))),
                    }
                    result.setdefault(t, []).append(article)
        except Exception as e:
            print(f"[FINVIZ NEWS] Failed v={v}: {e}")

    # Dedup per ticker — same headline may appear in both v=3 and v=4
    for t in result:
        seen, deduped = set(), []
        for a in result[t]:
            if a["title"] not in seen:
                seen.add(a["title"])
                deduped.append(a)
        result[t] = deduped

    with _finviz_news_lock:
        finviz_news_cache      = result
        finviz_news_cache_time = time.time()
    return result


# ─── ROUTES ───

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/scores")
def get_scores():
    window_key = request.args.get("window", "1h")
    config     = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"])
    window_min = config["minutes"]
    scores     = aggregate_ticker_scores(rolling_window_minutes=window_min)
    finviz     = get_finviz_data()

    # Only show tickers that are in the current Finviz screener
    scores = [s for s in scores if s["ticker"] in finviz]

    scored_tickers = set()
    for ticker_data in scores:
        t  = ticker_data["ticker"]
        scored_tickers.add(t)
        fv = finviz.get(t, {})
        ticker_data["company"]    = fv.get("company",    "--")
        ticker_data["sector"]     = fv.get("sector",     "--")
        ticker_data["industry"]   = fv.get("industry",   "--")
        ticker_data["country"]    = fv.get("country",    "--")
        ticker_data["market_cap"] = fv.get("market_cap", "--")
        ticker_data["pe"]         = fv.get("pe",         "--")
        ticker_data["price"]      = fv.get("price",      "--")
        ticker_data["change"]     = fv.get("change",     "--")
        ticker_data["volume"]     = fv.get("volume",     "--")
        ticker_data["avg_volume"] = fv.get("avg_volume", "--")
        ticker_data["rel_volume"] = fv.get("rel_volume", "--")

    # Finviz tickers with no messages at all in this window
    no_message_tickers = sorted(
        [
            {
                "ticker":   t,
                "company":  fv.get("company",  "--"),
                "sector":   fv.get("sector",   "--"),
                "industry": fv.get("industry", "--"),
                "price":    fv.get("price",    "--"),
                "change":   fv.get("change",   "--"),
            }
            for t, fv in finviz.items()
            if t not in scored_tickers
        ],
        key=lambda x: x["ticker"]
    )

    return jsonify({
        "status":             "ok",
        "window_key":         window_key,
        "window_minutes":     window_min,
        "ticker_count":       len(scores),
        "timestamp":          get_et_timestamp(),
        "tickers":            scores,
        "no_message_tickers": no_message_tickers
    })


@app.route("/api/ticker/<ticker>")
def get_ticker_detail(ticker):
    window_key    = request.args.get("window", "1h")
    rolling_key   = request.args.get("rolling", window_key)   # rolling window for score/density stats
    config        = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"])
    window_min    = config["minutes"]
    finviz_param  = config["finviz_param"]
    window_start  = get_window_start_iso(window_min)
    ticker        = ticker.upper()

    rolling_config = WINDOW_CONFIG.get(rolling_key, config)
    rolling_min    = rolling_config["minutes"]
    rolling_start  = get_window_start_iso(rolling_min)

    now_utc         = datetime.now(timezone.utc)
    window_start_dt = now_utc - timedelta(minutes=window_min)

    # ─── Rolling score history ───
    score_docs = list(scored_messages.find(
        {
            "ticker":          ticker,
            "created_at_utc":  {"$gte": window_start},
            "composite_score": {"$ne": None}
        },
        {"composite_score": 1, "sentiment_score": 1, "trust_score": 1, "impact_score": 1,
         "created_at_utc": 1, "_id": 0}
    ).sort("created_at_utc", 1).limit(5000))

    score_history = [
        {
            "time":     utc_to_et(d["created_at_utc"]),
            "score":    d["composite_score"],
            "sort_key": d["created_at_utc"]
        }
        for d in score_docs
    ]

    # ─── Real message density from scored_messages ───
    pipeline_query = [
        {
            "$match": {
                "ticker":         ticker,
                "created_at_utc": {"$gte": window_start}
            }
        },
        {
            "$group": {
                "_id":   {"$substr": ["$created_at_utc", 0, 16]},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    density_docs    = list(scored_messages.aggregate(pipeline_query))
    density_history = [
        {
            "time":     utc_to_et(d["_id"] + ":00Z"),
            "count":    d["count"],
            "sort_key": d["_id"]
        }
        for d in density_docs
    ]

    # ─── Price history from Finviz quote_export API ───
    # Returns all minute bars from the most recent session.
    # Frontend matches by HH:MM so past-session dates align with today's axis.
    price_hist = get_finviz_price_history(ticker, finviz_param, window_min)

    # ─── Build time axis ───
    # For intraday windows (p=i1): use the price data's actual time range so the
    # price line is always visible. Score/density match to this axis by HH:MM.
    # For daily/weekly/monthly windows: fall back to the rolling window range.
    time_axis = []
    month    = datetime.now(timezone.utc).month
    tz_label = "EDT" if 3 <= month <= 11 else "EST"

    if price_hist and finviz_param == "i1":
        sort_keys   = [p["sort_key"] for p in price_hist]
        axis_start  = datetime.strptime(min(sort_keys), "%Y-%m-%dT%H:%M:%S")
        axis_end    = datetime.strptime(max(sort_keys), "%Y-%m-%dT%H:%M:%S")
        current     = axis_start
        while current <= axis_end:
            time_axis.append(current.strftime("%Y-%m-%dT%H:%M:%S") + " " + tz_label)
            current += timedelta(minutes=1)
    else:
        step = 1 if window_min <= 60 else (5 if window_min <= 1440 else (60 if window_min <= 10080 else 1440))
        current = window_start_dt.replace(tzinfo=timezone.utc)
        while current <= now_utc:
            time_axis.append(utc_to_et(current.strftime("%Y-%m-%dT%H:%M:%SZ")))
            current += timedelta(minutes=step)
        time_axis.append(utc_to_et(now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")))

    # ─── Rolling window aggregate scores (independent of chart window) ───
    # Uses rolling_start so the header cards reflect the per-ticker rolling window,
    # while the chart still spans the full chart window (window_start).
    rolling_score_docs = list(scored_messages.find(
        {"ticker": ticker, "created_at_utc": {"$gte": rolling_start}, "composite_score": {"$ne": None}},
        {"composite_score": 1, "sentiment_score": 1, "trust_score": 1, "impact_score": 1, "_id": 0}
    ).limit(5000))
    rolling_message_total = scored_messages.count_documents(
        {"ticker": ticker, "created_at_utc": {"$gte": rolling_start}}
    )

    latest_scores = {}
    if rolling_score_docs:
        n = len(rolling_score_docs)
        latest_scores = {
            "composite_score": round(sum(d["composite_score"]            for d in rolling_score_docs) / n, 4),
            "sentiment_score": round(sum((d.get("sentiment_score") or 0) for d in rolling_score_docs) / n, 4),
            "trust_score":     round(sum((d.get("trust_score")     or 0) for d in rolling_score_docs) / n, 4),
            "impact_score":    round(sum((d.get("impact_score")    or 0) for d in rolling_score_docs) / n, 4),
            "scored_count":    n
        }

    finviz = get_finviz_data()
    fv     = finviz.get(ticker, {})

    # Finviz Stocks News — articles tagged with this ticker
    news_data              = get_finviz_stocks_news()
    confirmed_articles     = news_data.get(ticker, [])[:10]
    confirmed_event_types  = list({a["event_type"] for a in confirmed_articles if a["event_type"]})

    # Event markers — one entry per tagged message in the chart window
    event_docs = list(scored_messages.find(
        {"ticker": ticker, "created_at_utc": {"$gte": window_start}, "event_type": {"$ne": None}},
        {"event_type": 1, "created_at_utc": 1, "body": 1, "user": 1, "_id": 0}
    ).sort("created_at_utc", 1).limit(500))
    event_history = [
        {
            "time":       utc_to_et(d["created_at_utc"]),
            "event_type": d["event_type"],
            "body":       (d.get("body") or "").strip(),
            "username":   (d.get("user") or {}).get("username", "") if isinstance(d.get("user"), dict) else "",
        }
        for d in event_docs
    ]

    return jsonify({
        "status":                 "ok",
        "ticker":                 ticker,
        "window_key":             window_key,
        "window_minutes":         window_min,
        "rolling_key":            rolling_key,
        "rolling_message_total":  rolling_message_total,
        "company":                fv.get("company",  "--"),
        "sector":                 fv.get("sector",   "--"),
        "industry":               fv.get("industry", "--"),
        "latest_scores":          latest_scores,
        "score_history":          score_history,
        "density_history":        density_history,
        "price_history":          price_hist,
        "time_axis":              time_axis,
        "event_history":          event_history,
        "confirmed_news":         confirmed_articles,
        "confirmed_event_types":  confirmed_event_types,
    })


@app.route("/api/quickscore/<ticker>")
def get_quickscore(ticker):
    """Lightweight score summary for one ticker at a given window. No price fetch, no chart build."""
    window_key   = request.args.get("window", "5m")
    config       = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["5m"])
    window_min   = config["minutes"]
    window_start = get_window_start_iso(window_min)
    ticker       = ticker.upper()

    score_docs = list(scored_messages.find(
        {"ticker": ticker, "created_at_utc": {"$gte": window_start}, "composite_score": {"$ne": None}},
        {"composite_score": 1, "_id": 0}
    ))
    total = scored_messages.count_documents(
        {"ticker": ticker, "created_at_utc": {"$gte": window_start}}
    )
    n     = len(score_docs)
    score = round(sum(d["composite_score"] for d in score_docs) / n, 4) if n else 0.0

    return jsonify({
        "ticker":              ticker,
        "window_key":          window_key,
        "avg_composite_score": score,
        "scored_messages":     n,
        "total_messages":      total,
    })


@app.route("/api/bullish-feed")
def get_bullish_feed():
    """Returns recent significantly bullish messages with posted time and scoring delay."""
    threshold  = request.args.get("threshold", 0.5, type=float)
    window_min = request.args.get("window", 240, type=int)
    limit      = request.args.get("limit", 60, type=int)

    window_start = get_window_start_iso(window_min)

    docs = list(scored_messages.find(
        {
            "created_at_utc":  {"$gte": window_start},
            "composite_score": {"$gte": threshold},
            "body":            {"$exists": True, "$ne": ""}
        },
        {"ticker": 1, "body": 1, "composite_score": 1, "sentiment_score": 1,
         "trust_score": 1, "impact_score": 1, "created_at_utc": 1, "scored_at": 1,
         "user": 1, "event_type": 1, "_id": 0}
    ).sort("created_at_utc", -1).limit(limit))

    result = []
    for d in docs:
        created = d.get("created_at_utc", "")
        scored  = d.get("scored_at", "")
        delay_sec = None
        try:
            ct = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ")
            st = datetime.strptime(scored,  "%d:%m:%Y:%H:%M:%S")
            delay_sec = max(0, int((st - ct).total_seconds()))
        except Exception:
            pass

        user     = d.get("user") or {}
        username = user.get("username", "") if isinstance(user, dict) else ""
        body     = (d.get("body") or "").strip()

        # Use stored event_type if present; fall back to inline detection for older docs
        event_type = d.get("event_type") or detect_event(body)

        result.append({
            "ticker":          d.get("ticker", ""),
            "body":            body,
            "composite_score": round(d.get("composite_score") or 0, 4),
            "sentiment_score": round(d.get("sentiment_score") or 0, 4),
            "trust_score":     round(d.get("trust_score")     or 0, 4),
            "impact_score":    round(d.get("impact_score")    or 0, 4),
            "posted_et":       utc_to_et(created) if created else "",
            "delay_seconds":   delay_sec,
            "username":        username,
            "event_type":      event_type,
        })

    return jsonify({"messages": result, "threshold": threshold})


@app.route("/api/ticker-messages/<ticker>")
def get_ticker_messages(ticker):
    """Returns recent scored messages for a specific ticker (for the news view)."""
    ticker     = ticker.upper()
    limit      = request.args.get("limit", 15, type=int)
    window_min = request.args.get("window", 1440, type=int)
    window_start = get_window_start_iso(window_min)

    docs = list(scored_messages.find(
        {
            "ticker":          ticker,
            "created_at_utc":  {"$gte": window_start},
            "body":            {"$exists": True, "$ne": ""},
            "composite_score": {"$ne": None},
        },
        {"body": 1, "user": 1, "created_at_utc": 1, "composite_score": 1, "event_type": 1, "_id": 0}
    ).sort("created_at_utc", -1).limit(limit))

    result = []
    for d in docs:
        user     = d.get("user") or {}
        username = user.get("username", "") if isinstance(user, dict) else ""
        body     = (d.get("body") or "").strip()
        score    = d.get("composite_score")
        event_type = d.get("event_type") or detect_event(body)
        result.append({
            "body":            body,
            "username":        username,
            "posted_et":       utc_to_et(d.get("created_at_utc", "")),
            "composite_score": round(score, 4) if score is not None else None,
            "event_type":      event_type,
        })

    return jsonify({"ticker": ticker, "messages": result})


@app.route("/api/backfill-events", methods=["POST"])
def backfill_events():
    """
    Retroactively runs detect_event on all messages where event_type is null/missing.
    Uses bulk_write in batches of 500 for speed.
    """
    from pymongo import UpdateOne

    query = {
        "body": {"$exists": True, "$ne": ""},
        "$or": [
            {"event_type": {"$exists": False}},
            {"event_type": None},
        ],
    }
    total  = scored_messages.count_documents(query)
    found  = 0
    ops    = []

    cursor = scored_messages.find(query, {"body": 1, "_id": 1}).batch_size(500)
    for doc in cursor:
        body       = (doc.get("body") or "").strip()
        event_type = detect_event(body)
        if event_type:
            found += 1
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"event_type": event_type}}))
        if len(ops) >= 500:
            scored_messages.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        scored_messages.bulk_write(ops, ordered=False)

    # Return a summary of all event tags now in the DB
    agg = list(scored_messages.aggregate([
        {"$match": {"event_type": {"$ne": None}}},
        {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]))

    print(f"[BACKFILL] Processed {total:,} messages, found {found:,} events")
    return jsonify({
        "processed": total,
        "events_found": found,
        "summary": [{"event": r["_id"], "count": r["count"]} for r in agg],
    })


@app.route("/api/reset-scores", methods=["POST"])
def reset_scores():
    """Resets all composite scores to None so the scorer re-runs every message."""
    result = scored_messages.update_many(
        {},
        {"$set": {
            "composite_score": None,
            "sentiment_score": None,
            "trust_score":     None,
            "impact_score":    None
        }}
    )
    print(f"[RESET] Cleared scores for {result.modified_count} messages.")
    return jsonify({"status": "ok", "reset_count": result.modified_count})


@app.route("/api/pipeline/start", methods=["POST"])
def start_pipeline():
    global pipeline_thread, pipeline_stop_flag
    if pipeline_thread and pipeline_thread.is_alive():
        return jsonify({"status": "already_running"})

    pipeline_stop_flag = [False]

    def run_pipeline_thread():
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from pipeline import run_pipeline
        run_pipeline(rolling_window=60, stop_flag=pipeline_stop_flag)

    pipeline_thread = threading.Thread(target=run_pipeline_thread, daemon=True)
    pipeline_thread.start()
    return jsonify({"status": "started"})


@app.route("/api/pipeline/stop", methods=["POST"])
def stop_pipeline():
    global pipeline_stop_flag
    pipeline_stop_flag[0] = True
    return jsonify({"status": "stopped"})


@app.route("/api/pipeline/status")
def pipeline_status():
    running = pipeline_thread is not None and pipeline_thread.is_alive()
    return jsonify({"running": running})


def run_scorer_loop(stop_flag: list):
    """Runs the score calculator continuously in a background thread."""
    print("[SCORER THREAD] Starting continuous scoring loop...")
    while not stop_flag[0]:
        try:
            score_unscored_messages(batch_size=50)
        except Exception as e:
            print("[SCORER THREAD] Error: " + str(e))
        time.sleep(10)
    print("[SCORER THREAD] Stopped.")


if __name__ == "__main__":
    ensure_indexes()

    scorer_stop_flag = [False]
    scorer_thread    = threading.Thread(
        target=run_scorer_loop,
        args=(scorer_stop_flag,),
        daemon=True
    )
    scorer_thread.start()
    print("[APP] Score calculator started automatically.")
    print("[APP] Starting Stocktwits Sentiment Dashboard...")
    print("[APP] Open your browser at http://localhost:5000")

    app.run(debug=False, port=5000)