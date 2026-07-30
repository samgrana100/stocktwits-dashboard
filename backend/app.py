# app.py
# Flask web server for the Stocktwits News Sentiment Dashboard
# Serves the frontend and provides API endpoints for live data
# Runs pipeline and score calculator as background threads
# Samuel Grana - Stocktwits News Sentiment Dashboard

import sys
import os
import re
import json
import threading
import time
import random
import requests
import xml.etree.ElementTree as ET
from collections import defaultdict
from io import StringIO
from pathlib import Path
from datetime import datetime, date, timezone, timedelta
from urllib.parse import quote as url_quote
import pandas as pd
from curl_cffi import requests as cffi_requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, request, render_template
from db import scored_messages, ensure_indexes, upcoming_catalysts, auto_trades, completed_auto_trades, price_history, screener_snapshots
from score_calculator import aggregate_ticker_scores, score_unscored_messages
from utils import get_window_start_iso, get_timestamp
from event_detector import detect_event
from calendar_fetcher import start_calendar_threads
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, template_folder="../frontend")

# ── CREDENTIALS & API GLOBALS ──
# All secrets are read from .env at startup; refresh_finviz_token hot-swaps
# FINVIZ_API_TOKEN in memory when the session cookie expires.
FINVIZ_API_TOKEN  = os.getenv("FINVIZ_API_TOKEN", "")
FINVIZ_EXPORT_URL = os.getenv("FINVIZ_EXPORT_URL", "")
_FINVIZ_EMAIL    = os.getenv("FINVIZ_EMAIL", "")
_FINVIZ_PASSWORD = os.getenv("FINVIZ_PASSWORD", "")
FINNHUB_API_KEY  = os.getenv("FINNHUB_API_KEY", "")

# Double-checked locking guard so concurrent requests never trigger two simultaneous logins
_token_refresh_lock  = threading.Lock()
_last_token_refresh  = 0.0
_TOKEN_REFRESH_COOLDOWN = 300  # seconds between re-login attempts

# Bubble screener data is expensive to build (two Finviz CSV fetches + MongoDB aggregation)
_bubble_screener_cache = {}
_bubble_screener_lock  = threading.Lock()
BUBBLE_SCREENER_TTL    = 120  # 2-minute cache to avoid rate limiting

# ── AUTO TRADE THRESHOLDS ──
# Entry fires when density-price Pearson r ≥ AUTO_ENTRY_CORR and msgs/hr ≥ AUTO_ENTRY_MIN_DENS.
# Exit fires when density falls AUTO_EXIT_PEAK % below its intra-trade peak.
AUTO_ENTRY_CORR      = 0.30  # minimum correlation to enter
AUTO_ENTRY_MIN_DENS  = 4    # minimum msgs/hr (1h rolling) to enter
AUTO_EXIT_PEAK       = 0.20  # density % off peak to exit
AUTO_EXIT_MIN_DROP   = 6    # minimum absolute message drop before exit fires
AUTO_LOOP_SEC      = 60     # seconds between auto trade checks
REENTRY_COOLDOWN   = 3600   # seconds before re-entering the same ticker


def refresh_finviz_token() -> bool:
    """
    Logs into Finviz with stored credentials, scrapes elite.finviz.com/api_explanation
    to get the current auth token, then hot-swaps it in memory and rewrites .env.
    Returns True if a token was found (changed or unchanged).
    """
    global FINVIZ_API_TOKEN, _last_token_refresh

    if not _FINVIZ_EMAIL or not _FINVIZ_PASSWORD:
        print("[TOKEN] FINVIZ_EMAIL / FINVIZ_PASSWORD not set — skipping refresh")
        return False

    if time.time() - _last_token_refresh < _TOKEN_REFRESH_COOLDOWN:
        return False

    with _token_refresh_lock:
        if time.time() - _last_token_refresh < _TOKEN_REFRESH_COOLDOWN:
            return False
        _last_token_refresh = time.time()
        try:
            session = cffi_requests.Session()
            login_resp = session.post(
                "https://finviz.com/login_submit.ashx",
                data={"email": _FINVIZ_EMAIL, "password": _FINVIZ_PASSWORD, "remember": "1"},
                impersonate="chrome120",
                timeout=15,
            )
            if login_resp.status_code not in (200, 302):
                print(f"[TOKEN] Login failed — HTTP {login_resp.status_code}")
                return False

            # Token is shown on the API explanation page inside &auth=<uuid>
            api_page = session.get(
                "https://elite.finviz.com/api_explanation",
                impersonate="chrome120",
                timeout=15,
            )
            # Token is embedded as userToken in the page's JSON data
            token_match = re.search(
                r'"userToken"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"',
                api_page.text,
                re.IGNORECASE,
            )
            if not token_match:
                print("[TOKEN] Could not find token on api_explanation page")
                return False

            new_token = token_match.group(1).lower()
            if new_token == FINVIZ_API_TOKEN:
                print(f"[TOKEN] Token is current ({new_token[:8]}...)")
                return True

            FINVIZ_API_TOKEN = new_token
            os.environ["FINVIZ_API_TOKEN"] = new_token
            print(f"[TOKEN] Refreshed token -> {new_token[:8]}...")

            # Skip .env rewrite on Railway — filesystem is ephemeral there.
            # In-memory hot-swap above is sufficient for the current process.
            if not os.getenv("RAILWAY_ENVIRONMENT"):
                env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
                try:
                    with open(env_path, "r", encoding="utf-8") as f:
                        env_lines = f.readlines()
                    with open(env_path, "w", encoding="utf-8") as f:
                        for line in env_lines:
                            if line.startswith("FINVIZ_API_TOKEN="):
                                f.write(f"FINVIZ_API_TOKEN={new_token}\n")
                            else:
                                f.write(line)
                    print(f"[TOKEN] Updated .env with new token.")
                except Exception as e:
                    print(f"[TOKEN] Could not write .env: {e}")

            return True

        except Exception as e:
            print(f"[TOKEN] Refresh failed: {e}")
            return False

# ── PIPELINE STATE ──
# stop_flag is a one-element list so threads can observe the signal without
# needing to share a threading.Event across module reloads.
pipeline_thread    = None
pipeline_stop_flag = [False]

# ── RUNTIME CACHES ──
# Finviz screener data cache — shared across all /api/scores and /api/bubble-screener calls
finviz_cache      = {}
finviz_cache_time = 0
_finviz_lock      = threading.Lock()

# Per-ticker price cache: avoids hitting Finviz on every popup open
price_cache      = {}
PRICE_CACHE_TTL  = 60  # seconds base TTL; each entry gets +0-30s jitter to stagger expiry
_price_lock      = threading.Lock()
_finviz_price_sem = threading.Semaphore(3)  # max 3 concurrent Finviz price requests

# Per-ticker detail cache: shared by /api/ticker and /api/tickers/batch
# Short TTL keeps the dashboard real-time while avoiding redundant aggregations
ticker_detail_cache = {}
TICKER_DETAIL_TTL   = 30  # seconds
_ticker_detail_lock = threading.Lock()

# Finviz Stocks News cache (v=3 + v=4) — keyed by ticker
finviz_news_cache      = {}
finviz_news_cache_time = 0
_finviz_news_lock      = threading.Lock()

# Per-ticker news cache — independent of pipeline
_ticker_news_cache     = {}   # {ticker: {"articles": [...], "ts": float}}
_TICKER_NEWS_TTL       = 1800  # 30 minutes


# ── WINDOW CONFIG ──
# Maps frontend window keys to their minute durations and Finviz chart parameters.
# finviz_param drives the quote_export endpoint used for price bar fetching.
WINDOW_CONFIG = {
    "1m":  {"minutes": 1,     "finviz_param": "i1"},
    "3m":  {"minutes": 3,     "finviz_param": "i1"},
    "5m":  {"minutes": 5,     "finviz_param": "i1"},
    "15m": {"minutes": 15,    "finviz_param": "i1"},
    "30m": {"minutes": 30,    "finviz_param": "i1"},
    "1h":  {"minutes": 60,    "finviz_param": "i1"},
    "2h":  {"minutes": 120,   "finviz_param": "i1"},
    "4h":  {"minutes": 240,   "finviz_param": "i1"},
    "1d":  {"minutes": 1440,  "finviz_param": "i1"},
    "1w":  {"minutes": 10080, "finviz_param": "i1"},
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


def _utc_dt_to_et_hhmm(dt: datetime) -> str:
    """Convert a UTC datetime object to Eastern Time HH:MM string."""
    offset = timedelta(hours=-4) if 3 <= dt.month <= 11 else timedelta(hours=-5)
    return (dt + offset).strftime("%H:%M")


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

    refreshed = [False]  # shared flag — only refresh once across both v=111 and v=151

    def fetch_df(v):
        url = re.sub(r'v=\d+', f'v={v}', FINVIZ_EXPORT_URL) + "&auth=" + FINVIZ_API_TOKEN
        try:
            r = cffi_requests.get(url, impersonate="chrome120", timeout=15)
            if r.status_code == 401 and not refreshed[0]:
                print(f"[FINVIZ] 401 on v={v} — refreshing token")
                refreshed[0] = True
                if refresh_finviz_token():
                    url = re.sub(r'v=\d+', f'v={v}', FINVIZ_EXPORT_URL) + "&auth=" + FINVIZ_API_TOKEN
                    r   = cffi_requests.get(url, impersonate="chrome120", timeout=15)
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

        # Snapshot screener data for 3D bubble trail (piggybacked — no new Finviz call)
        # Only during regular market hours: rel_vol is meaningless pre/post market
        try:
            now_utc  = datetime.now(timezone.utc)
            et_off   = timedelta(hours=-4) if 3 <= now_utc.month <= 11 else timedelta(hours=-5)
            now_et   = now_utc + et_off
            et_min   = now_et.hour * 60 + now_et.minute
            in_hours = (4 * 60) <= et_min <= (20 * 60)
            if in_hours:
                def _pf(v):
                    try:
                        return float(str(v).replace("%","").replace(",","").strip())
                    except Exception:
                        return None
                snaps = []
                for tk, fv in result.items():
                    price_chg = _pf(fv.get("change"))
                    rel_vol   = _pf(fv.get("rel_volume"))
                    volume    = _pf(fv.get("volume"))
                    if price_chg is None or rel_vol is None or volume is None:
                        continue
                    snaps.append({
                        "ticker":    tk,
                        "ts":        now_utc,
                        "price_chg": price_chg,
                        "rel_vol":   max(0.0, rel_vol),
                        "volume":    volume,
                    })
                if snaps:
                    screener_snapshots.insert_many(snaps, ordered=False)
        except Exception:
            pass

        return result

    except Exception as e:
        print("[FINVIZ SCREENER] Failed: " + str(e))
        with _finviz_lock:
            return finviz_cache


# ─── FINVIZ PRICE FETCHER ───

def get_finviz_price_history(ticker: str, finviz_param: str, multi_day: bool = False) -> list:
    """
    Fetches price bars from Finviz quote_export (p=i1, 1-min bars).

    multi_day=False (sub-day charts): restricts to the most recent calendar day so
        cross-day HH:MM key collisions can't occur when needsDate=False in the frontend.
    multi_day=True  (1d+ charts):     returns all bars from the CSV; needsDate=True
        in the frontend keys every bar by "MM-DD HH:MM", so multi-day data is safe.

    CSV format: "MM/DD/YYYY HH:MM AM/PM"  e.g. "05/21/2026 04:00 AM"
    """
    cache_key = f"{ticker}_{finviz_param}_{'multi' if multi_day else 'today'}"
    now_ts    = time.time()
    with _price_lock:
        cached = price_cache.get(cache_key)
        if cached and (now_ts - cached["ts"]) < PRICE_CACHE_TTL + cached.get("jitter", 0):
            return cached["data"]

    url = (
        "https://elite.finviz.com/quote_export"
        f"?t={ticker}&p={finviz_param}&auth={FINVIZ_API_TOKEN}"
    )

    with _finviz_price_sem:
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
                # Some exports abbreviate to single char "P"/"A" — normalize first.
                # %I:%M %p rejects hour>=13, so we strip the suffix and use %H:%M instead.
                if s.endswith(" P"): s = s[:-2] + " PM"
                if s.endswith(" A"): s = s[:-2] + " AM"
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

            df          = df.sort_values("parsed_dt")
            session_end = df["parsed_dt"].max()
            if not multi_day:
                # Sub-day charts key price by HH:MM only (needsDate=False in frontend).
                # Restrict to the most recent session date so yesterday's bars never
                # collide with today's at the same minute key.
                df = df[df["parsed_dt"].dt.date == session_end.date()]

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

            print(f"[FINVIZ PRICE] {ticker}: {len(price_hist)} points (param={finviz_param})")
            with _price_lock:
                stale = [k for k, v in price_cache.items() if (now_ts - v["ts"]) >= PRICE_CACHE_TTL + v.get("jitter", 0)]
                for k in stale:
                    del price_cache[k]
                price_cache[cache_key] = {"data": price_hist, "ts": now_ts, "jitter": random.uniform(0, 30)}
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


def _parse_news_date(date_str: str):
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %I:%M%p", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # RFC 822 (RSS pubDate: "Mon, 30 Jun 2026 09:00:00 +0000")
    try:
        import email.utils
        return email.utils.parsedate_to_datetime(date_str).replace(tzinfo=None)
    except Exception:
        pass
    return None


# ── NEWS FETCH HELPERS ──

# Strips law-firm / investor-alert noise from wire service results
_WIRE_NOISE_RE = re.compile(
    r'DEADLINE ALERT|SHAREHOLDER ALERT|CLASS ACTION|Law Offices|'
    r'Announces Investigation|ALERT:|NOTICE:|INVESTIGATION NOTICE|'
    r'Reminds Investors|Pomerantz|Edelson|Kahn Swick|Bragar Eagel|Frank R\. Cruz',
    re.IGNORECASE,
)

# Exchange prefix patterns for per-ticker wire search
_WIRE_EXCHANGE_PATTERNS = [
    "NYSE", "NASDAQ", "AMEX", "OTC", "OTCQB", "OTCQX",
]


def _get_globenewswire_news(ticker: str) -> list:
    """Fetches GlobeNewswire press releases for a ticker via keyword RSS."""
    try:
        url = f"https://www.globenewswire.com/RssFeed/keyword/{ticker}"
        r   = requests.get(
            url,
            headers={"User-Agent": "stocktwits-dashboard samgrana100@gmail.com"},
            timeout=10,
        )
        if r.status_code != 200:
            print(f"[GNW] HTTP {r.status_code} for {ticker}")
            return []
        root  = ET.fromstring(r.content)
        items = root.findall(".//item") or root.findall(".//entry")
        articles = []
        for item in items:
            title = (item.findtext("title") or "").strip()
            if _WIRE_NOISE_RE.search(title):
                continue
            link    = (item.findtext("link") or "").strip()
            pub_raw = (item.findtext("pubDate") or item.findtext("updated") or "").strip()
            pub_dt  = _parse_news_date(pub_raw)
            articles.append({
                "title":           title,
                "url":             link,
                "source":          "GlobeNewswire",
                "date":            pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "source_type":     "news",
                "source_category": "globenewswire",
            })
        print(f"[GNW] {ticker}: {len(articles)} articles")
        return articles
    except Exception as e:
        print(f"[GNW] {ticker} failed: {e}")
        return []


def _get_wire_news_via_google(ticker: str) -> list:
    """
    Searches Google News for PR Newswire and Business Wire press releases that
    mention a specific ticker in exchange-prefix format: (NASDAQ: TICK).
    Returns articles tagged pr_newswire or businesswire based on source.
    """
    q_parts = " OR ".join(
        f'"{ex}: {ticker}"' for ex in _WIRE_EXCHANGE_PATTERNS
    )
    query = f"(site:prnewswire.com OR site:businesswire.com) ({q_parts})"
    url   = (
        "https://news.google.com/rss/search"
        f"?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        if r.status_code != 200:
            print(f"[WIRE-GOOGLE] HTTP {r.status_code} for {ticker}")
            return []
        root  = ET.fromstring(r.content)
        items = root.findall(".//item")
        articles = []
        for item in items:
            title = (item.findtext("title") or "").strip()
            if _WIRE_NOISE_RE.search(title):
                continue
            # Determine source from title suffix added by Google News
            title_lower = title.lower()
            if "business wire" in title_lower or "businesswire" in title_lower:
                source_category = "businesswire"
                source_label    = "Business Wire"
            else:
                source_category = "pr_newswire"
                source_label    = "PR Newswire"
            # Strip " - PR Newswire" / " - Business Wire" suffix Google appends
            title = re.sub(
                r"\s+-\s+(?:PR Newswire|Business Wire|BusinessWire)$", "", title
            ).strip()
            link    = (item.findtext("link") or "").strip()
            pub_raw = (item.findtext("pubDate") or "").strip()
            pub_dt  = _parse_news_date(pub_raw)
            articles.append({
                "title":           title,
                "url":             link,
                "source":          source_label,
                "date":            pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "date_dt":         pub_dt,
                "source_type":     "news",
                "source_category": source_category,
            })
        prn = sum(1 for a in articles if a["source_category"] == "pr_newswire")
        bw  = sum(1 for a in articles if a["source_category"] == "businesswire")
        print(f"[WIRE-GOOGLE] {ticker}: {prn} PRN, {bw} BW articles")
        return articles
    except Exception as e:
        print(f"[WIRE-GOOGLE] {ticker} failed: {e}")
        return []


def get_ticker_news(ticker: str) -> list:
    """
    Returns recent news for a ticker from five sources, each tagged with
    `source_category` for frontend filtering:
      pr_newswire   — PR Newswire articles (via Google News search)
      businesswire  — Business Wire articles (via Google News search)
      globenewswire — GlobeNewswire press releases (direct RSS)
      benzinga      — Benzinga articles via Finnhub
      fda           — Finviz Stocks News items with an FDA event tag
      sec           — SEC EDGAR 8-K filings (no date cutoff)

    Wire/Benzinga articles limited to the last 14 days.
    Cached per ticker for 30 minutes.
    """
    now    = time.time()
    cached = _ticker_news_cache.get(ticker)
    if cached and (now - cached["ts"]) < _TICKER_NEWS_TTL:
        return cached["articles"]

    articles  = []
    cutoff_dt = datetime.now() - timedelta(days=14)

    # ── 1. PR Newswire + Business Wire via Google News per-ticker search
    for item in _get_wire_news_via_google(ticker):
        if not item["date_dt"] or item["date_dt"] < cutoff_dt:
            continue
        articles.append({k: v for k, v in item.items() if k != "date_dt"})

    # ── 2. GlobeNewswire press releases
    for item in _get_globenewswire_news(ticker):
        pub_dt = _parse_news_date(item["date"])
        if not pub_dt or pub_dt < cutoff_dt:
            continue
        articles.append(item)

    # ── 3. Benzinga via Finnhub (filter to Benzinga only — skip Yahoo/CNBC/etc.)
    if FINNHUB_API_KEY:
        try:
            to_date   = datetime.now().strftime("%Y-%m-%d")
            from_date = cutoff_dt.strftime("%Y-%m-%d")
            url = (
                "https://finnhub.io/api/v1/company-news"
                f"?symbol={ticker}&from={from_date}&to={to_date}"
                f"&token={FINNHUB_API_KEY}"
            )
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                bzn_count = 0
                for item in r.json():
                    source = item.get("source", "")
                    if "benzinga" not in source.lower():
                        continue
                    dt = datetime.fromtimestamp(item.get("datetime", 0))
                    articles.append({
                        "title":           item.get("headline", "").strip(),
                        "url":             item.get("url", ""),
                        "source":          source,
                        "date":            dt.strftime("%Y-%m-%d"),
                        "source_type":     "news",
                        "source_category": "benzinga",
                    })
                    bzn_count += 1
                print(f"[FINNHUB] {ticker}: {bzn_count} Benzinga articles")
            else:
                print(f"[FINNHUB] HTTP {r.status_code} for {ticker}")
        except Exception as e:
            print(f"[FINNHUB] Failed for {ticker}: {e}")

    # ── 2. FDA catalysts (from Finviz Stocks News event tags) ────────────────
    for item in get_finviz_stocks_news().get(ticker, []):
        if not (item.get("event_type") or "").startswith("FDA"):
            continue
        article_dt = _parse_news_date(item.get("date", ""))
        if article_dt and article_dt < cutoff_dt:
            continue
        articles.append({
            "title":           item.get("title", ""),
            "url":             item.get("url", ""),
            "source":          item.get("source", ""),
            "date":            item.get("date", ""),
            "source_type":     "news",
            "source_category": "fda",
        })

    # ── 3. SEC EDGAR 8-K filings (no date cutoff — filings are always relevant)
    try:
        atom_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcompany&CIK={ticker}&type=8-K"
            f"&dateb=&owner=include&count=5&output=atom"
        )
        r = requests.get(
            atom_url,
            headers={"User-Agent": "stocktwits-dashboard samgrana100@gmail.com"},
            timeout=10,
        )
        if r.status_code == 200 and r.content:
            ns   = {"atom": "http://www.w3.org/2005/Atom"}
            root = ET.fromstring(r.content)
            for entry in root.findall("atom:entry", ns)[:5]:
                title   = (entry.findtext("atom:title",   "", ns) or "").strip()
                updated = (entry.findtext("atom:updated", "", ns) or "")[:10]
                link_el = entry.find("atom:link", ns)
                link    = link_el.get("href", "") if link_el is not None else ""
                if not title:
                    continue
                articles.append({
                    "title":           title,
                    "url":             link,
                    "source":          "SEC 8-K",
                    "date":            updated,
                    "source_type":     "filing",
                    "source_category": "sec",
                })
    except Exception as e:
        print(f"[TICKER NEWS] SEC {ticker}: {e}")

    articles.sort(key=lambda a: a.get("date", ""), reverse=True)
    _ticker_news_cache[ticker] = {"articles": articles, "ts": now}
    return articles


# ─── ROUTES ───

# ── ROUTES ──

@app.route("/")
def index():
    return render_template("index.html")


# ── /api/scores — main dashboard endpoint ──
# Merges FinBERT composite scores (from MongoDB) with Finviz screener metadata.
# Filtered to only the tickers currently in the Finviz screener so the table
# stays focused on the user's active watchlist.
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


# Generates the shared time axis used to align price, score, and density series in the chart.
# Builds a regular grid snapped to step boundaries rather than relying on price bar timestamps,
# which can be sparse or absent outside market hours.
def _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label):
    # Always generate a regular time axis covering the full window so that
    # score/density data is never cut off by sparse price bar timestamps.
    # Axis is snapped to clean step boundaries so frontend snapKey lookups match.
    axis = []
    step = 1 if window_min <= 1440 else (60 if window_min <= 10080 else 1440)

    # Snap start to the nearest step boundary (UTC minutes since midnight).
    # UTC-ET offset is always whole hours, so snapping in UTC = snapping in ET.
    raw = window_start_dt.replace(tzinfo=timezone.utc, second=0, microsecond=0)
    total_min = raw.hour * 60 + raw.minute
    snapped   = (total_min // step) * step
    current   = raw.replace(hour=snapped // 60, minute=snapped % 60)

    while current <= now_utc:
        axis.append(utc_to_et(current.strftime("%Y-%m-%dT%H:%M:%SZ")))
        current += timedelta(minutes=step)
    return axis


# ── /api/ticker — single-ticker detail (popup) ──
# Runs multiple MongoDB queries to assemble score history, density history,
# price history, and event markers for the popup chart. Results are cached
# for TICKER_DETAIL_TTL seconds to avoid hammering MongoDB on rapid opens.
@app.route("/api/ticker/<ticker>")
def get_ticker_detail(ticker):
    window_key  = request.args.get("window", "1h")
    rolling_key = request.args.get("rolling", window_key)
    ticker      = ticker.upper()

    cache_key = f"{ticker}|{window_key}|{rolling_key}"
    now_ts    = time.time()
    with _ticker_detail_lock:
        cached = ticker_detail_cache.get(cache_key)
        if cached and (now_ts - cached["ts"]) < TICKER_DETAIL_TTL:
            return jsonify(cached["data"])

    config        = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"])
    window_min    = config["minutes"]
    finviz_param  = config["finviz_param"]
    window_start  = get_window_start_iso(window_min)

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
    # Fetch density_rolling_min extra history before the chart window so the
    # rolling sum is primed at the chart's left edge (not starting from zero).
    density_window_key  = request.args.get("density_window", rolling_key)
    density_config_d    = WINDOW_CONFIG.get(density_window_key, rolling_config)
    density_rolling_min = density_config_d["minutes"]
    density_start       = get_window_start_iso(window_min + density_rolling_min)

    pipeline_query = [
        {
            "$match": {
                "ticker":         ticker,
                "created_at_utc": {"$gte": density_start}
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
    price_hist = get_finviz_price_history(ticker, finviz_param, multi_day=(window_min >= 1440))

    month    = datetime.now(timezone.utc).month
    tz_label = "EDT" if 3 <= month <= 11 else "EST"
    time_axis = _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label)

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

    out = {
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
    }
    with _ticker_detail_lock:
        stale = [k for k, v in ticker_detail_cache.items() if (now_ts - v["ts"]) >= TICKER_DETAIL_TTL]
        for k in stale:
            del ticker_detail_cache[k]
        ticker_detail_cache[cache_key] = {"data": out, "ts": now_ts}
    return jsonify(out)


@app.route("/api/tickers/batch")
def get_tickers_batch():
    """
    Returns chart data for multiple tickers in one request.
    Runs 4 MongoDB queries total instead of N×2, then builds per-ticker
    responses. Results are stored in ticker_detail_cache so subsequent
    popup opens for the same tickers are served instantly.
    """
    tickers_param = request.args.get("tickers", "")
    window_key    = request.args.get("window",  "1h")
    rolling_key   = request.args.get("rolling", "5m")

    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()][:100]
    if not tickers:
        return jsonify({}), 400

    config        = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"])
    window_min    = config["minutes"]
    finviz_param  = config["finviz_param"]
    window_start  = get_window_start_iso(window_min)

    rolling_config = WINDOW_CONFIG.get(rolling_key, WINDOW_CONFIG["5m"])
    rolling_min    = rolling_config["minutes"]
    rolling_start  = get_window_start_iso(rolling_min)

    density_window_key  = request.args.get("density_window", rolling_key)
    density_config_d    = WINDOW_CONFIG.get(density_window_key, rolling_config)
    density_rolling_min = density_config_d["minutes"]
    density_start       = get_window_start_iso(window_min + density_rolling_min)

    now_utc         = datetime.now(timezone.utc)
    window_start_dt = now_utc - timedelta(minutes=window_min)
    month           = now_utc.month
    tz_label        = "EDT" if 3 <= month <= 11 else "EST"
    now_ts          = time.time()

    # Serve from cache where available; only query MongoDB for the rest
    results   = {}
    uncached  = []
    for t in tickers:
        ck = f"{t}|{window_key}|{rolling_key}|{density_window_key}"
        with _ticker_detail_lock:
            cached = ticker_detail_cache.get(ck)
            if cached and (now_ts - cached["ts"]) < TICKER_DETAIL_TTL:
                results[t] = cached["data"]
            else:
                uncached.append(t)

    if not uncached:
        return jsonify(results)

    # ── 1. Score history (all uncached tickers, one query) ──
    all_score_docs = list(scored_messages.find(
        {"ticker": {"$in": uncached}, "created_at_utc": {"$gte": window_start},
         "composite_score": {"$ne": None}},
        {"ticker": 1, "composite_score": 1, "sentiment_score": 1,
         "trust_score": 1, "impact_score": 1, "created_at_utc": 1, "_id": 0}
    ).sort("created_at_utc", 1).limit(100_000))

    # ── 2. Message density (one aggregation) ──
    density_docs = list(scored_messages.aggregate([
        {"$match": {"ticker": {"$in": uncached}, "created_at_utc": {"$gte": density_start}}},
        {"$group": {
            "_id":   {"ticker": "$ticker", "minute": {"$substr": ["$created_at_utc", 0, 16]}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.minute": 1}}
    ]))

    # ── 3. Rolling-window scores ──
    rolling_docs = list(scored_messages.find(
        {"ticker": {"$in": uncached}, "created_at_utc": {"$gte": rolling_start},
         "composite_score": {"$ne": None}},
        {"ticker": 1, "composite_score": 1, "sentiment_score": 1,
         "trust_score": 1, "impact_score": 1, "_id": 0}
    ).limit(100_000))

    # ── 4. Rolling message totals (one aggregation instead of N count_documents) ──
    total_docs = list(scored_messages.aggregate([
        {"$match": {"ticker": {"$in": uncached}, "created_at_utc": {"$gte": rolling_start}}},
        {"$group": {"_id": "$ticker", "total": {"$sum": 1}}}
    ]))

    # ── 5. Event markers ──
    event_docs = list(scored_messages.find(
        {"ticker": {"$in": uncached}, "created_at_utc": {"$gte": window_start},
         "event_type": {"$ne": None}},
        {"ticker": 1, "event_type": 1, "created_at_utc": 1, "body": 1, "user": 1, "_id": 0}
    ).sort("created_at_utc", 1).limit(10_000))

    # Split results by ticker
    score_by_ticker   = defaultdict(list)
    density_by_ticker = defaultdict(list)
    rolling_by_ticker = defaultdict(list)
    total_by_ticker   = {}
    events_by_ticker  = defaultdict(list)

    for d in all_score_docs:
        score_by_ticker[d["ticker"]].append(d)
    for d in density_docs:
        density_by_ticker[d["_id"]["ticker"]].append(d)
    for d in rolling_docs:
        rolling_by_ticker[d["ticker"]].append(d)
    for d in total_docs:
        total_by_ticker[d["_id"]] = d["total"]
    for d in event_docs:
        events_by_ticker[d["ticker"]].append(d)

    finviz    = get_finviz_data()
    news_data = get_finviz_stocks_news()

    # ── Build per-ticker response ──
    # Price is intentionally omitted here — the frontend fetches it lazily
    # per-card via /api/price/<ticker> so Finviz is never hammered all at once.
    for t in uncached:
        score_docs_t   = score_by_ticker[t]
        density_docs_t = density_by_ticker[t]
        rolling_docs_t = rolling_by_ticker[t]
        event_docs_t   = events_by_ticker[t]
        price_hist     = []

        score_history = [
            {"time": utc_to_et(d["created_at_utc"]), "score": d["composite_score"],
             "sort_key": d["created_at_utc"]}
            for d in score_docs_t
        ]
        density_history = [
            {"time": utc_to_et(d["_id"]["minute"] + ":00Z"),
             "count": d["count"], "sort_key": d["_id"]["minute"]}
            for d in density_docs_t
        ]
        time_axis = _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label)

        latest_scores = {}
        if rolling_docs_t:
            n = len(rolling_docs_t)
            latest_scores = {
                "composite_score": round(sum(d["composite_score"]            for d in rolling_docs_t) / n, 4),
                "sentiment_score": round(sum((d.get("sentiment_score") or 0) for d in rolling_docs_t) / n, 4),
                "trust_score":     round(sum((d.get("trust_score")     or 0) for d in rolling_docs_t) / n, 4),
                "impact_score":    round(sum((d.get("impact_score")    or 0) for d in rolling_docs_t) / n, 4),
                "scored_count":    n,
            }

        fv                    = finviz.get(t, {})
        confirmed_articles    = news_data.get(t, [])[:10]
        confirmed_event_types = list({a["event_type"] for a in confirmed_articles if a["event_type"]})

        event_history = [
            {
                "time":       utc_to_et(d["created_at_utc"]),
                "event_type": d["event_type"],
                "body":       (d.get("body") or "").strip(),
                "username":   (d.get("user") or {}).get("username", "")
                              if isinstance(d.get("user"), dict) else "",
            }
            for d in event_docs_t
        ]

        out = {
            "status":                "ok",
            "ticker":                t,
            "window_key":            window_key,
            "window_minutes":        window_min,
            "rolling_key":           rolling_key,
            "rolling_message_total": total_by_ticker.get(t, 0),
            "company":               fv.get("company",  "--"),
            "sector":                fv.get("sector",   "--"),
            "industry":              fv.get("industry", "--"),
            "latest_scores":         latest_scores,
            "score_history":         score_history,
            "density_history":       density_history,
            "price_history":         price_hist,
            "time_axis":             time_axis,
            "event_history":         event_history,
            "confirmed_news":        confirmed_articles,
            "confirmed_event_types": confirmed_event_types,
        }

        results[t] = out

    return jsonify(results)


@app.route("/api/price/<ticker>")
def get_price_endpoint(ticker):
    """Lightweight price-only endpoint — no MongoDB queries. Used by News View cards."""
    ticker     = ticker.upper()
    window_key = request.args.get("window", "1h")
    config     = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"])
    window_min = config["minutes"]
    finviz_param = config["finviz_param"]

    now_utc        = datetime.now(timezone.utc)
    window_start_dt = now_utc - timedelta(minutes=window_min)
    month          = now_utc.month
    tz_label       = "EDT" if 3 <= month <= 11 else "EST"

    price_hist = get_finviz_price_history(ticker, finviz_param, multi_day=(window_min >= 1440))
    time_axis  = _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label)

    return jsonify({"price_history": price_hist, "time_axis": time_axis})


@app.route("/api/finviz-news/<ticker>")
def get_finviz_news_endpoint(ticker):
    """Lightweight endpoint — returns cached Finviz news articles for one ticker."""
    ticker    = ticker.upper()
    news_data = get_finviz_stocks_news()
    articles  = news_data.get(ticker, [])[:10]
    return jsonify({"articles": articles})


@app.route("/api/ticker-news/<ticker>")
def get_ticker_news_endpoint(ticker):
    """Returns aggregated news for one ticker from PRN, Business Wire, GlobeNewswire, Benzinga, and SEC EDGAR (cached 30 min)."""
    ticker   = ticker.upper()
    articles = get_ticker_news(ticker)
    return jsonify({"articles": articles})


@app.route("/api/upcoming-catalysts")
def get_upcoming_catalysts():
    """
    Returns PDUFA and earnings dates for the requested tickers.
    Query param: tickers=AAPL,MRNA,NVDA  (comma-separated)

    Response shape:
    {
      "MRNA": {"pdufa":    {"date": "2026-07-18", "days_until": 24, "description": "mRNA-1283 ..."}},
      "AAPL": {"earnings": {"date": "2026-07-31", "days_until": 37, "description": "Earnings Report"}},
    }
    """
    raw_tickers = request.args.get("tickers", "")
    tickers     = [t.strip().upper() for t in raw_tickers.split(",") if t.strip()]
    if not tickers:
        return jsonify({})

    today = datetime.now(timezone.utc).date()
    docs  = list(upcoming_catalysts.find(
        {
            "ticker":     {"$in": tickers},
            "date":       {"$gte": (today - timedelta(days=3)).isoformat()},
        },
        {"_id": 0, "ticker": 1, "event_type": 1, "date": 1, "description": 1},
    ))

    result = {}
    for doc in docs:
        ticker     = doc["ticker"]
        event_type = doc["event_type"]
        try:
            cat_date   = date.fromisoformat(doc["date"])
        except ValueError:
            continue
        days_until = (cat_date - today).days

        result.setdefault(ticker, {})[event_type] = {
            "date":        doc["date"],
            "days_until":  days_until,
            "description": doc.get("description", ""),
        }

    return jsonify(result)


# ── /api/bubble-screener — 2D bubble map endpoint ──
# Accepts filter params (cap, relvol, vol, float, change) as POST JSON.
# Fetches filtered Finviz data, overlays 60-min sentiment from MongoDB,
# and appends intraday trail history from screener_snapshots.
@app.route("/api/bubble-screener", methods=["POST"])
def get_bubble_screener():
    data = request.get_json(force=True) or {}

    filters = []
    cap = data.get("cap", "any")
    if cap and cap != "any":
        filters.append(cap)
    relvol = data.get("relvol", "")
    if relvol:
        try:
            if float(relvol) > 0:
                filters.append(f"sh_relvol_o{relvol}")
        except ValueError:
            pass
    vol = data.get("vol", "")
    if vol:
        try:
            if int(float(vol)) > 0:
                filters.append(f"sh_curvol_o{vol}")
        except ValueError:
            pass
    float_max = data.get("float_max", "")
    if float_max:
        try:
            if float(float_max) > 0:
                filters.append(f"sh_float_u{int(float(float_max))}")
        except ValueError:
            pass
    chg = data.get("chg", "any")
    if chg and chg != "any":
        filters.append(chg)

    cache_key = ",".join(sorted(filters)) if filters else "_all_"
    with _bubble_screener_lock:
        cached = _bubble_screener_cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < BUBBLE_SCREENER_TTL:
            return jsonify(cached["data"])

    token      = os.getenv("FINVIZ_API_TOKEN", "") or FINVIZ_API_TOKEN
    export_url = os.getenv("FINVIZ_EXPORT_URL", "") or FINVIZ_EXPORT_URL
    if not token or not export_url:
        return jsonify({"error": "Finviz credentials not configured"}), 500

    def _bubble_build_url(base, v, filt_list):
        """Build a Finviz export URL: swap view, replace f=, append auth."""
        u = re.sub(r"v=\d+", f"v={v}", base)
        u = re.sub(r"&?f=[^&]+", "", u)
        u = re.sub(r"&?auth=[^&]+", "", u).rstrip("?").rstrip("&")
        sep = "&" if "?" in u else "?"
        f_part = f"f={','.join(filt_list)}&" if filt_list else ""
        return f"{u}{sep}{f_part}auth={token}"

    def _bubble_fetch_df(v):
        url = _bubble_build_url(export_url, v, filters)
        try:
            r = cffi_requests.get(url, impersonate="chrome120", timeout=15)
            if r.status_code != 200:
                print(f"[BUBBLE] Finviz v={v} returned {r.status_code}")
                return None
            df = pd.read_csv(StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:
            print(f"[BUBBLE] Finviz v={v} error: {e}")
            return None

    # v=111 (Overview): Company, Price, Change, Volume
    # v=151 (Technical): Rel Volume, Avg Volume
    df_ov  = _bubble_fetch_df(111)
    df_tec = _bubble_fetch_df(151)

    if df_ov is None and df_tec is None:
        return jsonify({"error": "Both Finviz views failed"}), 502

    # Merge both views by ticker (same pattern as get_finviz_data)
    merged = {}
    if df_ov is not None and "Ticker" in df_ov.columns:
        for _, row in df_ov.iterrows():
            tk = str(row.get("Ticker", "")).strip().upper()
            if tk:
                merged[tk] = {
                    "company": str(row.get("Company", "--")),
                    "price":   str(row.get("Price",   "--")),
                    "change":  str(row.get("Change",  "--")),
                    "volume":  str(row.get("Volume",  "--")),
                    "rel_volume": "--",
                }
    if df_tec is not None and "Ticker" in df_tec.columns:
        for _, row in df_tec.iterrows():
            tk = str(row.get("Ticker", "")).strip().upper()
            if not tk:
                continue
            rv = str(row.get("Rel Volume") or row.get("Relative Volume") or "--")
            if tk in merged:
                merged[tk]["rel_volume"] = rv
            else:
                merged[tk] = {
                    "company": str(row.get("Company", "--")),
                    "price":   str(row.get("Price",   "--")),
                    "change":  str(row.get("Change",  "--")),
                    "volume":  str(row.get("Volume",  "--")),
                    "rel_volume": rv,
                }

    if not merged:
        return jsonify([])

    ticker_list = list(merged.keys())

    def _pf(v):
        try:
            return float(str(v).replace("%", "").replace(",", "").strip())
        except Exception:
            return None

    # Sentiment from scored_messages for these tickers (last 60 min)
    window_start = get_window_start_iso(60)
    sent_map = {}
    if ticker_list:
        for r in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": window_start}, "ticker": {"$in": ticker_list}, "sentiment_score": {"$ne": None}}},
            {"$group": {"_id": "$ticker", "avg": {"$avg": "$sentiment_score"}, "cnt": {"$sum": 1}}}
        ]):
            sent_map[r["_id"]] = (round(r["avg"], 4), r["cnt"])

    # Intraday trails from screener_snapshots — today only (UTC midnight).
    # Finviz "change" is always vs prior close, so mixing two calendar days
    # creates artificial baseline jumps in the Y axis.
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    trail_map   = defaultdict(list)
    if ticker_list:
        for d in screener_snapshots.find(
            {"ts": {"$gte": today_start}, "ticker": {"$in": ticker_list}},
            {"ticker": 1, "ts": 1, "price_chg": 1, "rel_vol": 1, "_id": 0}
        ).sort("ts", 1):
            trail_map[d["ticker"]].append({
                "price_chg": d["price_chg"],
                "rel_vol":   d["rel_vol"],
                "ts_et":     _utc_dt_to_et_hhmm(d["ts"]),
                "ts_bkt":    d["ts"].strftime("%Y-%m-%dT%H:%M"),   # UTC bucket for density lookup
            })

    # Per-bucket density counts for trail points (density mode)
    today_str = today_start.strftime("%Y-%m-%dT%H:%M:%S")
    dens_map_bkt = defaultdict(dict)
    if ticker_list:
        for d in scored_messages.aggregate([
            {"$match": {"created_at_utc": {"$gte": today_str}, "ticker": {"$in": ticker_list}}},
            {"$group": {
                "_id": {"ticker": "$ticker", "bucket": {"$substr": ["$created_at_utc", 0, 16]}},
                "count": {"$sum": 1}
            }}
        ]):
            dens_map_bkt[d["_id"]["ticker"]][d["_id"]["bucket"]] = d["count"]

    result = []
    for tk, fv in merged.items():
        price_chg = _pf(fv.get("change"))
        rel_vol   = _pf(fv.get("rel_volume"))
        volume    = _pf(fv.get("volume"))
        if not tk or any(v is None for v in [price_chg, rel_vol, volume]):
            continue
        sentiment, density = sent_map.get(tk, (0.0, 0))
        tk_dens_bkt = dens_map_bkt.get(tk, {})
        trail = [
            {
                "x":       sentiment,
                "density": tk_dens_bkt.get(snap["ts_bkt"], 0),
                "y":       snap["price_chg"],
                "z":       snap["rel_vol"],
                "t":       snap["ts_et"],
            }
            for snap in trail_map.get(tk, [])
        ]
        result.append({
            "ticker":    tk,
            "x":         sentiment,
            "y":         price_chg,
            "z":         max(0.0, rel_vol),
            "size":      volume,
            "density":   density,
            "composite": 0.0,
            "company":   fv.get("company", "--"),
            "price":     fv.get("price", "--"),
            "change":    price_chg if price_chg is not None else fv.get("change", "--"),
            "trail":     trail,
        })

    # Persist snapshots so every ticker visible in the bubble map accumulates
    # trail history, not just those in the main Finviz screener.
    # Runs only on real fetches (cache hits return early above).
    now_utc = datetime.now(timezone.utc)
    snaps = [
        {"ticker": item["ticker"], "ts": now_utc,
         "price_chg": item["y"], "rel_vol": max(0.0, item["z"]), "volume": item["size"]}
        for item in result
    ]
    if snaps:
        try:
            screener_snapshots.insert_many(snaps, ordered=False)
        except Exception:
            pass

    with _bubble_screener_lock:
        _bubble_screener_cache[cache_key] = {"data": result, "ts": time.time()}

    return jsonify(result)


# ── /api/3d-screener — 3D bubble map endpoint ──
# Axes: X = avg sentiment score, Y = price % change, Z = relative volume.
# Builds a per-ticker trail by joining screener_snapshots (price/vol over time)
# with per-minute sentiment averages so each trail point has an accurate X value.
@app.route("/api/3d-screener")
def get_3d_screener():
    scores = aggregate_ticker_scores(rolling_window_minutes=60)
    finviz = get_finviz_data()

    def _pf(v):
        try:
            return float(str(v).replace("%","").replace(",","").strip())
        except Exception:
            return None

    # Today only — Finviz "change" baseline shifts between calendar days,
    # so mixing yesterday's snapshots with today's creates unnatural Y-axis jumps.
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    snap_docs   = list(screener_snapshots.find(
        {"ts": {"$gte": today_start}},
        {"ticker": 1, "ts": 1, "price_chg": 1, "rel_vol": 1, "_id": 0}
    ).sort("ts", 1))
    trail_map = defaultdict(list)
    for d in snap_docs:
        trail_map[d["ticker"]].append({
            "price_chg": d["price_chg"],
            "rel_vol":   d["rel_vol"],
            "ts":        d["ts"].strftime("%H:%M"),       # UTC — used for sentiment key lookup
            "ts_et":     _utc_dt_to_et_hhmm(d["ts"]),   # ET — used for display/slider
        })

    # Sentiment history from scored_messages (per-ticker per-5min bucket today)
    today_str = today_start.strftime("%Y-%m-%dT%H:%M:%S")
    sent_hist = list(scored_messages.aggregate([
        {"$match": {"created_at_utc": {"$gte": today_str}, "sentiment_score": {"$ne": None}}},
        {"$group": {
            "_id": {
                "ticker": "$ticker",
                "bucket": {"$substr": ["$created_at_utc", 0, 16]}
            },
            "avg_sent": {"$avg": "$sentiment_score"},
            "count":    {"$sum": 1}
        }}
    ]))
    sent_map = defaultdict(dict)
    dens_map = defaultdict(dict)
    for d in sent_hist:
        tk  = d["_id"]["ticker"]
        bkt = d["_id"]["bucket"]
        sent_map[tk][bkt] = round(d["avg_sent"], 4)
        dens_map[tk][bkt] = d["count"]

    # Union of snapshot tickers and sentiment tickers, both intersected with
    # current Finviz data. This lets tickers with price/vol history but no
    # Stocktwits messages still appear with their price trail.
    scores_map  = {s["ticker"]: s for s in scores}
    all_tickers = (set(trail_map.keys()) | set(scores_map.keys())) & set(finviz.keys())

    result = []
    for ticker in all_tickers:
        fv        = finviz[ticker]
        price_chg = _pf(fv.get("change"))
        rel_vol   = _pf(fv.get("rel_volume"))
        volume    = _pf(fv.get("volume"))
        if any(v is None for v in [price_chg, rel_vol, volume]):
            continue

        s            = scores_map.get(ticker)
        sentiment    = s.get("avg_sentiment_score") if s else None
        no_sentiment = sentiment is None
        sentiment    = sentiment or 0.0

        # Build trail: for no-sentiment tickers x=0 throughout (no Stocktwits data)
        trail = []
        tk_sent = sent_map.get(ticker, {})
        tk_dens = dens_map.get(ticker, {})
        for snap in trail_map.get(ticker, []):
            snap_bucket = today_start.strftime("%Y-%m-%dT") + snap["ts"]
            sent_val    = 0.0 if no_sentiment else tk_sent.get(snap_bucket, sentiment)
            dens_val    = tk_dens.get(snap_bucket, 0)
            trail.append({
                "x":       sent_val,
                "density": dens_val,
                "y":       snap["price_chg"],
                "z":       snap["rel_vol"],
                "t":       snap["ts_et"],
            })

        result.append({
            "ticker":       ticker,
            "x":            round(sentiment, 4),
            "y":            price_chg,
            "z":            max(0.0, rel_vol),
            "size":         volume,
            "density":      s.get("total_messages", 0) if s else 0,
            "composite":    round(s.get("avg_composite_score") or 0, 3) if s else 0,
            "company":      fv.get("company", "--"),
            "price":        fv.get("price", "--"),
            "change":       price_chg,
            "no_sentiment": no_sentiment,
            "trail":        trail,
        })

    return jsonify(result)


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

    # Build confirmed event type sets per ticker from Finviz news (cached 10 min)
    news_data = get_finviz_stocks_news()
    confirmed_by_ticker = {
        t: {a["event_type"] for a in articles if a["event_type"]}
        for t, articles in news_data.items()
    }

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
        ticker   = d.get("ticker", "")

        # Use stored event_type if present; fall back to inline detection for older docs
        event_type = d.get("event_type") or detect_event(body)
        finviz_confirmed = bool(
            event_type and event_type in confirmed_by_ticker.get(ticker, set())
        )

        result.append({
            "ticker":            ticker,
            "body":              body,
            "composite_score":   round(d.get("composite_score") or 0, 4),
            "sentiment_score":   round(d.get("sentiment_score") or 0, 4),
            "trust_score":       round(d.get("trust_score")     or 0, 4),
            "impact_score":      round(d.get("impact_score")    or 0, 4),
            "posted_et":         utc_to_et(created) if created else "",
            "delay_seconds":     delay_sec,
            "username":          username,
            "event_type":        event_type,
            "finviz_confirmed":  finviz_confirmed,
        })

    return jsonify({"messages": result, "threshold": threshold})


# ── AUTO TRADE ROUTES ──

# Returns active positions, the last 50 completed trades, and saved trades.
@app.route("/api/auto-trades")
def get_auto_trades_endpoint():
    def fmt(dt):
        if not dt: return ""
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(dt, "strftime") else str(dt)

    active = [{
        "ticker":        t["ticker"],
        "entry_price":   t.get("entry_price"),
        "entry_corr":    t.get("entry_corr"),
        "entered_at":    fmt(t.get("entered_at")),
        "peak_density":  t.get("peak_density", 0),
        "entry_density": t.get("entry_density", 0),
    } for t in auto_trades.find({"status": "active"}, {"_id": 0})]

    def _fmt_completed(t):
        return {
            "ticker":       t["ticker"],
            "entry_price":  t.get("entry_price"),
            "exit_price":   t.get("exit_price"),
            "return_pct":   t.get("return_pct"),
            "entered_at":   fmt(t.get("entered_at")),
            "exited_at":    fmt(t.get("exited_at")),
            "entry_corr":   t.get("entry_corr"),
            "peak_density": t.get("peak_density", 0),
            "saved":        t.get("saved", False),
        }

    recent_50   = list(completed_auto_trades.find({}, {"_id": 0}).sort("exited_at", -1).limit(50))
    recent_keys = {(t["ticker"], str(t.get("exited_at"))): True for t in recent_50}
    saved_extra = [
        t for t in completed_auto_trades.find({"saved": True}, {"_id": 0})
        if (t["ticker"], str(t.get("exited_at"))) not in recent_keys
    ]
    completed = [_fmt_completed(t) for t in recent_50 + saved_extra]

    return jsonify({
        "active": active, "completed": completed,
        "config": {"entry_corr": AUTO_ENTRY_CORR, "entry_min_dens": AUTO_ENTRY_MIN_DENS, "exit_peak": AUTO_EXIT_PEAK, "exit_min_drop": AUTO_EXIT_MIN_DROP}
    })


# Allows the frontend to adjust entry/exit thresholds without restarting the server.
@app.route("/api/auto-trade-config", methods=["POST"])
def set_auto_trade_config():
    global AUTO_ENTRY_CORR, AUTO_ENTRY_MIN_DENS, AUTO_EXIT_PEAK, AUTO_EXIT_MIN_DROP
    data = request.get_json(force=True) or {}
    try:
        if "entry_corr" in data:
            val = float(data["entry_corr"])
            if 0.0 < val <= 1.0:
                AUTO_ENTRY_CORR = round(val, 2)
        if "entry_min_dens" in data:
            val = int(data["entry_min_dens"])
            if val >= 0:
                AUTO_ENTRY_MIN_DENS = val
        if "exit_peak" in data:
            val = float(data["exit_peak"])
            if 0.0 < val <= 1.0:
                AUTO_EXIT_PEAK = round(val, 2)
        if "exit_min_drop" in data:
            val = int(data["exit_min_drop"])
            if val >= 0:
                AUTO_EXIT_MIN_DROP = val
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid config value: {e}"}), 400
    print(f"[AUTO TRADE CONFIG] entry_corr={AUTO_ENTRY_CORR} entry_min_dens={AUTO_ENTRY_MIN_DENS} exit_peak={AUTO_EXIT_PEAK} exit_min_drop={AUTO_EXIT_MIN_DROP}")
    return jsonify({"entry_corr": AUTO_ENTRY_CORR, "entry_min_dens": AUTO_ENTRY_MIN_DENS, "exit_peak": AUTO_EXIT_PEAK, "exit_min_drop": AUTO_EXIT_MIN_DROP})


@app.route("/api/auto-trades/clear", methods=["POST"])
def clear_auto_trades():
    auto_trades.delete_many({})
    completed_auto_trades.delete_many({})
    return jsonify({"cleared": True})


@app.route("/api/auto-trades/clear-active", methods=["POST"])
def clear_active_auto_trades():
    auto_trades.delete_many({})
    return jsonify({"cleared": True})


@app.route("/api/auto-trades/clear-history", methods=["POST"])
def clear_auto_trade_history():
    completed_auto_trades.delete_many({})
    return jsonify({"cleared": True})


@app.route("/api/auto-trade/save", methods=["POST"])
def toggle_auto_trade_saved():
    data      = request.get_json(force=True) or {}
    ticker    = data.get("ticker", "")
    exited_at = data.get("exited_at", "")
    saved     = bool(data.get("saved", False))
    if not ticker or not exited_at:
        return jsonify({"error": "ticker and exited_at required"}), 400
    try:
        exited_dt = datetime.strptime(exited_at, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return jsonify({"error": "invalid exited_at format"}), 400
    completed_auto_trades.update_one(
        {"ticker": ticker, "exited_at": exited_dt},
        {"$set": {"saved": saved}}
    )
    return jsonify({"ticker": ticker, "saved": saved})


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

    news_data      = get_finviz_stocks_news()
    confirmed_types = {a["event_type"] for a in news_data.get(ticker, []) if a["event_type"]}

    result = []
    for d in docs:
        user     = d.get("user") or {}
        username = user.get("username", "") if isinstance(user, dict) else ""
        body     = (d.get("body") or "").strip()
        score    = d.get("composite_score")
        event_type = d.get("event_type") or detect_event(body)
        result.append({
            "body":              body,
            "username":          username,
            "posted_et":         utc_to_et(d.get("created_at_utc", "")),
            "composite_score":   round(score, 4) if score is not None else None,
            "event_type":        event_type,
            "finviz_confirmed":  bool(event_type and event_type in confirmed_types),
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


# ── PIPELINE CONTROL ROUTES ──

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


# ── SCREENER CONFIG ──
# Persists the user's filter choices to screener_config.json and hot-swaps
# FINVIZ_EXPORT_URL so the pipeline immediately picks up the new screener without a restart.

SCREENER_CONFIG_PATH = Path(__file__).parent / "screener_config.json"

_FINVIZ_CAP_MAP = {
    "mega":        "cap_mega",
    "large":       "cap_large",
    "mid":         "cap_mid",
    "small":       "cap_small",
    "micro":       "cap_micro",
    "nano":        "cap_nano",
    "plus_large":  "cap_largeover",
    "plus_mid":    "cap_midover",
    "plus_small":  "cap_smallover",
    "plus_micro":  "cap_microover",
    "minus_large": "cap_largeunder",
    "minus_mid":   "cap_midunder",
    "minus_small": "cap_smallunder",
    "minus_micro": "cap_microunder",
}

def _load_screener_config() -> dict:
    if SCREENER_CONFIG_PATH.exists():
        try:
            return json.loads(SCREENER_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _save_screener_config(config: dict):
    config["_saved_at"] = datetime.now(timezone.utc).isoformat()
    SCREENER_CONFIG_PATH.write_text(json.dumps(config, indent=2), encoding="utf-8")

def _build_finviz_filter_string(config: dict) -> str:
    parts = []

    change = config.get("change", "all")
    if change == "up":   parts.append("ta_change_u")
    elif change == "down": parts.append("ta_change_d")

    rel_vol = float(config.get("rel_vol_min") or 0)
    if rel_vol > 0:
        rv = int(rel_vol) if rel_vol == int(rel_vol) else rel_vol
        parts.append(f"sh_relvol_o{rv}")

    avg_vol = float(config.get("avg_vol_min") or 0)
    if avg_vol > 0:
        k = int(avg_vol // 1000)
        if k > 0:
            parts.append(f"sh_avgvol_o{k}")

    cur_vol = float(config.get("cur_vol_min") or 0)
    if cur_vol > 0:
        k = int(cur_vol // 1000)
        if k > 0:
            parts.append(f"sh_curvol_o{k}")

    price = float(config.get("price_min") or 0)
    if price > 0:
        p = int(price) if price == int(price) else price
        parts.append(f"sh_price_o{p}")

    cap = config.get("mkt_cap", "all")
    if cap in _FINVIZ_CAP_MAP:
        parts.append(_FINVIZ_CAP_MAP[cap])

    return ",".join(parts)

def _build_finviz_export_url(config: dict) -> str:
    f = url_quote(_build_finviz_filter_string(config))
    return f"https://elite.finviz.com/export.ashx?v=151&f={f}&o=-change"


@app.route("/api/screener-config")
def get_screener_config():
    return jsonify(_load_screener_config())


@app.route("/api/apply-screener", methods=["POST"])
def apply_screener():
    global FINVIZ_EXPORT_URL, finviz_cache, finviz_cache_time

    config = request.get_json(force=True)
    if not config:
        return jsonify({"error": "no config"}), 400

    # Persist the config to disk so it survives a server restart
    _save_screener_config(config)

    # Rebuild the Finviz export URL from the new filter params and hot-swap it
    new_url = _build_finviz_export_url(config)
    FINVIZ_EXPORT_URL = new_url
    os.environ["FINVIZ_EXPORT_URL"] = new_url

    # Flush the screener cache so the next /api/scores call fetches from the new URL
    with _finviz_lock:
        finviz_cache      = {}
        finviz_cache_time = 0

    was_running = pipeline_thread is not None and pipeline_thread.is_alive()
    print(f"[SCREENER] Config applied — filters: {_build_finviz_filter_string(config)} | pipeline_running={was_running}")

    return jsonify({"status": "ok", "was_running": was_running})


# ── AUTO TRADE ENGINE ──

# Core decision loop: checks every active position for an exit signal, then
# evaluates all screener tickers for an entry signal. Called on AUTO_LOOP_SEC cadence.
def _check_auto_trades():
    now_utc   = datetime.now(timezone.utc)

    # Only run during extended trading hours: 4:00–20:00 ET (Mon–Fri)
    et_offset = -4 if 3 <= now_utc.month <= 11 else -5
    now_et    = now_utc + timedelta(hours=et_offset)
    if now_et.weekday() >= 5:
        return
    market_open  = now_et.replace(hour=4,  minute=0, second=0, microsecond=0)
    market_close = now_et.replace(hour=20, minute=0, second=0, microsecond=0)
    if not (market_open <= now_et < market_close):
        return

    finviz    = get_finviz_data()
    window_1h  = get_window_start_iso(60)
    window_20m = get_window_start_iso(20)
    window_40m = get_window_start_iso(40)

    active_map = {t["ticker"]: t for t in auto_trades.find({"status": "active"})}

    cooldown_cutoff = now_utc - timedelta(seconds=REENTRY_COOLDOWN)
    recent_exits    = {
        t["ticker"] for t in completed_auto_trades.find(
            {"exited_at": {"$gte": cooldown_cutoff}}, {"ticker": 1}
        )
    }

    # ── EXIT monitoring — direct 1hr rolling density per active trade ──
    # Uses count_documents per ticker (same source as quickscore?window=1h) so the
    # density value is never affected by the main table's rolling window selection.
    for ticker, trade in active_map.items():
        # Use cached quote_export bars if available (no new HTTP request), else screener
        with _price_lock:
            _cached = price_cache.get(f"{ticker}_i1_today")
            _bars   = _cached["data"] if _cached else []
        current_price = _bars[-1]["price"] if _bars else finviz.get(ticker, {}).get("price")
        total_msgs    = scored_messages.count_documents({
            "ticker":         ticker,
            "created_at_utc": {"$gte": window_1h}
        })
        peak = trade.get("peak_density", 0)

        if total_msgs > peak:
            auto_trades.update_one(
                {"ticker": ticker, "status": "active"},
                {"$set": {"peak_density": total_msgs}}
            )
            peak = total_msgs

        if peak > 0 and ((peak - total_msgs) / peak) >= AUTO_EXIT_PEAK and (peak - total_msgs) >= AUTO_EXIT_MIN_DROP:
            entry_price = trade.get("entry_price")
            return_pct  = None
            try:
                if entry_price and current_price:
                    return_pct = round(
                        (float(current_price) - float(entry_price)) / float(entry_price) * 100, 2
                    )
            except Exception:
                pass

            ret_str = (("+" if return_pct >= 0 else "") + str(return_pct) + "%") if return_pct is not None else "N/A"
            completed_auto_trades.insert_one({
                "ticker":        ticker,
                "entry_price":   entry_price,
                "exit_price":    current_price,
                "return_pct":    return_pct,
                "entered_at":    trade["entered_at"],
                "exited_at":     now_utc,
                "entry_corr":    trade.get("entry_corr"),
                "peak_density":  peak,
                "exit_density":  total_msgs,
                "exit_reason":   "density_off_peak",
            })
            auto_trades.delete_one({"ticker": ticker, "status": "active"})
            drop_pct = round((peak - total_msgs) / peak * 100, 1) if peak else 0
            print(f"[AUTO TRADE] EXIT {ticker} | peak={peak} density={total_msgs} drop={peak - total_msgs} ({drop_pct}%) | price={current_price} | return: {ret_str}")

    # ── ENTRY monitoring — uses full correlation scores ──
    scores = aggregate_ticker_scores(rolling_window_minutes=60)
    for s in scores:
        ticker         = s["ticker"]
        corr           = s.get("correlation")
        screener_price = finviz.get(ticker, {}).get("price")

        if ticker in active_map or ticker in recent_exits:
            continue

        # 1hr rolling density — recorded on entry for peak tracking
        total_msgs = scored_messages.count_documents({
            "ticker":         ticker,
            "created_at_utc": {"$gte": window_1h}
        })

        # density_rising: last 20 min vs prior 20 min — blocks entries when both density and price are declining
        count_20m = scored_messages.count_documents({
            "ticker":         ticker,
            "created_at_utc": {"$gte": window_20m}
        })
        count_prior_20m = scored_messages.count_documents({
            "ticker":         ticker,
            "created_at_utc": {"$gte": window_40m, "$lt": window_20m}
        })
        density_rising = count_20m > count_prior_20m

        if (corr is not None and
                corr >= AUTO_ENTRY_CORR and
                total_msgs >= AUTO_ENTRY_MIN_DENS and
                density_rising and
                screener_price):
            current_price = screener_price
            corr_pct = round(corr * 100)
            auto_trades.insert_one({
                "ticker":        ticker,
                "status":        "active",
                "entry_price":   current_price,
                "entry_corr":    corr_pct,
                "entered_at":    now_utc,
                "peak_density":  total_msgs,
                "entry_density": total_msgs,
            })
            print(f"[AUTO TRADE] ENTER {ticker} @ {current_price} | corr: {corr_pct}%")


# Wraps _check_auto_trades in an infinite loop with error isolation so a single
# bad tick never crashes the thread and halts all auto trade monitoring.
def run_auto_trade_loop(stop_flag: list):
    print("[AUTO TRADE] Starting auto trade loop...")
    while not stop_flag[0]:
        try:
            _check_auto_trades()
        except Exception as e:
            print(f"[AUTO TRADE] Error: {e}")
        time.sleep(AUTO_LOOP_SEC)
    print("[AUTO TRADE] Stopped.")


def run_scorer_loop(stop_flag: list):
    """Runs the score calculator continuously in a background thread."""
    print("[SCORER THREAD] Starting continuous scoring loop...")
    while not stop_flag[0]:
        try:
            score_unscored_messages(batch_size=500)
            after = scored_messages.count_documents({"composite_score": None})
            # Only sleep when the queue is fully drained — keep hammering if backlog remains
            if after == 0:
                time.sleep(10)
        except Exception as e:
            print("[SCORER THREAD] Error: " + str(e))
            time.sleep(10)
    print("[SCORER THREAD] Stopped.")


# ── BACKGROUND SERVICES ──

# Runs once at startup (after a 15-second delay) to populate screener_snapshots
# from today's price_history so the 3D/bubble trail doesn't start empty after a restart.
def _backfill_screener_snapshots():
    """
    Seeds screener_snapshots at startup from price_history if available.
    """
    time.sleep(15)

    def _pf(v):
        try:
            return float(str(v).replace("%", "").replace(",", "").strip())
        except Exception:
            return None

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    ph_docs = list(price_history.find(
        {"ts": {"$gte": today_start}},
        {"ticker": 1, "price": 1, "ts": 1}
    ).sort("ts", 1))

    if not ph_docs:
        print("[BACKFILL] No price_history for today — skipping.")
        return

    first_price    = {}
    ticker_buckets = defaultdict(dict)

    for doc in ph_docs:
        tk    = doc.get("ticker", "")
        price = doc.get("price")
        ts    = doc.get("ts")
        if not tk or not price or not ts:
            continue
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except Exception:
                continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if tk not in first_price:
            first_price[tk] = price
        bucket = ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)
        ticker_buckets[tk][bucket] = price

    finviz   = get_finviz_data()
    inserted = 0
    for tk, buckets in ticker_buckets.items():
        fv      = finviz.get(tk, {})
        rel_vol = max(0.0, _pf(fv.get("rel_volume")) or 1.0)
        volume  = _pf(fv.get("volume")) or 0.0
        fp      = first_price.get(tk)
        if not fp:
            continue
        for bucket_ts, price in sorted(buckets.items()):
            price_chg = round((price - fp) / fp * 100, 2)
            try:
                res = screener_snapshots.update_one(
                    {"ticker": tk, "ts": bucket_ts},
                    {"$setOnInsert": {
                        "ticker": tk, "ts": bucket_ts,
                        "price_chg": price_chg, "rel_vol": rel_vol, "volume": volume,
                    }},
                    upsert=True
                )
                if res.upserted_id:
                    inserted += 1
            except Exception:
                pass

    print(f"[BACKFILL] price_history path complete — {inserted} new buckets inserted.")


def _start_background_services():
    """
    Initialises DB indexes, refreshes the Finviz token, and starts the
    scorer + calendar background threads.  Called once at process startup —
    works for both `python app.py` (dev) and gunicorn (production).
    """
    try:
        ensure_indexes()
    except Exception as e:
        print(f"[DB] Warning: could not create indexes at startup: {e}")

    if _FINVIZ_EMAIL and _FINVIZ_PASSWORD:
        refresh_finviz_token()
    else:
        print("[TOKEN] No FINVIZ_EMAIL/PASSWORD in .env — using static token")

    _scorer_stop = [False]
    threading.Thread(target=run_scorer_loop, args=(_scorer_stop,), daemon=True).start()
    print("[APP] Score calculator started automatically.")

    _auto_stop = [False]
    threading.Thread(target=run_auto_trade_loop, args=(_auto_stop,), daemon=True).start()
    print("[APP] Auto trade loop started.")

    threading.Thread(target=_backfill_screener_snapshots, daemon=True).start()
    print("[APP] Screener snapshot backfill scheduled.")

    def _start_pipeline():
        global pipeline_thread, pipeline_stop_flag
        pipeline_stop_flag = [False]
        def _run():
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from pipeline import run_pipeline
            run_pipeline(rolling_window=60, stop_flag=pipeline_stop_flag)
        pipeline_thread = threading.Thread(target=_run, daemon=True)
        pipeline_thread.start()
        print("[APP] Pipeline started automatically.")

    _start_pipeline()

    # Watchdog: polls every 30 s and restarts the pipeline if it died unexpectedly.
    # Checks pipeline_stop_flag so a manual /api/pipeline/stop is never auto-restarted.
    def _pipeline_watchdog():
        while True:
            time.sleep(30)
            # Only restart if the thread died unexpectedly — not if stop was requested
            stopped_intentionally = pipeline_stop_flag and pipeline_stop_flag[0]
            if not stopped_intentionally and (pipeline_thread is None or not pipeline_thread.is_alive()):
                print("[APP] Pipeline watchdog: thread died unexpectedly, restarting.")
                _start_pipeline()

    threading.Thread(target=_pipeline_watchdog, daemon=True).start()

    def _screener_tickers():
        with _finviz_lock:
            return list(finviz_cache.keys())

    start_calendar_threads(_screener_tickers)
    print("[APP] Background services started.")


# Start once at import time (covers gunicorn workers that never enter __main__)
_start_background_services()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print("[APP] Starting Stocktwits Sentiment Dashboard...")
    print(f"[APP] Open your browser at http://localhost:{port}")
    app.run(debug=False, host="0.0.0.0", port=port)