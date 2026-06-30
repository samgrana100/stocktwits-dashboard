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
import xml.etree.ElementTree as ET
from collections import defaultdict
from io import StringIO
from datetime import datetime, date, timezone, timedelta
import pandas as pd
from curl_cffi import requests as cffi_requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, request, render_template
from db import scored_messages, message_density, ensure_indexes, upcoming_catalysts
from score_calculator import aggregate_ticker_scores, score_unscored_messages
from utils import get_window_start_iso, get_timestamp
from event_detector import detect_event
from calendar_fetcher import start_calendar_threads
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, template_folder="../frontend")

FINVIZ_API_TOKEN  = os.getenv("FINVIZ_API_TOKEN", "")
FINVIZ_EXPORT_URL = os.getenv("FINVIZ_EXPORT_URL", "")
_FINVIZ_EMAIL    = os.getenv("FINVIZ_EMAIL", "")
_FINVIZ_PASSWORD = os.getenv("FINVIZ_PASSWORD", "")

_token_refresh_lock  = threading.Lock()
_last_token_refresh  = 0.0
_TOKEN_REFRESH_COOLDOWN = 300  # seconds between re-login attempts


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
                print(f"[TOKEN] Token is current ({new_token[:8]}…)")
                return True

            FINVIZ_API_TOKEN = new_token
            os.environ["FINVIZ_API_TOKEN"] = new_token
            print(f"[TOKEN] Refreshed token → {new_token[:8]}…")

            # Skip .env rewrite on Railway — filesystem is ephemeral there.
            # In-memory hot-swap above is sufficient for the current process.
            if not os.getenv("RAILWAY_ENVIRONMENT"):
                env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
                try:
                    with open(env_path, "r") as f:
                        env_lines = f.readlines()
                    with open(env_path, "w") as f:
                        for line in env_lines:
                            if line.startswith("FINVIZ_API_TOKEN="):
                                f.write(f"FINVIZ_API_TOKEN={new_token}\n")
                            else:
                                f.write(line)
                except Exception as e:
                    print(f"[TOKEN] Could not write .env: {e}")

            return True

        except Exception as e:
            print(f"[TOKEN] Refresh failed: {e}")
            return False

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

# Global wire-service RSS caches (PR Newswire, BusinessWire)
_prn_cache      = {"articles": [], "ts": 0.0}
_prn_lock       = threading.Lock()
_bw_cache       = {"articles": [], "ts": 0.0}
_bw_lock        = threading.Lock()
_WIRE_TTL       = 900   # 15 minutes

# Matches "(NYSE: TICK)", "(Nasdaq: TICK)", "(OTC: TICK)", or "$TICK"
_TICKER_RE = re.compile(
    r'(?:(?:NYSE(?:AMERICAN)?|NASDAQ|AMEX|OTCQX|OTCQB|OTC(?:[^:]{0,8})?|TSX)[:\s]+|\$)([A-Z]{1,6})\b',
    re.IGNORECASE,
)

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


def _get_wire_articles(url, source_name, cache, lock):
    """Fetches a global wire-service RSS feed, extracts ticker symbols from titles,
    caches for _WIRE_TTL seconds. Returns list of article dicts with a 'tickers' set."""
    now = time.time()
    with lock:
        if cache["ts"] > 0 and (now - cache["ts"]) < _WIRE_TTL:
            return cache["articles"]

    try:
        r = cffi_requests.get(url, impersonate="chrome120", timeout=15)
        if r.status_code != 200:
            print(f"[WIRE] {source_name}: HTTP {r.status_code}")
            return cache["articles"]
        root  = ET.fromstring(r.content)
        # Support both RSS (<item>) and Atom (<entry>) feed formats
        items = root.findall(".//item") or root.findall(".//entry")
        articles = []
        for item in items[:200]:
            title   = (item.findtext("title") or "").strip()
            # RSS: <link>url</link>  |  Atom: <link href="url"/>
            link = (item.findtext("link") or "").strip()
            if not link:
                link_el = item.find("link")
                if link_el is not None:
                    link = link_el.get("href", "")
            # RSS: <pubDate>  |  Atom: <updated> or <published>
            pub_raw = (
                item.findtext("pubDate") or
                item.findtext("updated") or
                item.findtext("published") or ""
            ).strip()
            tickers = set(m.upper() for m in _TICKER_RE.findall(title))
            if not tickers:
                continue
            pub_dt = _parse_news_date(pub_raw)
            articles.append({
                "title":   title,
                "url":     link,
                "source":  source_name,
                "date":    pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "date_dt": pub_dt,
                "tickers": tickers,
            })
        sample = [item.findtext("title") or "" for item in items[:3]]
        print(f"[WIRE] {source_name}: {len(items)} items in feed, {len(articles)} ticker-tagged | sample: {sample}")

        with lock:
            cache["articles"] = articles
            cache["ts"]       = now
        return articles
    except Exception as e:
        print(f"[WIRE] {source_name} fetch failed: {e}")
        return cache["articles"]


# Global Benzinga RSS cache (same pattern as PRN/BW)
_bzn_cache = {"articles": [], "ts": 0.0}
_bzn_lock  = threading.Lock()


def _get_benzinga_articles(ticker: str) -> list:
    """Filters the global Benzinga RSS cache for articles mentioning this ticker."""
    all_articles = _get_wire_articles(
        "https://www.benzinga.com/feed",
        "Benzinga", _bzn_cache, _bzn_lock,
    )
    return [a for a in all_articles if ticker in a["tickers"]]


def get_ticker_news(ticker: str) -> list:
    """
    Returns recent news for a ticker from four sources, each tagged with
    `source_category` for frontend filtering:
      pr_newswire  — PR Newswire global RSS, articles mentioning this ticker
      businesswire — BusinessWire global RSS, articles mentioning this ticker
      benzinga     — Benzinga per-ticker RSS feed
      fda          — Finviz Stocks News items with an FDA event tag
      sec          — SEC EDGAR 8-K filings (no date cutoff)

    PR Newswire / BusinessWire / Benzinga are limited to the last 14 days.
    Cached per ticker for 30 minutes.
    """
    now    = time.time()
    cached = _ticker_news_cache.get(ticker)
    if cached and (now - cached["ts"]) < _TICKER_NEWS_TTL:
        return cached["articles"]

    articles  = []
    cutoff_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=14)

    # ── 1. PR Newswire ────────────────────────────────────────────────────────
    for item in _get_wire_articles(
        "https://www.prnewswire.com/rss/financial-news-list.rss",
        "PR Newswire", _prn_cache, _prn_lock,
    ):
        if ticker not in item["tickers"]:
            continue
        if item["date_dt"] and item["date_dt"] < cutoff_dt:
            continue
        articles.append({
            "title": item["title"], "url": item["url"],
            "source": item["source"], "date": item["date"],
            "source_type": "news", "source_category": "pr_newswire",
        })

    # ── 2. BusinessWire ───────────────────────────────────────────────────────
    for item in _get_wire_articles(
        "https://feed.businesswire.com/rss/home/20061204005906/en/rss.xml",
        "Business Wire", _bw_cache, _bw_lock,
    ):
        if ticker not in item["tickers"]:
            continue
        if item["date_dt"] and item["date_dt"] < cutoff_dt:
            continue
        articles.append({
            "title": item["title"], "url": item["url"],
            "source": item["source"], "date": item["date"],
            "source_type": "news", "source_category": "businesswire",
        })

    # ── 3. Benzinga ───────────────────────────────────────────────────────────
    for item in _get_benzinga_articles(ticker):
        if item["date_dt"] and item["date_dt"] < cutoff_dt:
            continue
        articles.append({
            "title": item["title"], "url": item["url"],
            "source": item["source"], "date": item["date"],
            "source_type": "news", "source_category": "benzinga",
        })

    # ── 4. FDA catalysts (from Finviz Stocks News event tags) ────────────────
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

    # ── 5. SEC EDGAR 8-K filings (no date cutoff — filings are always relevant)
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


def _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label):
    if price_hist and finviz_param == "i1":
        sort_keys = sorted(set(p["sort_key"] for p in price_hist))
        return [sk + " " + tz_label for sk in sort_keys]
    axis = []
    step = 1 if window_min <= 60 else (5 if window_min <= 1440 else (60 if window_min <= 10080 else 1440))
    current = window_start_dt.replace(tzinfo=timezone.utc)
    while current <= now_utc:
        axis.append(utc_to_et(current.strftime("%Y-%m-%dT%H:%M:%SZ")))
        current += timedelta(minutes=step)
    axis.append(utc_to_et(now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")))
    return axis


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

    now_utc         = datetime.now(timezone.utc)
    window_start_dt = now_utc - timedelta(minutes=window_min)
    month           = now_utc.month
    tz_label        = "EDT" if 3 <= month <= 11 else "EST"
    now_ts          = time.time()

    # Serve from cache where available; only query MongoDB for the rest
    results   = {}
    uncached  = []
    for t in tickers:
        ck = f"{t}|{window_key}|{rolling_key}"
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
        {"$match": {"ticker": {"$in": uncached}, "created_at_utc": {"$gte": window_start}}},
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

    price_hist = get_finviz_price_history(ticker, finviz_param, window_min)
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
    """Returns Yahoo Finance news + SEC 8-K filings for one ticker (cached 30 min)."""
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