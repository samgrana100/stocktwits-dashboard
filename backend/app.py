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
import traceback
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
from db import scored_messages, ensure_indexes, upcoming_catalysts, auto_trades, completed_auto_trades, price_history, screener_snapshots, pipeline_events
from score_calculator import aggregate_ticker_scores, score_unscored_messages
import score_calculator  # module-qualified access for density-window config (see
                          # /api/density-window-config) — a plain `from X import NAME`
                          # binding wouldn't stay live-linked after set_*() mutates it
from utils import get_window_start_iso, get_timestamp, get_current_session_start_utc, get_current_session_name
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
# Exit fires when density falls AUTO_EXIT_PEAK % below its intra-trade peak, OR when
# price SMA falls AUTO_EXIT_SMA_PEAK % below its intra-trade peak (whichever fires first).
AUTO_ENTRY_CORR      = 0.30  # minimum correlation to enter
AUTO_ENTRY_MIN_DENS  = 4    # minimum msgs/hr (1h rolling) to enter
# Blocks entries on a ticker whose density already peaked earlier today and is
# now drifting down — a short-window density_rising blip can still fire during
# a longer decline, which is exactly the "entering on the way down" pattern.
# Tracked per ticker across the whole ET session (see _session_peak_density
# below), independent of trading hours, so a fresh move late in the day still
# enters fine while a stale one doesn't, regardless of the clock.
AUTO_ENTRY_MAX_OFF_PEAK = 0.35  # max % current density may sit below today's session peak to still enter
# Gates RE-entry on a ticker that already exited today: density must bounce this %
# above the lowest rolling density seen SINCE that exit (see _post_exit_density_trough
# below), not just tick up from whatever the immediately-prior minute happened to read.
# Replaces the old blanket "count_20m > count_prior_20m" check for these tickers, which
# fired on ordinary noise and kept re-entering during an overall afternoon decline.
AUTO_ENTRY_TROUGH_BOUNCE = 0.07  # min % bounce off post-exit trough density to re-enter
AUTO_EXIT_PEAK       = 0.20  # density % off peak to exit
AUTO_EXIT_MIN_DROP   = 6    # minimum absolute message drop before exit fires
AUTO_EXIT_SMA_PEAK   = 0.10  # price SMA % off peak to exit
# Master on/off switches for each exit mechanism — peak tracking (peak_density/
# peak_sma) keeps running either way so thresholds stay meaningful if re-enabled
# mid-trade; only the resulting exit decision is suppressed when disabled.
AUTO_EXIT_DENSITY_ENABLED = True
AUTO_EXIT_SMA_ENABLED     = True
# When on, NEW-ENTRY screening (density floor, density_rising/trough-bounce, and
# correlation) is bounded to "since the current session started" (4:00, 9:30, or
# 16:00 ET) instead of a fixed rolling window, so a stale premarket read can't
# carry into the regular session or vice versa. Deliberately does NOT touch
# already-open positions — their own peak_density/peak_sma exit tracking keeps
# running on the fixed rolling window they entered under, uninterrupted by a
# session boundary crossing mid-trade — and does not affect the correlation shown
# on the main dashboard/bubble map/popup charts, which always use the unscoped
# calculation regardless of this setting. See aggregate_ticker_scores's
# session_scoped param and _check_auto_trades' entry_window_* variables.
AUTO_SESSION_SCOPED_ENABLED = False
AUTO_LOOP_SEC      = 60     # seconds between auto trade checks
REENTRY_COOLDOWN   = 3600   # seconds before re-entering the same ticker — adjustable
                            # live via /api/auto-trade-config (reentry_cooldown_min)

# SMA window for the exit watch, in minutes. Not user-configurable (unlike the
# threshold above) — computed from price_history, which the main ingestion pipeline
# already populates for every tracked ticker every ~90s independent of auto-trading,
# so this adds zero Finviz calls regardless of how many trades are active.
SMA_WINDOW_MIN = 10

# Assumed $ position size per trade, used only to turn return_pct into a dollar
# pnl_dollars figure (no real position sizing exists yet). Snapshotted onto each
# trade doc at entry time so changing this later never rewrites historical PnL.
TRADE_POSITION_SIZE = 1000.0


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

# Auto-trade loop thread — tracked the same way as pipeline_thread so the shared
# watchdog below (see _pipeline_watchdog) can also detect and restart it if it
# ever dies unexpectedly. There's no manual stop endpoint for this one (unlike
# the pipeline's /api/pipeline/stop), so auto_trade_stop_flag[0] never gets set
# intentionally today — a dead thread always means "restart it."
auto_trade_thread    = None
auto_trade_stop_flag = [False]

# Set once at import time. /api/pipeline/status uses this for a boot grace period —
# without it, a brand-new process reports unhealthy on the very first healthcheck hit
# (last_seen_alive_at is still None right after boot, before the pipeline thread has
# had a chance to start), which combined with Railway's on_failure restart policy can
# turn into a self-inflicted crash loop: 503 -> Railway kills the container -> fresh
# boot -> 503 again before the pipeline is up -> repeat.
_app_boot_time = datetime.now(timezone.utc)

# Crash diagnostics — run_pipeline() is wrapped in _launch_pipeline_thread() below
# with `except BaseException` (not just Exception) so a SystemExit or any other
# non-Exception error still gets logged with a full traceback instead of being
# silently swallowed by Python's default per-thread exception hook.
# In-memory only — reset on every process restart/redeploy. See pipeline_events
# (MongoDB, below) for the durable version that survives those resets.
pipeline_health = {
    "last_start_at":       None,
    "last_seen_alive_at":  None,
    "last_crash_at":       None,
    "last_crash_type":     None,
    "last_crash_error":    None,
    "crash_count":         0,
    # Incremented once per watchdog loop iteration, regardless of outcome — proves
    # the watchdog thread itself is alive and looping, independent of last_seen_alive_at
    # (which /api/pipeline/status also updates on its own, so polling it can't be used
    # to tell "the watchdog is ticking" apart from "we just happened to check").
    "watchdog_ticks":      0,
    "last_watchdog_tick_at": None,
}

# Same shape as pipeline_health, for the auto-trade loop thread. Shares the
# pipeline's watchdog_ticks/last_watchdog_tick_at (one shared watchdog loop
# checks both threads each tick) rather than duplicating those two fields.
auto_trade_health = {
    "last_start_at":      None,
    "last_seen_alive_at": None,
    "last_crash_at":      None,
    "last_crash_type":    None,
    "last_crash_error":   None,
    "crash_count":        0,
}

# Session-long density peak per ticker, for AUTO_ENTRY_MAX_OFF_PEAK — tracked for
# every ticker seen in _check_auto_trades' entry loop each tick (not just ones
# with an open position, unlike peak_density on the trade doc itself, since this
# needs to exist for candidate tickers before any trade exists). Reset whenever
# the ET calendar date changes so yesterday's peak never blocks today's entries.
_session_peak_density = {}
_session_peak_date    = None

# Lowest rolling density seen since a ticker's most recent exit today, for
# AUTO_ENTRY_TROUGH_BOUNCE — seeded at exit-time density when a trade closes,
# then ratcheted down each entry-loop tick until the ticker re-enters. A ticker
# with no entry here today has never exited, so the trough-bounce gate simply
# doesn't apply to it (first entries keep using density_rising). Reset on ET
# calendar date change alongside _session_peak_density.
_post_exit_density_trough = {}

PIPELINE_DOWN_GRACE_SECONDS = 120  # how long /api/pipeline/status tolerates a stopped
                                   # pipeline before reporting unhealthy to Railway's
                                   # healthcheck — avoids flapping on a normal restart.


def _log_pipeline_event(event: str, detail: str = None):
    """Durable lifecycle log in MongoDB — survives process restarts and redeploys,
    unlike pipeline_health (in-memory) and Railway's own log stream (both get wiped
    by the exact events — a crash, a redeploy — we need to diagnose)."""
    try:
        pipeline_events.insert_one({
            "event":     event,
            "detail":    detail,
            "timestamp": get_timestamp(),
            "ts":        datetime.now(timezone.utc),
        })
    except Exception as e:
        print(f"[PIPELINE EVENTS] Failed to log '{event}': {e}")


def _launch_pipeline_thread():
    """
    Starts the pipeline in a background thread with crash logging.
    Shared by the auto-start-on-boot path and the /api/pipeline/start route so
    both get the same diagnostics — any crash updates pipeline_health and prints
    a full traceback, so the watchdog's restart is never a silent guess.
    """
    global pipeline_thread, pipeline_stop_flag
    pipeline_stop_flag = [False]
    stop_flag_ref = pipeline_stop_flag

    def _run():
        pipeline_health["last_start_at"]      = get_timestamp()
        pipeline_health["last_seen_alive_at"] = get_timestamp()
        _log_pipeline_event("started")
        try:
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from pipeline import run_pipeline
            run_pipeline(rolling_window=60, stop_flag=stop_flag_ref)
        except BaseException as e:
            pipeline_health["last_crash_at"]    = get_timestamp()
            pipeline_health["last_crash_type"]  = type(e).__name__
            pipeline_health["last_crash_error"] = str(e)
            pipeline_health["crash_count"]     += 1
            print(f"[PIPELINE] CRASHED ({type(e).__name__}): {e}")
            print(traceback.format_exc())
            _log_pipeline_event("crashed", f"{type(e).__name__}: {e}")

    pipeline_thread = threading.Thread(target=_run, daemon=True)
    pipeline_thread.start()


@app.route("/api/pipeline/restart", methods=["POST"])
def restart_pipeline():
    """Atomic stop+start, entirely server-side — replaces the old client-driven
    two-step flow (POST /stop, poll /status until stopped, POST /start) used by
    "Update Filters". That flow had a real failure mode: if the browser tab
    closed, lost network, or was navigated away between the stop call and the
    start call, the pipeline was left stopped with nothing to bring it back —
    the watchdog deliberately never auto-restarts an intentional stop. Doing
    both halves in one request means a dropped client connection can't strand
    it mid-restart; the request either lands with the pipeline back up, or it
    never got the stop signal to begin with."""
    global pipeline_stop_flag
    if pipeline_thread and pipeline_thread.is_alive():
        pipeline_stop_flag[0] = True
        _log_pipeline_event("restart_requested")
        pipeline_thread.join(timeout=10)
    _launch_pipeline_thread()
    return jsonify({"status": "restarted"})

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

# Session-anchored chart windows — used only by the Trades tab's popup for a trade
# taken under AUTO_SESSION_SCOPED_ENABLED, so the chart shows just the session that
# trade's entry decision was actually evaluated in, instead of the whole day. Each
# is an absolute ET clock-time range (today's date), not a rolling duration back
# from now — see get_ticker_detail's session_key handling.
SESSION_WINDOW_BOUNDS = {
    "premarket": ((4, 0),  (9, 30)),
    "market":    ((9, 30), (16, 0)),
    "afterhours": ((16, 0), (20, 0)),
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


def _utc_dt_to_et_hhmmss(dt: datetime) -> str:
    """Same as _utc_dt_to_et_hhmm but with seconds — used for trade entered_at_et/
    exited_at_et so the recorded time is traceable to the exact live price fetch,
    not just the minute. Range-filter queries against it (time_from/time_to, both
    "HH:MM") still work: "HH:MM" is a strict string prefix of "HH:MM:SS", so
    lexicographic $gte/$lte comparisons order correctly across the two lengths."""
    offset = timedelta(hours=-4) if 3 <= dt.month <= 11 else timedelta(hours=-5)
    return (dt + offset).strftime("%H:%M:%S")


def _utc_dt_to_et_date(dt: datetime) -> str:
    """Convert a UTC datetime to its Eastern Time calendar date, YYYY-MM-DD —
    used to bucket completed trades by day for the Trades tab's Calendar view."""
    offset = timedelta(hours=-4) if 3 <= dt.month <= 11 else timedelta(hours=-5)
    return (dt + offset).strftime("%Y-%m-%d")


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


# Bucket labels/thresholds mirror _FINVIZ_CAP_MAP's own nano/micro/small/mid/large/mega
# convention, so a trade's captured market cap reads consistently with the rest of the app.
_CAP_BUCKET_THRESHOLDS = [
    (50e6,   "Nano"),
    (300e6,  "Micro"),
    (2e9,    "Small"),
    (10e9,   "Mid"),
    (200e9,  "Large"),
]

def _current_ticker_price(ticker: str, finviz: dict, live: bool = False):
    """Price lookup used for trade entry/exit prices.

    live=True (used at the exact moment a trade enters/exits) makes a direct,
    uncached Finviz quote_export call so the recorded fill price is accurate to
    that instant, not up to PRICE_CACHE_TTL+jitter seconds stale. Falls back to
    the cached bar / screener price if the live call comes back empty.

    live=False prefers the cached intraday quote, falling back to the Finviz
    screener price — used where an approximate price is fine (e.g. gating
    conditions before a trade decision is actually made)."""
    if live:
        bars = get_finviz_price_history(ticker, "i1", multi_day=False, force_refresh=True)
        if bars:
            return bars[-1]["price"]
    with _price_lock:
        cached = price_cache.get(f"{ticker}_i1_today")
        bars   = cached["data"] if cached else []
    return bars[-1]["price"] if bars else finviz.get(ticker, {}).get("price")


def _current_sma(ticker: str, window_min: int = SMA_WINDOW_MIN):
    """Simple moving average of price_history snapshots for `ticker` over the
    trailing `window_min` minutes. Reads data the main ingestion pipeline already
    collects for every tracked ticker every ~90s (pipeline.py's run_pipeline) rather
    than calling Finviz directly — this is a MongoDB query, so it adds zero API load
    no matter how many trades are being monitored. Returns None if no snapshots
    fall in the window yet (e.g. a ticker that just entered the screener)."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=window_min)
    docs = price_history.find({"ticker": ticker, "ts": {"$gte": cutoff}}, {"price": 1})
    prices = [d["price"] for d in docs if d.get("price") is not None]
    if not prices:
        return None
    return sum(prices) / len(prices)


def _market_cap_bucket(raw) -> str | None:
    """Parses a Finviz "Market Cap" export value into a bucket label
    (Nano/Micro/Small/Mid/Large/Mega). Returns None if unparseable.

    Finviz's CSV export (get_finviz_data, used here) reports Market Cap as a
    plain number already in millions of dollars, e.g. "4.45" == $4.45M — NOT
    the suffixed "1.23B"/"450M" format Finviz's website displays. A suffix is
    still handled defensively in case that ever changes."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip().upper()
    if not s or s == "--":
        return None
    suffix_mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
    mult = suffix_mult.get(s[-1])
    try:
        value = float(s[:-1]) * mult if mult else float(s) * 1e6
    except ValueError:
        return None
    for threshold, label in _CAP_BUCKET_THRESHOLDS:
        if value < threshold:
            return label
    return "Mega"


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

def get_finviz_price_history(ticker: str, finviz_param: str, multi_day: bool = False, force_refresh: bool = False) -> list:
    """
    Fetches price bars from Finviz quote_export (p=i1, 1-min bars).

    multi_day=False (sub-day charts): restricts to the most recent calendar day so
        cross-day HH:MM key collisions can't occur when needsDate=False in the frontend.
    multi_day=True  (1d+ charts):     returns all bars from the CSV; needsDate=True
        in the frontend keys every bar by "MM-DD HH:MM", so multi-day data is safe.

    force_refresh=True skips the cache-hit check and always hits Finviz directly —
    used to capture an accurate trade entry/exit price rather than a bar that may be
    up to PRICE_CACHE_TTL+jitter seconds stale. The fresh result still gets written
    into price_cache below, so normal (non-live) reads benefit from it too.

    CSV format: "MM/DD/YYYY HH:MM AM/PM"  e.g. "05/21/2026 04:00 AM"
    """
    cache_key = f"{ticker}_{finviz_param}_{'multi' if multi_day else 'today'}"
    now_ts    = time.time()
    if not force_refresh:
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
def _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label, axis_end_dt=None):
    # Always generate a regular time axis covering the full window so that
    # score/density data is never cut off by sparse price bar timestamps.
    # Axis is snapped to clean step boundaries so frontend snapKey lookups match.
    # axis_end_dt lets a session-anchored window (see SESSION_WINDOW_BOUNDS) stop the
    # axis at that session's own end instead of always extending out to right now.
    axis = []
    step = 1 if window_min <= 1440 else (60 if window_min <= 10080 else 1440)
    axis_end = axis_end_dt if axis_end_dt is not None else now_utc

    # Snap start to the nearest step boundary (UTC minutes since midnight).
    # UTC-ET offset is always whole hours, so snapping in UTC = snapping in ET.
    raw = window_start_dt.replace(tzinfo=timezone.utc, second=0, microsecond=0)
    total_min = raw.hour * 60 + raw.minute
    snapped   = (total_min // step) * step
    current   = raw.replace(hour=snapped // 60, minute=snapped % 60)

    while current <= axis_end:
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

    now_utc = datetime.now(timezone.utc)

    if window_key in SESSION_WINDOW_BOUNDS:
        # Session-anchored: an absolute ET clock-time range for TODAY, not a rolling
        # duration back from now — used by the Trades tab popup so a trade taken under
        # AUTO_SESSION_SCOPED_ENABLED shows just its own session instead of the whole day.
        (sh, sm), (eh, em) = SESSION_WINDOW_BOUNDS[window_key]
        et_offset   = timedelta(hours=-4) if 3 <= now_utc.month <= 11 else timedelta(hours=-5)
        now_et      = now_utc + et_offset
        session_start_et = now_et.replace(hour=sh, minute=sm, second=0, microsecond=0)
        session_end_et   = now_et.replace(hour=eh, minute=em, second=0, microsecond=0)
        window_start_dt  = session_start_et - et_offset
        axis_end_dt       = min(session_end_et - et_offset, now_utc)
        window_min    = max(1, round((axis_end_dt - window_start_dt).total_seconds() / 60))
        finviz_param  = "i1"
        window_start  = window_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        config        = WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"])
        window_min    = config["minutes"]
        finviz_param  = config["finviz_param"]
        window_start  = get_window_start_iso(window_min)
        window_start_dt = now_utc - timedelta(minutes=window_min)
        axis_end_dt      = now_utc

    rolling_config = WINDOW_CONFIG.get(rolling_key, WINDOW_CONFIG.get(window_key, WINDOW_CONFIG["1h"]))
    rolling_min    = rolling_config["minutes"]
    rolling_start  = get_window_start_iso(rolling_min)

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
    # Session-anchored windows deliberately skip that priming — messages from before
    # the session started shouldn't feed the rolling density line, so it visibly ramps
    # up from the session open instead of already being elevated at the left edge.
    # This mirrors what AUTO_SESSION_SCOPED_ENABLED's entry_window_1h actually sees,
    # so the chart matches what the bot itself is evaluating.
    if window_key in SESSION_WINDOW_BOUNDS:
        density_start = max(density_start, window_start)

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
    time_axis = _build_time_axis(price_hist, finviz_param, window_min, window_start_dt, now_utc, tz_label, axis_end_dt=axis_end_dt)

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
        "peak_sma":      t.get("peak_sma"),
    } for t in auto_trades.find({"status": "active", "source": "auto"}, {"_id": 0})]

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
            "peak_sma":     t.get("peak_sma"),
            "exit_sma":     t.get("exit_sma"),
            "exit_reason":  t.get("exit_reason"),
            "saved":        t.get("saved", False),
        }

    recent_50   = list(completed_auto_trades.find({"source": "auto"}, {"_id": 0}).sort("exited_at", -1).limit(50))
    recent_keys = {(t["ticker"], str(t.get("exited_at"))): True for t in recent_50}
    saved_extra = [
        t for t in completed_auto_trades.find({"saved": True, "source": "auto"}, {"_id": 0})
        if (t["ticker"], str(t.get("exited_at"))) not in recent_keys
    ]
    completed = [_fmt_completed(t) for t in recent_50 + saved_extra]

    return jsonify({
        "active": active, "completed": completed,
        "config": _auto_trade_config_dict()
    })


def _auto_trade_config_dict() -> dict:
    """Single source of truth for the auto-trade config payload — used by both
    GET /api/auto-trades and the response of POST /api/auto-trade-config, so the
    two can never drift out of sync with each other."""
    return {
        "entry_corr":            AUTO_ENTRY_CORR,
        "entry_min_dens":        AUTO_ENTRY_MIN_DENS,
        "entry_max_off_peak":    AUTO_ENTRY_MAX_OFF_PEAK,
        "entry_trough_bounce":   AUTO_ENTRY_TROUGH_BOUNCE,
        "exit_peak":             AUTO_EXIT_PEAK,
        "exit_min_drop":         AUTO_EXIT_MIN_DROP,
        "exit_sma_peak":         AUTO_EXIT_SMA_PEAK,
        "exit_density_enabled":  AUTO_EXIT_DENSITY_ENABLED,
        "exit_sma_enabled":      AUTO_EXIT_SMA_ENABLED,
        "session_scoped_enabled": AUTO_SESSION_SCOPED_ENABLED,
        "position_size":         TRADE_POSITION_SIZE,
        "reentry_cooldown_min":  round(REENTRY_COOLDOWN / 60, 1),
    }


# Allows the frontend to adjust entry/exit thresholds without restarting the server.
@app.route("/api/auto-trade-config", methods=["POST"])
def set_auto_trade_config():
    global AUTO_ENTRY_CORR, AUTO_ENTRY_MIN_DENS, AUTO_ENTRY_MAX_OFF_PEAK, AUTO_ENTRY_TROUGH_BOUNCE, AUTO_EXIT_PEAK, AUTO_EXIT_MIN_DROP, AUTO_EXIT_SMA_PEAK, \
        AUTO_EXIT_DENSITY_ENABLED, AUTO_EXIT_SMA_ENABLED, AUTO_SESSION_SCOPED_ENABLED, TRADE_POSITION_SIZE, REENTRY_COOLDOWN
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
        if "entry_max_off_peak" in data:
            val = float(data["entry_max_off_peak"])
            if 0.0 < val <= 1.0:
                AUTO_ENTRY_MAX_OFF_PEAK = round(val, 2)
        if "entry_trough_bounce" in data:
            val = float(data["entry_trough_bounce"])
            if 0.0 < val <= 1.0:
                AUTO_ENTRY_TROUGH_BOUNCE = round(val, 2)
        if "exit_peak" in data:
            val = float(data["exit_peak"])
            if 0.0 < val <= 1.0:
                AUTO_EXIT_PEAK = round(val, 2)
        if "exit_min_drop" in data:
            val = int(data["exit_min_drop"])
            if val >= 0:
                AUTO_EXIT_MIN_DROP = val
        if "exit_sma_peak" in data:
            val = float(data["exit_sma_peak"])
            if 0.0 < val <= 1.0:
                AUTO_EXIT_SMA_PEAK = round(val, 2)
        if "exit_density_enabled" in data:
            AUTO_EXIT_DENSITY_ENABLED = bool(data["exit_density_enabled"])
        if "exit_sma_enabled" in data:
            AUTO_EXIT_SMA_ENABLED = bool(data["exit_sma_enabled"])
        if "session_scoped_enabled" in data:
            AUTO_SESSION_SCOPED_ENABLED = bool(data["session_scoped_enabled"])
        if "position_size" in data:
            val = float(data["position_size"])
            if val > 0:
                TRADE_POSITION_SIZE = round(val, 2)
        if "reentry_cooldown_min" in data:
            val = float(data["reentry_cooldown_min"])
            if 0 < val <= 1440:  # cap at 24h — guards against a fat-fingered value locking a ticker out for days
                REENTRY_COOLDOWN = round(val * 60)
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid config value: {e}"}), 400
    cfg = _auto_trade_config_dict()
    print(f"[AUTO TRADE CONFIG] {cfg}")
    return jsonify(cfg)


# Msg Density Rolling window used everywhere density is computed — the main dashboard's
# display AND the correlation calc (score_calculator._compute_correlation) share this one
# setting, so what you see is genuinely what the auto-trader is using. A ticker with no
# explicit override uses the default; the main table's "Set Default" control only ever
# touches the default, never an individual ticker's own choice.
def _density_window_config_dict() -> dict:
    return {
        "default_minutes": score_calculator.DEFAULT_DENSITY_WINDOW_MIN,
        "overrides":       dict(score_calculator._ticker_density_window_min),
    }


@app.route("/api/density-window-config")
def get_density_window_config():
    return jsonify(_density_window_config_dict())


@app.route("/api/density-window-config", methods=["POST"])
def set_density_window_config():
    data = request.get_json(force=True) or {}
    try:
        if "default_minutes" in data:
            val = int(data["default_minutes"])
            if val > 0:
                score_calculator.set_default_density_window_min(val)
        if "ticker" in data:
            ticker = (data["ticker"] or "").upper().strip()
            if ticker:
                minutes = data.get("minutes")
                score_calculator.set_ticker_density_window_min(
                    ticker, int(minutes) if minutes is not None else None
                )
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid value: {e}"}), 400
    return jsonify(_density_window_config_dict())


# NOTE: scoped to source="auto" — auto_trades/completed_auto_trades also hold manual
# trades (see MANUAL TRADE ROUTES below) which must survive these clears untouched.
@app.route("/api/auto-trades/clear", methods=["POST"])
def clear_auto_trades():
    auto_trades.delete_many({"source": "auto"})
    completed_auto_trades.delete_many({"source": "auto"})
    return jsonify({"cleared": True})


@app.route("/api/auto-trades/clear-active", methods=["POST"])
def clear_active_auto_trades():
    auto_trades.delete_many({"source": "auto", "status": "active"})
    return jsonify({"cleared": True})


@app.route("/api/auto-trades/clear-history", methods=["POST"])
def clear_auto_trade_history():
    completed_auto_trades.delete_many({"source": "auto"})
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


# ── MANUAL TRADE ROUTES ──
# Server-side counterpart to the Correlation tab's Entry Zone "Enter Trade"/"Exit Trade"
# buttons. Shares the auto_trades/completed_auto_trades collections with the automated
# engine (source="manual") so both show up in one filterable /api/trades query, but price
# is always re-derived server-side — never trusted from the client — for data integrity.

def _fmt_trade_doc(t: dict) -> dict:
    def fmt(dt):
        if not dt: return None
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(dt, "strftime") else str(dt)
    out = dict(t)
    out.pop("_id", None)
    out["entered_at"] = fmt(out.get("entered_at"))
    out["exited_at"]  = fmt(out.get("exited_at"))
    return out


@app.route("/api/manual-trades/enter", methods=["POST"])
def enter_manual_trade():
    data   = request.get_json(force=True) or {}
    ticker = (data.get("ticker") or "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    if auto_trades.find_one({"ticker": ticker, "status": "active", "source": "manual"}):
        return jsonify({"error": f"{ticker} already has an open manual trade"}), 400

    try:
        entry_corr    = round(float(data.get("entry_corr"))) if data.get("entry_corr") is not None else None
        peak_density  = int(data.get("peak_density") or 0)
    except (ValueError, TypeError):
        return jsonify({"error": "invalid entry_corr/peak_density"}), 400

    finviz      = get_finviz_data()
    entry_price = _current_ticker_price(ticker, finviz, live=True)
    if not entry_price:
        return jsonify({"error": f"no current price available for {ticker}"}), 400

    now_utc  = datetime.now(timezone.utc)
    fv       = finviz.get(ticker, {})
    doc = {
        "ticker":            ticker,
        "status":            "active",
        "entry_price":       entry_price,
        "entry_corr":        entry_corr,
        "entered_at":        now_utc,
        "entered_at_et":     _utc_dt_to_et_hhmmss(now_utc),
        "peak_density":      peak_density,
        "entry_density":     peak_density,
        "source":            "manual",
        "market_cap":        fv.get("market_cap", "--"),
        "market_cap_bucket": _market_cap_bucket(fv.get("market_cap")),
        "position_size":     TRADE_POSITION_SIZE,
    }
    auto_trades.insert_one(doc)
    return jsonify(_fmt_trade_doc(doc))


@app.route("/api/manual-trades/exit", methods=["POST"])
def exit_manual_trade():
    data   = request.get_json(force=True) or {}
    ticker = (data.get("ticker") or "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    trade = auto_trades.find_one({"ticker": ticker, "status": "active", "source": "manual"})
    if not trade:
        return jsonify({"error": f"no open manual trade for {ticker}"}), 404

    finviz     = get_finviz_data()
    exit_price = _current_ticker_price(ticker, finviz, live=True)
    entry_price = trade.get("entry_price")
    return_pct  = None
    try:
        if entry_price and exit_price:
            return_pct = round((float(exit_price) - float(entry_price)) / float(entry_price) * 100, 2)
    except Exception:
        pass

    # Same 1hr rolling density source the auto engine uses for exit_density, so
    # manual and auto completed trades are directly comparable/filterable together.
    exit_density = scored_messages.count_documents({
        "ticker":         ticker,
        "created_at_utc": {"$gte": get_window_start_iso(60)}
    })

    # Client-supplied, same convention as entry_corr — the frontend already has
    # this ticker's live correlation from its own polling at the moment of exit,
    # so there's no need for a second server-side aggregate_ticker_scores() pass.
    exit_corr = None
    try:
        if data.get("exit_corr") is not None:
            exit_corr = round(float(data["exit_corr"]))
    except (ValueError, TypeError):
        pass

    now_utc       = datetime.now(timezone.utc)
    position_size = trade.get("position_size", TRADE_POSITION_SIZE)
    pnl_dollars   = round(return_pct * position_size / 100, 2) if return_pct is not None else None

    doc = {
        "ticker":            ticker,
        "entry_price":       entry_price,
        "exit_price":        exit_price,
        "return_pct":        return_pct,
        "entered_at":        trade.get("entered_at"),
        "exited_at":         now_utc,
        "entered_at_et":     trade.get("entered_at_et"),
        "exited_at_et":      _utc_dt_to_et_hhmmss(now_utc),
        "entry_corr":        trade.get("entry_corr"),
        "exit_corr":         exit_corr,
        "entry_density":     trade.get("entry_density", 0),
        "peak_density":      trade.get("peak_density", 0),
        "exit_density":      exit_density,
        "exit_reason":       "manual",
        "source":            "manual",
        "market_cap":        trade.get("market_cap"),
        "market_cap_bucket": trade.get("market_cap_bucket"),
        "position_size":     position_size,
        "pnl_dollars":       pnl_dollars,
    }
    completed_auto_trades.insert_one(doc)
    auto_trades.delete_one({"ticker": ticker, "status": "active", "source": "manual"})
    return jsonify(_fmt_trade_doc(doc))


@app.route("/api/manual-trades/remove", methods=["POST"])
def remove_manual_trade():
    """Cancels an open manual position without recording a completed trade — the
    server-side equivalent of the Correlation tab's silent-discard 'Remove' button."""
    data   = request.get_json(force=True) or {}
    ticker = (data.get("ticker") or "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    result = auto_trades.delete_one({"ticker": ticker, "status": "active", "source": "manual"})
    return jsonify({"ticker": ticker, "removed": result.deleted_count > 0})


def _et_date_str_to_utc(date_str: str, end_of_day: bool = False):
    """Converts a "YYYY-MM-DD" ET calendar date into a UTC datetime boundary,
    inverting the UTC->ET offset used by _utc_dt_to_et_hhmm/get_et_now."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None
    offset = timedelta(hours=-4) if 3 <= d.month <= 11 else timedelta(hours=-5)
    utc_dt = (d - offset).replace(tzinfo=timezone.utc)
    return utc_dt + timedelta(days=1) if end_of_day else utc_dt


# Combined view backing the Trades tab: auto + manual trades, filterable by source,
# status, ticker, ET time-of-day range, market cap bucket, and ET calendar date range.
@app.route("/api/trades")
def get_trades_endpoint():
    source = (request.args.get("source") or "all").lower()
    status = (request.args.get("status") or "all").lower()
    ticker = (request.args.get("ticker") or "").upper().strip()
    cap    = (request.args.get("cap") or "all")
    time_from = request.args.get("time_from") or None
    time_to   = request.args.get("time_to") or None
    date_from = request.args.get("date_from") or None
    date_to   = request.args.get("date_to") or None
    entry_density_min = request.args.get("entry_density_min", type=int)
    exit_density_min  = request.args.get("exit_density_min", type=int)
    entry_corr_min    = request.args.get("entry_corr_min", type=int)
    limit  = min(request.args.get("limit", 200, type=int) or 200, 500)

    def base_filter():
        f = {}
        if source in ("auto", "manual"):
            f["source"] = source
        if ticker:
            f["ticker"] = ticker
        if cap and cap.lower() != "all":
            f["market_cap_bucket"] = cap
        if entry_density_min is not None:
            f["entry_density"] = {"$gte": entry_density_min}
        if entry_corr_min is not None:
            f["entry_corr"] = {"$gte": entry_corr_min}
        return f

    active_docs, completed_docs = [], []

    if status in ("all", "active"):
        f = base_filter()
        f["status"] = "active"
        if time_from or time_to:
            rng = {}
            if time_from: rng["$gte"] = time_from
            if time_to:   rng["$lte"] = time_to
            f["entered_at_et"] = rng
        if date_from or date_to:
            rng = {}
            start = _et_date_str_to_utc(date_from) if date_from else None
            end   = _et_date_str_to_utc(date_to, end_of_day=True) if date_to else None
            if start: rng["$gte"] = start
            if end:   rng["$lt"]  = end
            if rng: f["entered_at"] = rng
        # exit_density doesn't exist on active docs — filtering by it implicitly
        # asks for closed trades, so skip the active side entirely in that case.
        if exit_density_min is None:
            active_docs = list(auto_trades.find(f).sort("entered_at", -1).limit(limit))

    if status in ("all", "completed"):
        f = base_filter()
        if exit_density_min is not None:
            f["exit_density"] = {"$gte": exit_density_min}
        if time_from or time_to:
            # entered_at_et, not exited_at_et — matches the active-trade branch above
            # and the adjacent Entry Corr/Entry Density filters, so "9:30-10:00" means
            # "trades taken in this window" regardless of when they closed.
            rng = {}
            if time_from: rng["$gte"] = time_from
            if time_to:   rng["$lte"] = time_to
            f["entered_at_et"] = rng
        if date_from or date_to:
            rng = {}
            start = _et_date_str_to_utc(date_from) if date_from else None
            end   = _et_date_str_to_utc(date_to, end_of_day=True) if date_to else None
            if start: rng["$gte"] = start
            if end:   rng["$lt"]  = end
            if rng: f["exited_at"] = rng
        completed_docs = list(completed_auto_trades.find(f).sort("exited_at", -1).limit(limit))

    # Totals over the filtered completed set — return_pct/pnl_dollars only exist
    # once a trade is closed, so open positions don't factor into these.
    pnl_vals = [t["pnl_dollars"] for t in completed_docs if t.get("pnl_dollars") is not None]
    ret_vals = [t["return_pct"]  for t in completed_docs if t.get("return_pct")  is not None]
    wins     = sum(1 for v in pnl_vals if v > 0)
    losses   = sum(1 for v in pnl_vals if v < 0)
    totals = {
        "pnl_dollars":    round(sum(pnl_vals), 2) if pnl_vals else 0.0,
        "avg_return_pct": round(sum(ret_vals) / len(ret_vals), 2) if ret_vals else None,
        "trade_count":    len(completed_docs),
        "wins":           wins,
        "losses":         losses,
    }

    return jsonify({
        "active":    [_fmt_trade_doc(t) for t in active_docs],
        "completed": [_fmt_trade_doc(t) for t in completed_docs],
        "counts": {"active": len(active_docs), "completed": len(completed_docs)},
        "totals": totals,
    })


# Realized PnL for AUTO trades closed during today's ET calendar day — backs the
# dashboard's "Day's PnL" tile. Manual trades are intentionally excluded (only the
# automated engine runs unattended 24/7, so it's the only one with a meaningful
# "today" headline figure).
@app.route("/api/trades/pnl-today")
def get_trades_pnl_today():
    today_et  = get_et_now().strftime("%Y-%m-%d")
    start_utc = _et_date_str_to_utc(today_et)
    end_utc   = _et_date_str_to_utc(today_et, end_of_day=True)

    docs = list(completed_auto_trades.find({
        "source": "auto",
        "exited_at": {"$gte": start_utc, "$lt": end_utc},
    }))

    pnl_dollars = 0.0
    wins = losses = 0
    for t in docs:
        pnl = t.get("pnl_dollars")
        if pnl is None:
            rp = t.get("return_pct")
            pnl = round(rp * t.get("position_size", TRADE_POSITION_SIZE) / 100, 2) if rp is not None else 0.0
        pnl_dollars += pnl
        if pnl > 0: wins += 1
        elif pnl < 0: losses += 1

    return jsonify({
        "pnl_dollars":  round(pnl_dollars, 2),
        "trade_count":  len(docs),
        "wins":         wins,
        "losses":       losses,
        "position_size": TRADE_POSITION_SIZE,
    })


# Per-day PnL/trade-count rollup for one ET calendar month — backs the Trades tab's
# Calendar view. Groups completed_auto_trades by the ET date of exited_at so a whole
# month renders from a single query rather than one request per day.
@app.route("/api/trades/calendar")
def get_trades_calendar():
    month_str = request.args.get("month") or get_et_now().strftime("%Y-%m")
    source    = (request.args.get("source") or "all").lower()
    try:
        year, month = (int(x) for x in month_str.split("-"))
        if not (1 <= month <= 12):
            raise ValueError
    except (ValueError, AttributeError):
        return jsonify({"error": "invalid month, expected YYYY-MM"}), 400

    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    month_start = _et_date_str_to_utc(f"{year:04d}-{month:02d}-01")
    month_end   = _et_date_str_to_utc(f"{next_year:04d}-{next_month:02d}-01")

    f = {"exited_at": {"$gte": month_start, "$lt": month_end}}
    if source in ("auto", "manual"):
        f["source"] = source

    days = {}
    for t in completed_auto_trades.find(f, {"exited_at": 1, "pnl_dollars": 1}):
        exited = t.get("exited_at")
        if not exited:
            continue
        day = days.setdefault(_utc_dt_to_et_date(exited), {"pnl_dollars": 0.0, "trade_count": 0, "wins": 0, "losses": 0})
        pnl = t.get("pnl_dollars")
        day["trade_count"] += 1
        if pnl is not None:
            day["pnl_dollars"] += pnl
            if pnl > 0: day["wins"] += 1
            elif pnl < 0: day["losses"] += 1

    for d in days.values():
        d["pnl_dollars"] = round(d["pnl_dollars"], 2)

    return jsonify({"month": f"{year:04d}-{month:02d}", "days": days})


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
    if pipeline_thread and pipeline_thread.is_alive():
        return jsonify({"status": "already_running"})
    _launch_pipeline_thread()
    return jsonify({"status": "started"})


@app.route("/api/pipeline/stop", methods=["POST"])
def stop_pipeline():
    global pipeline_stop_flag
    pipeline_stop_flag[0] = True
    _log_pipeline_event("stopped_manually")
    return jsonify({"status": "stopped"})


@app.route("/api/pipeline/status")
def pipeline_status():
    running = pipeline_thread is not None and pipeline_thread.is_alive()
    if running:
        pipeline_health["last_seen_alive_at"] = get_timestamp()
    auto_trade_running = auto_trade_thread is not None and auto_trade_thread.is_alive()
    if auto_trade_running:
        auto_trade_health["last_seen_alive_at"] = get_timestamp()
    # auto_trade health is surfaced here for visibility only — it does NOT affect
    # this endpoint's HTTP status below. That 503 path is what Railway's restart
    # policy acts on; folding a second, independently-healing subsystem into it
    # would need its own carefully-tuned grace period or risk exactly the kind of
    # self-inflicted crash loop already fixed for the pipeline itself. The
    # watchdog already restarts this thread on its own — this is just so a human
    # (or a future alert) can see it happened.
    payload = {
        "running": running,
        **pipeline_health,
        "auto_trade": {"running": auto_trade_running, **auto_trade_health},
    }

    # Report unhealthy to Railway's healthcheck only once the pipeline has been down
    # for longer than PIPELINE_DOWN_GRACE_SECONDS, and only when it wasn't stopped on
    # purpose — gives Railway's own restart policy a shot at recovering it if our
    # in-process watchdog somehow doesn't, without flapping on a normal quick restart.
    stopped_intentionally = pipeline_stop_flag and pipeline_stop_flag[0]
    if not running and not stopped_intentionally:
        since_boot = (datetime.now(timezone.utc) - _app_boot_time).total_seconds()
        if since_boot < PIPELINE_DOWN_GRACE_SECONDS:
            # Process itself just booted — the pipeline thread hasn't had a real
            # chance to start yet. Never report unhealthy this early, or a fresh
            # container can fail its own first healthcheck before it's had time
            # to come up, which is exactly the self-inflicted loop described above.
            return jsonify(payload)
        last_alive = pipeline_health.get("last_seen_alive_at")
        down_too_long = True
        if last_alive:
            try:
                last_alive_dt = datetime.strptime(last_alive, "%d:%m:%Y:%H:%M:%S").replace(tzinfo=timezone.utc)
                down_too_long = (datetime.now(timezone.utc) - last_alive_dt) > timedelta(seconds=PIPELINE_DOWN_GRACE_SECONDS)
            except Exception:
                pass
        if down_too_long:
            return jsonify(payload), 503
    return jsonify(payload)


@app.route("/api/debug/pipeline-events")
def debug_pipeline_events():
    """TEMP diagnostic — recent pipeline lifecycle events (started/crashed/watchdog_restart/stopped_manually/restart_requested)."""
    docs = list(pipeline_events.find({}, {"_id": 0}).sort("ts", -1).limit(50))
    for d in docs:
        if isinstance(d.get("ts"), datetime):
            d["ts"] = d["ts"].isoformat()
    return jsonify({"count": len(docs), "events": docs})


@app.route("/api/debug/auto-trade/<ticker>")
def debug_auto_trade(ticker):
    """TEMP diagnostic — raw active + completed auto-trade docs for one ticker, full fields."""
    ticker = ticker.upper()
    def _fmt(d):
        d.pop("_id", None)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d
    active    = [_fmt(t) for t in auto_trades.find({"ticker": ticker})]
    completed = [_fmt(t) for t in completed_auto_trades.find({"ticker": ticker}).sort("exited_at", -1)]
    return jsonify({"ticker": ticker, "active": active, "completed": completed})


@app.route("/api/debug/price-history/<ticker>")
def debug_price_history(ticker):
    """TEMP diagnostic — raw price_history docs for one ticker, newest first. Remove after debugging correlation staleness."""
    ticker = ticker.upper()
    docs = list(price_history.find(
        {"ticker": ticker},
        {"_id": 0, "ticker": 1, "price": 1, "minute_bucket": 1, "timestamp": 1, "ts": 1, "source": 1}
    ).sort("ts", -1).limit(40))
    for d in docs:
        if isinstance(d.get("ts"), datetime):
            d["ts"] = d["ts"].isoformat()
    return jsonify({"ticker": ticker, "count": len(docs), "docs": docs})


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
    global _session_peak_density, _session_peak_date, _post_exit_density_trough
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

    # Reset key for the entry-gating peak/trough dicts: just the ET calendar date
    # normally (once/day), or date+session (3x/day, at 4:00/9:30/16:00 ET) when
    # AUTO_SESSION_SCOPED_ENABLED is on. Flipping the toggle mid-day causes one
    # harmless extra reset the moment the key format changes.
    reset_key = f"{now_et.strftime('%Y-%m-%d')}:{get_current_session_name()}" if AUTO_SESSION_SCOPED_ENABLED else now_et.strftime("%Y-%m-%d")
    if _session_peak_date != reset_key:
        _session_peak_density     = {}
        _post_exit_density_trough = {}
        _session_peak_date        = reset_key

    finviz    = get_finviz_data()
    window_1h  = get_window_start_iso(60)
    window_20m = get_window_start_iso(20)
    window_40m = get_window_start_iso(40)

    # Entry-side-only windows — bounded to the current session's start when
    # AUTO_SESSION_SCOPED_ENABLED is on, so a candidate's density reading can't be
    # inflated by a prior session's activity. Deliberately separate from window_1h
    # above: the EXIT loop below always uses the fixed rolling window, because an
    # already-open position's own peak tracking must never reset mid-trade just
    # because a session boundary was crossed.
    if AUTO_SESSION_SCOPED_ENABLED:
        session_start_iso = get_current_session_start_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
        entry_window_1h  = max(window_1h,  session_start_iso)
        entry_window_20m = max(window_20m, session_start_iso)
        entry_window_40m = max(window_40m, session_start_iso)
    else:
        entry_window_1h, entry_window_20m, entry_window_40m = window_1h, window_20m, window_40m

    # Computed once here (moved ahead of the exit loop below) so both EXIT and ENTRY
    # monitoring share the same pass instead of calling this twice per tick.
    scores        = aggregate_ticker_scores(rolling_window_minutes=60, session_scoped=AUTO_SESSION_SCOPED_ENABLED)
    corr_by_ticker = {s["ticker"]: s.get("correlation") for s in scores}

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
        total_msgs = scored_messages.count_documents({
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

        # SMA peak tracking — mirrors peak_density above, but reads price_history
        # (already collected by the pipeline) instead of calling Finviz.
        current_sma = _current_sma(ticker)
        peak_sma    = trade.get("peak_sma")
        if current_sma is not None and (peak_sma is None or current_sma > peak_sma):
            auto_trades.update_one(
                {"ticker": ticker, "status": "active"},
                {"$set": {"peak_sma": current_sma}}
            )
            peak_sma = current_sma

        density_exit = (
            AUTO_EXIT_DENSITY_ENABLED and
            peak > 0 and
            ((peak - total_msgs) / peak) >= AUTO_EXIT_PEAK and
            (peak - total_msgs) >= AUTO_EXIT_MIN_DROP
        )
        sma_exit = (
            AUTO_EXIT_SMA_ENABLED and
            peak_sma is not None and current_sma is not None and
            peak_sma > 0 and
            ((peak_sma - current_sma) / peak_sma) >= AUTO_EXIT_SMA_PEAK
        )

        if density_exit or sma_exit:
            # Live Finviz call right at the exit decision — not a cached/stale bar —
            # so the recorded exit price and exit time are accurate to this instant.
            current_price = _current_ticker_price(ticker, finviz, live=True)
            exit_time     = datetime.now(timezone.utc)

            entry_price = trade.get("entry_price")
            return_pct  = None
            try:
                if entry_price and current_price:
                    return_pct = round(
                        (float(current_price) - float(entry_price)) / float(entry_price) * 100, 2
                    )
            except Exception:
                pass

            reasons = []
            if density_exit: reasons.append("density_off_peak")
            if sma_exit:      reasons.append("sma_off_peak")
            exit_reason = "+".join(reasons)

            exit_corr = corr_by_ticker.get(ticker)
            exit_corr_pct = round(exit_corr * 100) if exit_corr is not None else None

            ret_str = (("+" if return_pct >= 0 else "") + str(return_pct) + "%") if return_pct is not None else "N/A"
            position_size = trade.get("position_size", TRADE_POSITION_SIZE)
            pnl_dollars   = round(return_pct * position_size / 100, 2) if return_pct is not None else None
            completed_auto_trades.insert_one({
                "ticker":            ticker,
                "entry_price":       entry_price,
                "exit_price":        current_price,
                "return_pct":        return_pct,
                "entered_at":        trade["entered_at"],
                "exited_at":         exit_time,
                "entered_at_et":     trade.get("entered_at_et"),
                "exited_at_et":      _utc_dt_to_et_hhmmss(exit_time),
                "entry_corr":        trade.get("entry_corr"),
                "exit_corr":         exit_corr_pct,
                "entry_density":     trade.get("entry_density", 0),
                "peak_density":      peak,
                "exit_density":      total_msgs,
                "peak_sma":          peak_sma,
                "exit_sma":          current_sma,
                "exit_reason":       exit_reason,
                "source":            trade.get("source", "auto"),
                "market_cap":        trade.get("market_cap"),
                "market_cap_bucket": trade.get("market_cap_bucket"),
                "position_size":     position_size,
                "pnl_dollars":       pnl_dollars,
                "session_scoped":    trade.get("session_scoped"),
            })
            auto_trades.delete_one({"ticker": ticker, "status": "active"})
            # Seed the post-exit trough at this exit's density — AUTO_ENTRY_TROUGH_BOUNCE
            # requires a re-entry on this ticker to bounce up from whatever low point
            # density reaches from here, not just from this exit-time reading itself.
            _post_exit_density_trough[ticker] = total_msgs
            drop_pct    = round((peak - total_msgs) / peak * 100, 1) if peak else 0
            sma_drop_pct = round((peak_sma - current_sma) / peak_sma * 100, 1) if (peak_sma and current_sma is not None) else 0
            print(f"[AUTO TRADE] EXIT {ticker} | reason={exit_reason} | peak={peak} density={total_msgs} drop={peak - total_msgs} ({drop_pct}%) | peak_sma={peak_sma} sma={current_sma} ({sma_drop_pct}%) | price={current_price} | return: {ret_str}")

    # ── ENTRY monitoring — uses full correlation scores (computed above) ──
    for s in scores:
        ticker = s["ticker"]
        corr   = s.get("correlation")

        if ticker in active_map or ticker in recent_exits:
            continue

        # 1hr rolling density — recorded on entry for peak tracking
        total_msgs = scored_messages.count_documents({
            "ticker":         ticker,
            "created_at_utc": {"$gte": entry_window_1h}
        })

        # Session-so-far peak for this ticker, independent of trading hours —
        # blocks entries that are really just a local bounce inside a longer
        # decline from an earlier peak today (see AUTO_ENTRY_MAX_OFF_PEAK above).
        session_peak = max(_session_peak_density.get(ticker, 0), total_msgs)
        _session_peak_density[ticker] = session_peak
        not_off_session_peak = (
            session_peak <= 0 or
            ((session_peak - total_msgs) / session_peak) < AUTO_ENTRY_MAX_OFF_PEAK
        )

        # Tickers with no exit yet today have no trough to bounce off of — fall back
        # to the original "last 20 min vs prior 20 min" rising check for those.
        # Tickers that already exited today use AUTO_ENTRY_TROUGH_BOUNCE instead:
        # noisy tick-to-tick upticks kept re-triggering the old check throughout an
        # overall afternoon decline, so re-entry now requires density to have
        # genuinely bounced off the low point reached since that exit.
        trough = _post_exit_density_trough.get(ticker)
        if trough is None:
            count_20m = scored_messages.count_documents({
                "ticker":         ticker,
                "created_at_utc": {"$gte": entry_window_20m}
            })
            count_prior_20m = scored_messages.count_documents({
                "ticker":         ticker,
                "created_at_utc": {"$gte": entry_window_40m, "$lt": entry_window_20m}
            })
            density_rising = count_20m > count_prior_20m
        else:
            density_rising = trough <= 0 or ((total_msgs - trough) / trough) >= AUTO_ENTRY_TROUGH_BOUNCE
            if total_msgs < trough:
                _post_exit_density_trough[ticker] = total_msgs

        if (corr is not None and
                corr >= AUTO_ENTRY_CORR and
                total_msgs >= AUTO_ENTRY_MIN_DENS and
                density_rising and
                not_off_session_peak):
            # Live Finviz call right at the entry decision, same reasoning as the
            # exit side above — an accurate fill price for this exact moment.
            current_price = _current_ticker_price(ticker, finviz, live=True)
            if not current_price:
                continue
            entry_time = datetime.now(timezone.utc)
            corr_pct   = round(corr * 100)
            fv = finviz.get(ticker, {})
            auto_trades.insert_one({
                "ticker":            ticker,
                "status":            "active",
                "entry_price":       current_price,
                "entry_corr":        corr_pct,
                "entered_at":        entry_time,
                "entered_at_et":     _utc_dt_to_et_hhmmss(entry_time),
                "peak_density":      total_msgs,
                "entry_density":     total_msgs,
                "peak_sma":          _current_sma(ticker),
                "source":            "auto",
                "market_cap":        fv.get("market_cap", "--"),
                "market_cap_bucket": _market_cap_bucket(fv.get("market_cap")),
                "position_size":     TRADE_POSITION_SIZE,
                "session_scoped":    AUTO_SESSION_SCOPED_ENABLED,
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


def _launch_auto_trade_thread():
    """Starts run_auto_trade_loop in a background thread with the same crash-
    logging pattern as _launch_pipeline_thread() — run_auto_trade_loop's own
    while loop already isolates per-tick errors (a single bad _check_auto_trades
    call can't kill the thread), so the except BaseException here only catches
    something that escapes that loop entirely. Without this, a dead auto-trade
    thread would be a completely silent failure: trades just stop happening,
    with nothing in the logs and no way to notice short of checking manually."""
    global auto_trade_thread, auto_trade_stop_flag
    auto_trade_stop_flag = [False]
    stop_flag_ref = auto_trade_stop_flag

    def _run():
        auto_trade_health["last_start_at"]      = get_timestamp()
        auto_trade_health["last_seen_alive_at"] = get_timestamp()
        _log_pipeline_event("auto_trade_started")
        try:
            run_auto_trade_loop(stop_flag_ref)
        except BaseException as e:
            auto_trade_health["last_crash_at"]    = get_timestamp()
            auto_trade_health["last_crash_type"]  = type(e).__name__
            auto_trade_health["last_crash_error"] = str(e)
            auto_trade_health["crash_count"]     += 1
            print(f"[AUTO TRADE] CRASHED ({type(e).__name__}): {e}")
            print(traceback.format_exc())
            _log_pipeline_event("auto_trade_crashed", f"{type(e).__name__}: {e}")

    auto_trade_thread = threading.Thread(target=_run, daemon=True)
    auto_trade_thread.start()


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

    _launch_auto_trade_thread()
    print("[APP] Auto trade loop started.")

    threading.Thread(target=_backfill_screener_snapshots, daemon=True).start()
    print("[APP] Screener snapshot backfill scheduled.")

    _launch_pipeline_thread()
    print("[APP] Pipeline started automatically.")

    # Watchdog: polls every 30 s and restarts the pipeline (and, below, the
    # auto-trade loop) if either died unexpectedly. Checks each thread's own
    # stop_flag so an intentional stop is never auto-restarted. The whole loop
    # body is wrapped in try/except so the watchdog itself can never go silent
    # the way a monitored thread did — any unexpected error here still gets
    # logged and the loop keeps polling instead of dying quietly.
    def _pipeline_watchdog():
        while True:
            time.sleep(30)
            try:
                pipeline_health["watchdog_ticks"]        += 1
                pipeline_health["last_watchdog_tick_at"]  = get_timestamp()
                stopped_intentionally = pipeline_stop_flag and pipeline_stop_flag[0]
                alive = pipeline_thread is not None and pipeline_thread.is_alive()
                if alive:
                    pipeline_health["last_seen_alive_at"] = get_timestamp()
                if not stopped_intentionally and not alive:
                    print("[APP] Pipeline watchdog: thread died unexpectedly, restarting.")
                    _log_pipeline_event("watchdog_restart")
                    _launch_pipeline_thread()

                # Auto-trade thread — same self-healing, checked in the same tick
                # so there's one watchdog loop to reason about instead of two.
                # No manual stop exists for this thread today, so
                # auto_trade_stop_flag[0] is never intentionally True — a dead
                # thread always means "restart it."
                auto_stopped_intentionally = auto_trade_stop_flag and auto_trade_stop_flag[0]
                auto_alive = auto_trade_thread is not None and auto_trade_thread.is_alive()
                if auto_alive:
                    auto_trade_health["last_seen_alive_at"] = get_timestamp()
                if not auto_stopped_intentionally and not auto_alive:
                    print("[APP] Auto-trade watchdog: thread died unexpectedly, restarting.")
                    _log_pipeline_event("auto_trade_watchdog_restart")
                    _launch_auto_trade_thread()
            except BaseException as e:
                print(f"[APP] Pipeline watchdog error ({type(e).__name__}): {e}")
                print(traceback.format_exc())
                _log_pipeline_event("watchdog_error", f"{type(e).__name__}: {e}")

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