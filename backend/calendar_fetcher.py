# calendar_fetcher.py
# Background fetcher for PDUFA (FDA decision dates) and earnings calendars.
# PDUFA: BioPharma Catalyst REST API at /api/fda-calendar (returns JSON directly)
# Earnings: Yahoo Finance quoteSummary API (no auth required)
# Samuel Grana - Stocktwits News Sentiment Dashboard

import time
import threading
import requests
from datetime import datetime, date, timedelta

from curl_cffi import requests as cffi_requests

from db import upcoming_catalysts

# ── Refresh intervals ──────────────────────────────────────────────────────────
PDUFA_INTERVAL    = 6  * 3600   # 6 hours
EARNINGS_INTERVAL = 24 * 3600   # 24 hours

LOOKAHEAD_DAYS = 90
LOOKBACK_DAYS  = 3   # keep recent-past earnings for post-earnings suppression

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

_BPC_API = "https://www.biopharmcatalyst.com/api/fda-calendar"


# ─── PDUFA CALENDAR ───────────────────────────────────────────────────────────

def fetch_pdufa_calendar():
    """
    Fetches PDUFA dates from BioPharma Catalyst's internal REST API.
    Endpoint: GET /api/fda-calendar
    Response:  {"data": {"data": [...rows...], "last_page": N, ...}}
    Row fields: company_ticker, catalyst_date (YYYY-MM-DD), drug_name, indication
    """
    print("[PDUFA] Fetching FDA calendar from BioPharma Catalyst API...")
    today  = date.today()
    cutoff = today + timedelta(days=LOOKAHEAD_DAYS)
    stored = 0

    try:
        page      = 1
        last_page = 1

        while page <= last_page:
            r = cffi_requests.get(
                _BPC_API,
                params={"page": page, "per_page": 200},
                impersonate="chrome120",
                timeout=20,
            )
            if r.status_code != 200:
                print(f"[PDUFA] HTTP {r.status_code} on page {page} — stopping")
                break

            payload   = r.json()
            data_obj  = payload.get("data", {})
            rows      = data_obj.get("data", [])
            last_page = data_obj.get("last_page", 1)

            for row in rows:
                ticker    = (row.get("company_ticker") or "").strip().upper()
                raw_date  = (row.get("catalyst_date")  or "").strip()
                drug      = (row.get("drug_name")      or "").strip()
                indication = (row.get("indication")    or "").strip()

                if not ticker or not raw_date:
                    continue

                try:
                    pdufa_date = date.fromisoformat(raw_date)
                except ValueError:
                    continue

                if pdufa_date < today - timedelta(days=1) or pdufa_date > cutoff:
                    continue

                desc = " — ".join(filter(None, [drug, indication])) or "PDUFA Decision"

                upcoming_catalysts.replace_one(
                    {"ticker": ticker, "event_type": "pdufa", "date": pdufa_date.isoformat()},
                    {
                        "ticker":      ticker,
                        "event_type":  "pdufa",
                        "date":        pdufa_date.isoformat(),
                        "description": desc,
                        "source":      "biopharmcatalyst",
                        "fetched_at":  datetime.utcnow(),
                    },
                    upsert=True,
                )
                stored += 1

            page += 1

        print(f"[PDUFA] Stored {stored} upcoming PDUFA dates")

    except Exception as e:
        print(f"[PDUFA] Fetch failed: {e}")


# ─── EARNINGS CALENDAR ────────────────────────────────────────────────────────

def fetch_earnings_calendar(tickers: list):
    """
    Fetches upcoming (and very recent past) earnings dates from Yahoo Finance.
    Stores into upcoming_catalysts.  Recent-past dates are kept so the frontend
    can suppress expected post-earnings event badges for 48 h.
    """
    if not tickers:
        return

    print(f"[EARNINGS] Fetching earnings dates for {len(tickers)} tickers...")
    today  = date.today()
    stored = 0

    for ticker in tickers:
        try:
            url = (
                "https://query1.finance.yahoo.com/v11/finance/quoteSummary/"
                f"{ticker}?modules=calendarEvents"
            )
            r = requests.get(url, headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                continue

            payload    = r.json()
            result     = ((payload.get("quoteSummary") or {}).get("result") or [])
            if not result:
                continue

            earn_dates = (
                result[0]
                .get("calendarEvents", {})
                .get("earnings", {})
                .get("earningsDate", [])
            )
            if not earn_dates:
                continue

            for ed in earn_dates:
                raw_ts = ed.get("raw")
                if not raw_ts:
                    continue
                earn_date = datetime.utcfromtimestamp(raw_ts).date()
                days_diff = (earn_date - today).days
                if days_diff < -LOOKBACK_DAYS or days_diff > LOOKAHEAD_DAYS:
                    continue

                upcoming_catalysts.replace_one(
                    {"ticker": ticker, "event_type": "earnings"},
                    {
                        "ticker":      ticker,
                        "event_type":  "earnings",
                        "date":        earn_date.isoformat(),
                        "description": "Earnings Report",
                        "source":      "yahoo",
                        "fetched_at":  datetime.utcnow(),
                    },
                    upsert=True,
                )
                stored += 1
                break  # nearest date only

        except Exception as e:
            print(f"[EARNINGS] {ticker}: {e}")

        time.sleep(0.25)  # gentle pacing

    print(f"[EARNINGS] Stored earnings dates for {stored}/{len(tickers)} tickers")


# ─── BACKGROUND THREAD LAUNCHER ──────────────────────────────────────────────

def start_calendar_threads(get_screener_tickers_fn):
    """
    Starts two daemon threads:
      pdufa-calendar:    immediate, then every 6 h
      earnings-calendar: waits until the screener cache is populated,
                         then runs once and every 24 h thereafter
    """

    def pdufa_loop():
        while True:
            try:
                fetch_pdufa_calendar()
            except Exception as e:
                print(f"[PDUFA LOOP] {e}")
            time.sleep(PDUFA_INTERVAL)

    def earnings_loop():
        # Poll until the screener cache has tickers (Finviz fetch happens on
        # the first /api/scores call from the frontend, which can take > 30 s).
        print("[EARNINGS] Waiting for screener cache to populate...")
        for _ in range(60):          # up to 5 minutes of waiting
            tickers = get_screener_tickers_fn()
            if tickers:
                break
            time.sleep(5)
        else:
            print("[EARNINGS] Screener cache never populated — skipping first run")

        while True:
            try:
                tickers = get_screener_tickers_fn()
                fetch_earnings_calendar(tickers)
            except Exception as e:
                print(f"[EARNINGS LOOP] {e}")
            time.sleep(EARNINGS_INTERVAL)

    threading.Thread(target=pdufa_loop,    daemon=True, name="pdufa-calendar").start()
    threading.Thread(target=earnings_loop, daemon=True, name="earnings-calendar").start()
    print("[CALENDAR] PDUFA + earnings background threads started")
