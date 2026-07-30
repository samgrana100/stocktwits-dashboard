# Stocktwits Sentiment Dashboard

A real-time stock sentiment screener that ingests messages from Stocktwits, scores them with FinBERT and a composite trust/impact model, and surfaces actionable signals through a live dashboard — updated every 45 seconds.

**Built by Samuel Grana · Supervised by Dr. Kaamran Raahemifar · Pennsylvania State University**

🔴 **Live:** [web-production-8515c.up.railway.app](https://web-production-8515c.up.railway.app)

---

## Overview

Small-cap stocks often see Stocktwits message activity spike before a significant price move. This system measures that crowd behavior in real time and computes the correlation between message density and price — surfacing the alignment before it becomes obvious in price alone.

The pipeline fetches Stocktwits messages for every ticker in a Finviz Elite screener, scores each message using three AI agents (sentiment, trust, and impact), then computes a rolling composite score and a 30-minute correlation coefficient for every ticker. The frontend visualizes these signals across five views: a ranked screener table, a correlation card view, a news confirmation view, a 3D bubble map, and an exit monitor for active positions.

---

## Features

- **Live screener table** — tickers ranked by composite sentiment score with price/change from Finviz, bull/bear percentages, message density, and correlation
- **Correlation view** — Pearson correlation between message density and price over a 30-minute rolling window; tickers auto-sorted into Entry Zone, Watching, and user-pinned sections with drag-and-drop
- **Ticker popup** — combined chart of normalized price, rolling score, and message density; event markers (FDA, earnings, squeeze); toggleable SMA and MACD overlays; entry/exit trade markers
- **News view** — each ticker as a card with a mini chart, top-scored messages, and news from SEC 8-K, PR Newswire, Business Wire, GlobeNewswire, Benzinga, and FDA wire
- **Bubble map** — 3D scatter of sentiment vs. price change vs. volume with a time scrubber to replay the trading day
- **Signals panel** — live stream of highest-scoring Stocktwits messages across all tickers
- **Exit monitor** — tracks message density falloff from peak for active positions; plots entry/exit triangles on the chart

---

## Tech Stack

| Layer | Technology |
|---|---|
| Web framework | Flask (Python) — API routes, background threads, serves frontend |
| Production server | Gunicorn — WSGI server that wraps Flask for concurrent request handling |
| Cloud host | Railway — builds the container, runs Gunicorn, provides the public HTTPS URL |
| AI scoring | HuggingFace Transformers, `ProsusAI/finbert` |
| Database | MongoDB (Atlas or local) |
| Stocktwits fetch | `curl-cffi` with Chrome TLS impersonation |
| Finviz integration | Elite API — screener export CSV + `quote_export` price bars |
| Frontend | Vanilla JS, Chart.js, single HTML file served by Flask |

### Flask + Gunicorn + Railway

**Flask** is the web framework. It defines every API endpoint the dashboard uses (`/api/scores`, `/api/ticker/<ticker>`, etc.), serves the frontend `index.html`, and spawns the pipeline and calendar fetcher as background threads. Flask includes a built-in development server (`python app.py`), but that server is single-threaded and not suitable for production.

**Gunicorn** is the production WSGI server that wraps Flask. In production it runs as:

```
gunicorn --worker-class gthread --workers 1 --threads 4 --timeout 120 app:app
```

This gives Flask 4 concurrent threads — enough to handle dashboard polling, popup requests, and the pipeline control endpoint simultaneously without blocking.

**Railway** is the cloud platform that hosts the running application. It does not build the project — it hosts it. When code is pushed to GitHub, Railway detects the Python project via Nixpacks, installs all dependencies from `requirements.txt`, and starts Gunicorn using the command in `Procfile` and `railway.toml`. Railway injects a `PORT` environment variable that Gunicorn listens on, provides a public HTTPS URL, and automatically restarts the process on failure.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.10+ | Tested on 3.14 |
| MongoDB | Atlas or Community Edition on port 27017 |
| Finviz Elite account | Required for screener export and price history API |
| ~4 GB disk | FinBERT model downloads on first run (~440 MB) + PyTorch |

---

## Setup

### 1. Clone and create a virtual environment

```bash
git clone https://github.com/samgrana100/stocktwits-dashboard.git
cd stocktwits-dashboard
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> **Note:** PyTorch (`torch`) is large (~2 GB). The first install may take several minutes.

### 3. Configure environment variables

Create a `.env` file in the project root:

```env
MONGO_URI=mongodb://localhost:27017        # or your Atlas connection string
FINVIZ_API_TOKEN=your_token_here
FINVIZ_EXPORT_URL=https://elite.finviz.com/export.ashx?v=111&f=...
```

**Getting your Finviz credentials:**
- `FINVIZ_API_TOKEN` — Finviz Elite account → Settings → API Token
- `FINVIZ_EXPORT_URL` — Finviz Screener → apply your filters → Export → copy the URL and remove `&auth=...` from the end

### 4. Start MongoDB

```bash
# Windows (if installed as a service)
net start MongoDB

# Or start manually
mongod --dbpath C:\data\db
```

### 5. Run

```bash
cd backend
python app.py
```

Open **http://localhost:5000** in your browser.

FinBERT downloads automatically on first run (~440 MB from HuggingFace). Subsequent starts are instant.

---

## Usage

1. **Start the pipeline** — click **Start Pipeline** in the top bar. The system begins fetching Stocktwits messages and backfilling 24 hours of history for each ticker.

2. **Main table** — tickers ranked by composite score. Click any row to open the ticker popup. Sort by any column. Filter by change direction, relative volume, market cap, or keyword search.

3. **Correlation view** — click **Correlation** in the nav. Tickers with a high positive correlation between message density and price appear in the Entry Zone. Drag cards between sections to pin your own watchlist. Use the ticker search to add any symbol.

4. **News view** — click **News View** to see all active tickers as cards with mini charts, scored messages, and news articles.

5. **Bubble map** — click **Bubble Map** to see the full universe plotted by sentiment vs. price change vs. volume. Use the time slider to replay the day.

6. **Signals panel** — click **Signals** to open the live message stream.

7. **Rolling window** — use the window buttons (15m → 1w) to change how far back all score calculations look.

---

## Project Structure

```
stocktwits-dashboard/
├── backend/
│   ├── app.py                  # Flask server — all API routes, pipeline control, watchdog
│   ├── pipeline.py             # Concurrent Stocktwits ingestion loop (90-second cycle)
│   ├── fetcher.py              # Stocktwits message fetcher with Chrome TLS impersonation
│   ├── score_calculator.py     # Rolling score aggregation + 30-min correlation signal
│   ├── event_detector.py       # Catalyst keyword extraction (FDA, earnings, squeeze, M&A)
│   ├── calendar_fetcher.py     # Background PDUFA and earnings calendar fetcher
│   ├── backfill_events.py      # One-shot utility: retroactively tag historical messages
│   ├── db.py                   # MongoDB connection and index definitions
│   ├── utils.py                # Finviz ticker loader, timestamp helpers
│   └── agents/
│       ├── sentiment_agent.py  # FinBERT sentiment scoring (−1 to +1)
│       ├── trust_agent.py      # Author credibility scoring
│       └── impact_agent.py     # Catalyst keyword + engagement impact scoring
├── frontend/
│   └── index.html              # Single-page dashboard — all CSS, JS, and HTML
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
└── README.md
```

---

## Scoring Model

Every Stocktwits message produces three sub-scores that combine into a single composite:

```
composite_score = sentiment_score × (0.40 + trust_score × 0.35 + impact_score × 0.25)
```

| Score | Source |
|---|---|
| **Sentiment** | `ProsusAI/finbert` output — positive probability minus negative probability. Range: −1 to +1. |
| **Trust** | Poster follower count (log scale), ideas published, account classification, and verified status. |
| **Impact** | Boosted when the message contains a catalyst keyword (FDA, earnings, squeeze, merger, clinical trial). |

The 0.40 floor ensures that even anonymous, low-impact messages retain 40% of their raw FinBERT signal rather than collapsing toward zero.

**Bull/Bear breakdown** — messages with composite > +0.05 count as bullish; < −0.05 as bearish. Displayed as a weighted percentage split on each ticker row.

---

## Correlation Signal

The CORR column and Correlation view use a **30-minute rolling Pearson coefficient** between message density (posts per minute from Stocktwits) and price. Price snapshots are primarily sourced from the Finviz screener CSV, written to the database at the start of every 90-second pipeline loop; if Finviz hasn't priced a given ticker yet that loop, the price embedded in that ticker's own Stocktwits messages is used as a fallback so the signal keeps getting fresh data points through a redeploy or restart. A value near +1 means crowd activity and price are moving together — the earliest detectable signal of a momentum setup. Values are recomputed on every score refresh cycle, and require at least 5 price snapshots in the last 30 minutes before a ticker shows a value at all.

---

## Technical Notes

- **Stocktwits access** — `curl-cffi` impersonates Chrome at the TLS fingerprint level, not just the User-Agent string. This is required because Stocktwits blocks standard Python HTTP clients.
- **Price charts** — served from the Finviz Elite `quote_export` endpoint (1-minute intraday OHLC bars). Cached per ticker for 60 seconds.
- **Price and % Change columns** — sourced from the Finviz screener export CSV, not from Stocktwits.
- **MongoDB deduplication** — a unique index on `message_id` silently rejects duplicate inserts across pipeline loops and backfill runs.
- **FinBERT model** — `ProsusAI/finbert` from HuggingFace, fine-tuned on financial analyst reports and earnings call transcripts. Cached locally after first download (~440 MB).
- **PDUFA / earnings calendars** — fetched from BioPharma Catalyst (PDUFA) and Yahoo Finance (earnings) in a background thread. Refresh intervals: PDUFA every 6 hours, earnings every 24 hours.
- **Stale-cache guard** — if the Finviz API is temporarily unreachable, the pipeline continues using the last successfully fetched ticker list rather than stopping.
- **Correlation continuity across restarts** — the Finviz price cache (`_last_good_prices`) is in-memory and resets to empty on every process restart. Since correlation depends on 5+ price snapshots in the trailing 30-minute window, a slow restart can otherwise blank out correlation for every ticker until the cache repopulates. The Stocktwits-message price fallback in `pipeline.py` closes that gap by writing to `price_history` immediately once message fetching resumes, without waiting on a fresh Finviz fetch.
