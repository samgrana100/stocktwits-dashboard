# Stocktwits News Sentiment Dashboard

A real-time financial sentiment dashboard that ingests messages from Stocktwits, scores them with FinBERT and a composite trust/impact model, and displays ranked tickers on a live dashboard with interactive price and sentiment charts.

Built by Samuel Grana for Professor Dr. Kaamran Raahemifar.

---

## What It Does

- Fetches messages from Stocktwits every 90 seconds for every ticker in your Finviz screener
- Scores each message using three agents:
  - **Sentiment** — ProsusAI/FinBERT financial language model (−1 to +1)
  - **Trust** — author credibility based on followers, account age, post count, verified status
  - **Impact** — message density, sentiment change, and volume change signals
- Combines scores into a single composite score with recency weighting (15-minute half-life)
- Shows ranked tickers on a live dashboard with rolling windows from 1 minute to 1 month
- Popup chart per ticker shows normalized price, rolling score, and message density over the selected window
- Backfills 24 hours of history for each ticker on first pipeline run

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.10+ | Tested on 3.14 |
| MongoDB | Community Edition, running locally on port 27017 |
| Finviz Elite account | Required for screener export and price history API |
| ~4 GB disk | FinBERT model downloads on first run (~440 MB) + PyTorch |

---

## Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url>
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

> **Note:** PyTorch (`torch`) is a large package (~2 GB). The first install may take several minutes.

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Open `.env` and set:

- `FINVIZ_API_TOKEN` — your Finviz Elite API token (found in your account settings)
- `FINVIZ_EXPORT_URL` — the export URL from your Finviz screener (log in → Screener → apply filters → Export → copy the URL, excluding `&auth=...`)

MongoDB defaults (`localhost:27017`) can be left as-is for a local install.

### 4. Start MongoDB

Make sure MongoDB is running before starting the app:

```bash
# Windows (if installed as a service)
net start MongoDB

# Or start manually
mongod --dbpath C:\data\db
```

### 5. Run the dashboard

```bash
cd backend
python app.py
```

Open your browser at **http://localhost:5000**

The FinBERT model will download automatically on first run (~440 MB from HuggingFace). Subsequent starts are instant.

---

## Usage

1. **Start the pipeline** — click the **Start Pipeline** button in the top bar. This begins fetching Stocktwits messages and backfilling 24 hours of history for each ticker.

2. **Rolling window** — use the window buttons (1m → 1mo) to change how far back the sentiment scores look. The table re-ranks tickers immediately.

3. **Click any ticker** — opens a popup with:
   - Composite, Sentiment, Trust, and Impact scores
   - Combined price / rolling score / message density chart
   - Its own window selector independent of the main table
   - Mouse-wheel zoom and click-drag pan on the chart

4. **No Recent Messages section** — tickers from your Finviz screener that have no messages in the current window are shown below the ranked table. Click any chip to view its price chart.

5. **Reset scores** — if you want to re-score all messages with updated model weights, call:
   ```
   POST http://localhost:5000/api/reset-scores
   ```

---

## Project Structure

```
stocktwits-dashboard/
├── backend/
│   ├── app.py              # Flask server — all API routes, background threads
│   ├── pipeline.py         # 90-second Stocktwits fetch loop
│   ├── fetcher.py          # curl-cffi Stocktwits message fetcher (Chrome impersonation)
│   ├── score_calculator.py # FinBERT scoring + recency-weighted aggregation
│   ├── db.py               # MongoDB connection and index setup
│   ├── utils.py            # Ticker loading (Finviz screener), time helpers
│   └── agents/
│       ├── sentiment_agent.py  # FinBERT sentiment scoring
│       ├── trust_agent.py      # Author credibility scoring
│       └── impact_agent.py     # Message density / volume impact scoring
├── frontend/
│   └── index.html          # Single-page dashboard (Chart.js, vanilla JS)
├── data/
│   └── screener.csv        # Fallback ticker list if Finviz API is unavailable
├── .env.example            # Required environment variables template
├── requirements.txt        # Python dependencies
└── README.md
```

---

## Scoring Model

### Composite Score

```
quality         = 0.40 + (trust_score × 0.35) + (impact_score × 0.25)
composite_score = sentiment_score × quality
```

The floor of 0.40 ensures that even anonymous, low-impact messages retain 40% of their raw FinBERT signal rather than collapsing to near zero.

### Trust Score

Weighted combination of follower count (log scale), account age, post count, and verified status. A user with 200 followers scores ~0.58 (logarithmic, not linear).

### Recency Weighting

Ticker-level aggregation uses exponential decay with a 15-minute half-life, so messages from the last few minutes dominate the rolling score.

### Bull / Bear Breakdown

Messages with composite score > +0.05 count as bullish; < −0.05 as bearish. Displayed as a weighted percentage split under each ticker's score bar.

---

## Technical Notes

- **Stocktwits access** — uses `curl-cffi` with Chrome browser impersonation to bypass bot detection. No API key is required (the official API costs ~$10,000/year for commercial access).
- **Price data** — served from the Finviz Elite `quote_export` endpoint (1-minute intraday bars). Cached per ticker for 60 seconds to avoid hammering the API.
- **MongoDB deduplication** — a unique index on `message_id` prevents duplicate messages from being stored across pipeline loops and backfill runs.
- **FinBERT model** — `ProsusAI/finbert` from HuggingFace, a BERT model fine-tuned on financial text. Cached locally after first download.
