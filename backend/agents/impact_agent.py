# impact_agent.py
# Scores how much impact a Stocktwits message is likely to have
# Based on message density and symbol activity signals
# Samuel Grana - Stocktwits News Sentiment Dashboard

# ── IMPACT SCORING ──

def calculate_impact_score(message: dict, density_count: int) -> float:
    """
    Takes a raw Stocktwits message and the current message density
    for that ticker and returns an impact score between 0 and 1.

    Higher score = message posted during high activity, high attention ticker.
    Lower score = quiet ticker, low activity at time of posting.

    Scoring is based on three signals:
    - Message density (how many messages per minute for this ticker right now)
    - Sentiment change (how much the ticker sentiment has shifted on Stocktwits)
    - Volume change (how much trading volume has changed for this ticker)

    Args:
        message: Raw message dictionary from Stocktwits
        density_count: Number of messages stored for this ticker in current minute bucket

    Returns:
        Impact score as a float between 0.0 and 1.0
    """

    if not message:
        return 0.0

    # ─── SIGNAL 1: Message Density Score (0 to 1) ───
    # Scale: 0 messages/min = 0, 20+ messages/min = 1
    # High message density means the ticker is getting a lot of attention
    density_score = min(density_count / 20, 1.0)

    # ─── SIGNAL 2: Sentiment Change Score (0 to 1) ───
    # Stocktwits provides sentiment_change on the symbol object
    # A large shift in sentiment (positive or negative) = higher impact
    sentiment_change = 0.0
    symbols = message.get("symbols", [])
    if symbols:
        raw_change = symbols[0].get("sentiment_change", 0.0)
        if raw_change is None:
            raw_change = 0.0
        # Take absolute value — big shift in either direction = high impact
        # Scale: 0 = no change, 10+ = maximum impact
        sentiment_change = min(abs(raw_change) / 10.0, 1.0)

    # ─── SIGNAL 3: Volume Change Score (0 to 1) ───
    # A large spike in trading volume = higher impact
    # Scale: 0 = no change, 50%+ volume change = maximum impact
    volume_change = 0.0
    if symbols:
        raw_volume = symbols[0].get("volume_change", 0.0)
        if raw_volume is None:
            raw_volume = 0.0
        volume_change = min(abs(raw_volume) / 50.0, 1.0)

    # ─── WEIGHTED COMBINATION ───
    # Density weighted most heavily — message activity is the strongest signal
    # Sentiment change weighted moderately
    # Volume change weighted least — already captured partly by Finviz filters
    impact_score = (
        density_score    * 0.50 +
        sentiment_change * 0.30 +
        volume_change    * 0.20
    )

    return round(impact_score, 4)


# Quick test - only runs when you execute this file directly
if __name__ == "__main__":

    # Test 1 - High activity message
    high_activity_message = {
        "symbols": [
            {
                "symbol": "NVDA",
                "sentiment_change": 8.5,
                "volume_change": 45.2
            }
        ]
    }

    # Test 2 - Low activity message
    low_activity_message = {
        "symbols": [
            {
                "symbol": "AAPL",
                "sentiment_change": 0.25,
                "volume_change": 2.18
            }
        ]
    }

    # Test 3 - No symbol data
    no_symbol_message = {
        "symbols": []
    }

    print("--- Impact Agent Tests ---")
    print("High activity (density=18): " + str(calculate_impact_score(high_activity_message, 18)))
    print("Low activity  (density=3):  " + str(calculate_impact_score(low_activity_message, 3)))
    print("No symbol data (density=5): " + str(calculate_impact_score(no_symbol_message, 5)))
    print("High density only (density=25): " + str(calculate_impact_score(low_activity_message, 25)))