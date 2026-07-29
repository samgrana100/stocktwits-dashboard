# trust_agent.py
# Scores the credibility of a Stocktwits message author
# Based on followers, account age, post count, and verified status
# Samuel Grana - Stocktwits News Sentiment Dashboard

import math
from datetime import datetime, timezone


# ── TRUST SCORING ──

def calculate_trust_score(user: dict) -> float:
    """
    Takes a user object from a Stocktwits message and returns
    a trust score between 0 and 1.

    Higher score = more credible author.
    Lower score = less credible author.

    Scoring is based on four signals:
    - Follower count (how many people follow this user)
    - Account age (how long they have been on Stocktwits)
    - Post count (how many messages they have posted total)
    - Verified status (whether Stocktwits officially verified them)

    Args:
        user: The user dictionary from a Stocktwits message document

    Returns:
        Trust score as a float between 0.0 and 1.0
    """

    if not user:
        return 0.0

    # ─── SIGNAL 1: Follower Score (0 to 1) ───
    # Logarithmic scale: captures the meaningful difference between 100 and 1,000
    # followers without treating 9,000 and 10,000 as equally significant.
    # log10(10,000) = 4, so a 10k-follower account scores 1.0.
    followers = user.get("followers", 0)
    follower_score = min(math.log10(followers + 1) / 4.0, 1.0)

    # ─── SIGNAL 2: Account Age Score (0 to 1) ───
    # Scale: new account = 0, 5+ years old = 1
    join_date_str = user.get("join_date", "")
    age_score = 0.0
    if join_date_str:
        try:
            join_date = datetime.strptime(join_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            years_old = (now - join_date).days / 365.25
            age_score = min(years_old / 5.0, 1.0)
        except Exception:
            age_score = 0.0

    # ─── SIGNAL 3: Post Count Score (0 to 1) ───
    # Logarithmic scale: log10(1,000) = 3, so a 1k-post account scores 1.0.
    ideas = user.get("ideas", 0)
    post_score = min(math.log10(ideas + 1) / 3.0, 1.0)

    # ─── SIGNAL 4: Verified Bonus ───
    # Official verified accounts get a flat bonus
    official = user.get("official", False)
    verified_bonus = 0.2 if official else 0.0

    # ─── WEIGHTED COMBINATION ───
    # Followers and age weighted most heavily
    # Post count weighted moderately
    # Verified status adds a flat bonus on top
    raw_score = (
        follower_score * 0.40 +
        age_score      * 0.35 +
        post_score     * 0.25
    )

    # Add verified bonus and clamp to maximum of 1.0
    trust_score = min(raw_score + verified_bonus, 1.0)

    return round(trust_score, 4)


# Quick test - only runs when you execute this file directly
if __name__ == "__main__":

    # Test 1 - High trust user
    high_trust_user = {
        "followers": 8500,
        "join_date": "2016-03-15",
        "ideas": 3200,
        "official": False
    }

    # Test 2 - Low trust user
    low_trust_user = {
        "followers": 12,
        "join_date": "2024-11-01",
        "ideas": 8,
        "official": False
    }

    # Test 3 - Verified user
    verified_user = {
        "followers": 25000,
        "join_date": "2014-06-20",
        "ideas": 890,
        "official": True
    }

    # Test 4 - The AAPL user from our live fetch
    live_user = {
        "followers": 196,
        "join_date": "2015-04-15",
        "ideas": 6345,
        "official": False
    }

    print("--- Trust Agent Tests ---")
    print("High trust user:  " + str(calculate_trust_score(high_trust_user)))
    print("Low trust user:   " + str(calculate_trust_score(low_trust_user)))
    print("Verified user:    " + str(calculate_trust_score(verified_user)))
    print("Live AAPL user:   " + str(calculate_trust_score(live_user)))