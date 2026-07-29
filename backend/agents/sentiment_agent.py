# sentiment_agent.py
# Scores the sentiment of a Stocktwits message body
# Uses HuggingFace FinBERT - a model trained on financial text
# Samuel Grana - Stocktwits News Sentiment Dashboard

from transformers import pipeline
import re

# ── MODEL LOADING ──
# Loaded once at import time and shared across all requests in the process.
# First run downloads the ~400 MB weights to HuggingFace's local cache;
# subsequent imports are instantaneous.
print("[SENTIMENT] Loading FinBERT model...")
sentiment_pipeline = pipeline(
    "text-classification",
    model="ProsusAI/finbert",
    tokenizer="ProsusAI/finbert"
)
print("[SENTIMENT] Model loaded and ready.")


def clean_message(text) -> str:
    if not text:
        return ""
    """
    Cleans a raw Stocktwits message before sending to FinBERT.
    Removes ticker symbols, URLs, and excess whitespace.

    Args:
        text: Raw message body string

    Returns:
        Cleaned text string
    """
    # Remove ticker symbols like $AAPL
    text = re.sub(r'\$[A-Z]+', '', text)

    # Remove URLs
    text = re.sub(r'http\S+', '', text)

    # Remove extra whitespace and newlines
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def calculate_sentiment_score(message_body: str) -> float:
    """
    Takes the body text of a Stocktwits message and returns
    a sentiment score between -1 and 1.

    Positive score = bullish sentiment
    Negative score = bearish sentiment
    Near zero = neutral

    FinBERT returns three labels: positive, negative, neutral
    We convert these to a numeric score.

    Args:
        message_body: Raw text of the Stocktwits message

    Returns:
        Sentiment score as a float between -1.0 and 1.0
    """

    if not message_body or len(message_body.strip()) == 0:
        return 0.0

    # Clean the message first
    cleaned = clean_message(message_body)

    if len(cleaned) == 0:
        return 0.0

    # Truncate to 512 tokens max (FinBERT limit)
    cleaned = cleaned[:512]

    try:
        result = sentiment_pipeline(cleaned)[0]
        label = result["label"]
        confidence = result["score"]

        # Convert label and confidence to -1 to 1 scale
        if label == "positive":
            return round(confidence, 4)
        elif label == "negative":
            return round(-confidence, 4)
        else:
            # Neutral — return small value based on confidence
            return round(0.0, 4)

    except Exception as e:
        print("[SENTIMENT] Error scoring message: " + str(e))
        return 0.0


# ── BATCH SCORING ──

def calculate_sentiment_scores_batch(message_bodies: list) -> list:
    """
    Scores a list of message bodies in a single FinBERT forward pass.
    Returns a list of floats in the same order as the input.
    Falls back to individual scoring if the batch call fails.
    """
    if not message_bodies:
        return []

    cleaned = [clean_message(b)[:512] for b in message_bodies]

    # Pipeline errors on empty strings — replace with "neutral" as placeholder
    safe = [c if c else "neutral" for c in cleaned]

    try:
        # batch_size=32 keeps GPU memory usage bounded during large backfill runs
        results = sentiment_pipeline(safe, truncation=True, batch_size=32)
        scores = []
        for i, result in enumerate(results):
            # Messages that were empty after cleaning always score 0 regardless of FinBERT output
            if not cleaned[i]:
                scores.append(0.0)
                continue
            label      = result["label"]
            confidence = result["score"]
            if label == "positive":
                scores.append(round(confidence, 4))
            elif label == "negative":
                scores.append(round(-confidence, 4))
            else:
                scores.append(0.0)
        return scores
    except Exception as e:
        # If the batch call fails (e.g. OOM), fall back to one message at a time
        print("[SENTIMENT] Batch scoring failed, falling back to individual: " + str(e))
        return [calculate_sentiment_score(b) for b in message_bodies]


# Quick test - only runs when you execute this file directly
if __name__ == "__main__":

    test_messages = [
        "$NVDA breaking out hard, AI demand not slowing down. Adding more here.",
        "$TSLA this is going to zero, Elon has completely lost the plot.",
        "$AAPL looks okay I guess, nothing special happening today.",
        "$MSFT solid earnings beat, guidance raised, buying the dip.",
        "$AMD not sure about this move, feels extended to me."
    ]

    print("--- Sentiment Agent Tests ---\n")
    for msg in test_messages:
        score = calculate_sentiment_score(msg)
        if score > 0.3:
            label = "Bullish"
        elif score < -0.3:
            label = "Bearish"
        else:
            label = "Neutral"
        print("Score: " + str(score) + "  [" + label + "]")
        print("Message: " + msg[:60] + "...")
        print()