# event_detector.py
# Keyword-based event detection for Stocktwits messages and Finviz news headlines
# Returns a category label (e.g. "FDA Approval", "Earnings Beat") when a pattern matches
# Samuel Grana - Stocktwits News Sentiment Dashboard

import re

# ── EVENT PATTERN REGISTRY ──
# Each key is the display label returned by detect_event.
# Within each category, more-specific sub-types must come first — matching stops
# at the first hit, so a generic catch-all like "FDA Approval" must trail its sub-types.
EVENT_PATTERNS = {
    # ── FDA sub-types (most specific first; generic "FDA Approval" is the catch-all) ──
    "FDA Final Approval": [
        r"\b(nda|bla)\b.{0,40}approv",
        r"approv.{0,40}\b(nda|bla)\b",
        r"new drug application.{0,20}approv",
        r"biologics? license.{0,20}approv",
        r"(full|traditional).{0,20}(fda.{0,10}approv|approv.{0,10}fda)",
        r"fda.{0,10}(grants?|grant(ed)?).{0,10}approv",
    ],
    "FDA Breakthrough": [
        r"breakthrough therapy designation",
        r"breakthrough designation",
        r"\bfda\b.{0,30}breakthrough",
        r"breakthrough.{0,30}\bfda\b",
    ],
    "FDA Accel Approval": [
        r"accelerated approval",
        r"accelerated pathway",
        r"\bfda\b.{0,30}accelerat",
        r"surrogate endpoint.{0,30}approv",
        r"accelerat.{0,30}approv",
    ],
    "FDA Priority Review": [
        r"priority review",
        r"\bpdufa\b",
        r"6.month review",
        r"\bfda\b.{0,20}priority",
    ],
    "FDA Approval": [
        r"fda.{0,40}approv",
        r"approv.{0,40}fda",
        r"fda.{0,20}grant",
        r"approved by.{0,20}fda",
        r"receives? fda",
        r"fda clearance",
    ],
    "FDA Rejection": [
        r"complete response letter",
        r"\bcrl\b.{0,20}(fda|drug|receiv)",
        r"fda.{0,40}(reject|den(y|ied)|refus|declin)",
        r"(reject|denied|refused).{0,40}fda",
        r"fda.{0,20}not approv",
    ],
    "Merger/Acq": [
        r"\bmerger\b",
        r"\bacquir(es?|ed|ing|ition)\b",
        r"\bbuyout\b",
        r"\btakeover bid\b",
        r"\bgoing private\b",
        r"\bto be acquired\b",
    ],
    "Earnings Beat": [
        r"\b(earnings|eps|revenue|sales)\b.{0,40}(beat|exceed|surpass|topped|above estimate)",
        r"(beat|beats|topped|exceeded).{0,40}\b(estimate|expectation|consensus|eps|forecast)\b",
        r"better.{0,20}(than expected|than estimate)",
        r"blowout (earnings|quarter|results)",
    ],
    "Earnings Miss": [
        r"\b(earnings|eps|revenue|sales)\b.{0,40}(miss|below|fall short|disappoint|came in light)",
        r"(miss|missed|missing).{0,40}(estimate|expectation|consensus|eps)",
        r"below.{0,20}(estimate|expectation|forecast|consensus)",
    ],
    "Phase Trial": [
        r"\bphase (3|iii)\b.{0,60}(result|data|success|positive|meet|primary|endpoint)",
        r"(result|data|success|positive).{0,60}\bphase (3|iii)\b",
        r"\bphase (2|ii)\b.{0,60}(result|data|success|positive)",
        r"primary endpoint.{0,30}(met|achieved|reached)",
        r"(met|achieved).{0,30}primary endpoint",
    ],
    "Trading Halt": [
        r"\btrading halt(ed)?\b",
        r"\bhalted for\b",
        r"\bcircuit breaker\b",
        r"\bregulatory halt\b",
    ],
    "Guidance Up": [
        r"raise[sd]?.{0,20}(guidance|outlook|forecast)",
        r"guidance.{0,30}(above|raise[sd]?|increas|upward|better)",
        r"(raise[sd]?|increas).{0,20}(full.year|annual|fy\d{0,4}|q[1-4]).{0,20}(guidance|outlook)",
        r"above.{0,20}consensus.{0,20}(guidance|outlook)",
    ],
    "Short Squeeze": [
        r"\bshort squeeze\b",
        r"\bgamma squeeze\b",
        r"high short interest.{0,30}(squeeze|cover)",
        r"short (covering|sellers).{0,20}(squeez|rush|scrambl)",
    ],
    "Partnership": [
        r"\bstrategic (partnership|alliance|collaboration)\b",
        r"\blicensing (agreement|deal)\b",
        r"\bjoint venture\b",
        r"\bcollaboration agreement\b",
    ],
    "Dilution": [
        r"\b(public offering|secondary offering|follow.on offering)\b",
        r"\bat.the.market offering\b",
        r"\bshelf offering\b",
        r"\bprivate placement\b",
        r"(issu|offer).{0,20}(new|additional).{0,20}shares",
        r"\bdilut(ion|ive|ed|ing)\b.{0,30}shares",
    ],
    "Patent": [
        r"\bpatent.{0,30}(grant(ed)?|approv|issu|award)",
        r"(grant(ed)?|issu(ed)?|award(ed)?).{0,30}\bpatent\b",
    ],
}

# ── EVENT DISPLAY METADATA ──
# Maps each event label to a hex accent color and a short pill string for the frontend table.
EVENT_META = {
    "FDA Final Approval":  ("#22c55e", "FDA ✓✓"),
    "FDA Breakthrough":    ("#a855f7", "FDA BT"),
    "FDA Accel Approval":  ("#f97316", "FDA Acc"),
    "FDA Priority Review": ("#06b6d4", "FDA PR"),
    "FDA Approval":        ("#f59e0b", "FDA ✓"),
    "FDA Rejection":       ("#ef4444", "FDA ✗"),
    "Merger/Acq":    ("#a855f7", "M&A"),
    "Earnings Beat": ("#22c55e", "EPS ✓"),
    "Earnings Miss": ("#ef4444", "EPS ✗"),
    "Phase Trial":   ("#06b6d4", "Phase"),
    "Trading Halt":  ("#f97316", "HALT"),
    "Guidance Up":   ("#4ade80", "Guide ↑"),
    "Short Squeeze": ("#eab308", "Squeeze"),
    "Partnership":   ("#3b82f6", "Partner"),
    "Dilution":      ("#f87171", "Dilution"),
    "Patent":        ("#14b8a6", "Patent"),
}


def detect_event(body: str) -> str | None:
    """Returns the first matching event category label, or None."""
    if not body:
        return None
    # Iterates EVENT_PATTERNS in insertion order; returns as soon as a pattern hits.
    # Caller treats None as "no newsworthy event detected in this message."
    for event_type, patterns in EVENT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, body, re.IGNORECASE):
                return event_type
    return None
