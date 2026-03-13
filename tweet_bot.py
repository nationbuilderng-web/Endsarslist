#!/usr/bin/env python3
"""
EndSARSList — Tweet Bot
========================
Posts a thread to X every 2 days summarising new missing/arrested persons
added to the database since the last tweet.

Setup:
  pip install requests requests-oauthlib supabase python-dotenv

Env vars (GitHub Actions secrets):
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  X_CONSUMER_KEY
  X_CONSUMER_SECRET
  X_ACCESS_TOKEN
  X_ACCESS_TOKEN_SECRET
"""

import os, logging
from datetime import datetime, timedelta
from supabase import create_client, Client
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── X / Twitter ───────────────────────────────────────────────────────────────
X_API_URL = "https://api.twitter.com/2/tweets"

oauth = OAuth1Session(
    client_key=os.environ["X_CONSUMER_KEY"],
    client_secret=os.environ["X_CONSUMER_SECRET"],
    resource_owner_key=os.environ["X_ACCESS_TOKEN"],
    resource_owner_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
)

# ── Config ────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = 2     # fetch records added in last 2 days
MAX_NAMES_PER_TWEET = 5   # names per tweet in thread


def post_tweet(text: str, reply_to_id: str = None) -> str:
    """Post a tweet. Returns tweet ID."""
    payload = {"text": text}
    if reply_to_id:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to_id}
    r = oauth.post(X_API_URL, json=payload)
    if r.status_code not in (200, 201):
        raise Exception(f"Tweet failed {r.status_code}: {r.text}")
    tweet_id = r.json()["data"]["id"]
    log.info(f"  ✓ tweeted: {text[:60]}...")
    return tweet_id


def format_missing(r: dict) -> str:
    parts = [f"👤 {r['full_name']}"]
    if r.get("age"):
        parts.append(f"Age {r['age']}")
    if r.get("state"):
        parts.append(r["state"])
    return " · ".join(parts)


def format_arrested(r: dict) -> str:
    parts = [f"🔒 {r['full_name']}"]
    if r.get("age"):
        parts.append(f"Age {r['age']}")
    if r.get("state"):
        parts.append(r["state"])
    if r.get("arresting_authority"):
        parts.append(f"Detained by {r['arresting_authority']}")
    return " · ".join(parts)


def run():
    since = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    # Fetch new records from Supabase
    missing = supabase.table("missing_persons")\
        .select("*")\
        .eq("is_approved", True)\
        .gte("created_at", since)\
        .order("created_at", desc=True)\
        .execute().data

    arrested = supabase.table("arrested_persons")\
        .select("*")\
        .eq("is_approved", True)\
        .gte("created_at", since)\
        .order("created_at", desc=True)\
        .execute().data

    total = len(missing) + len(arrested)

    if total == 0:
        log.info("No new records in last 2 days — skipping tweet")
        return

    log.info(f"Found {len(missing)} missing, {len(arrested)} arrested — building thread")

    # ── Tweet 1: headline ────────────────────────────────────────────────────
    date_str = datetime.utcnow().strftime("%d %b %Y")
    tweet1 = (
        f"🇳🇬 EndSARSList Update — {date_str}\n\n"
        f"{total} new record{'s' if total != 1 else ''} added in the last 48 hours"
    )
    if missing:
        tweet1 += f"\n👤 {len(missing)} missing person{'s' if len(missing) != 1 else ''}"
    if arrested:
        tweet1 += f"\n🔒 {len(arrested)} arrested/detained"
    tweet1 += "\n\nendsarslist.com"

    # ── Tweet 2: names summary + CTA ─────────────────────────────────────────
    lines = []

    if missing:
        lines.append(f"MISSING ({len(missing)}):")
        for r in missing[:3]:
            lines.append(format_missing(r))


    if arrested:
        if lines:
            lines.append("")
        lines.append(f"ARRESTED/DETAINED ({len(arrested)}):")
        for r in arrested[:3]:
            lines.append(format_arrested(r))


    lines.append("")
    lines.append(
        "If you have information about any of these individuals "
        "or want to submit a report:\n"
        "👉 endsarslist.com\n\n"
        "#EndSARS #Nigeria #MissingPersons #HumanRights"
    )

    tweet2 = "\n".join(lines)
    if len(tweet2) > 280:
        tweet2 = tweet2[:277] + "..."

    # ── Post as thread ────────────────────────────────────────────────────────
    thread_id = post_tweet(tweet1)
    post_tweet(tweet2, reply_to_id=thread_id)
    log.info("Thread posted — 2 tweets")


if __name__ == "__main__":
    run()
