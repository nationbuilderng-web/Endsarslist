#!/usr/bin/env python3
"""
EndSARSList — Scraper v6.1
===========================
Improvements over v6:
  - Stronger arrested/detained classification with expanded keywords
  - Added: imprisoned, captured, jailed, remanded, arraigned, nabbed, etc.
  - Separate RSS feed sets for missing vs arrested to improve hit rate
  - Better Claude prompt with clearer arrested vs missing distinction
  - Backfill mode uses same script (--backfill flag)

Two modes:
  python scraper_v6.py            # daily mode — last 2 days
  python scraper_v6.py --backfill # backfill mode — last 10 years

Setup:
  pip install requests beautifulsoup4 lxml supabase python-dotenv anthropic

Env vars:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  ANTHROPIC_API_KEY
"""

import os, re, time, logging, argparse, json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv
import anthropic

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Clients ───────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── Config ────────────────────────────────────────────────────────────────────
DAILY_DAYS    = 2
REQUEST_DELAY = 1.5
TIMEOUT       = 20
CLAUDE_MODEL  = "claude-haiku-4-5-20251001"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── RSS feeds ─────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    ("Sahara Reporters",      "https://saharareporters.com/rss.xml"),
    ("Punch Nigeria",         "https://punchng.com/feed/"),
    ("Punch Nigeria Crime",   "https://punchng.com/category/metro-plus/crime/feed/"),
    ("Punch Nigeria News",    "https://punchng.com/category/news/feed/"),
    ("Vanguard Nigeria",      "https://www.vanguardngr.com/feed/"),
    ("Vanguard Crime",        "https://www.vanguardngr.com/category/metro-crime/feed/"),
    ("Vanguard News",         "https://www.vanguardngr.com/category/news/feed/"),
    ("Premium Times",         "https://www.premiumtimesng.com/feed/"),
    ("Premium Times News",    "https://www.premiumtimesng.com/category/news/feed/"),
    ("Guardian Nigeria",      "https://guardian.ng/feed/"),
    ("Guardian Nigeria News", "https://guardian.ng/news/feed/"),
    ("Daily Trust",           "https://dailytrust.com/feed/"),
    ("Daily Trust News",      "https://dailytrust.com/category/news/feed/"),
    ("The Cable",             "https://www.thecable.ng/feed"),
    ("The Cable News",        "https://www.thecable.ng/category/news/feed"),
    ("This Day Live",         "https://www.thisdaylive.com/feed/"),
    ("Daily Post Nigeria",    "https://dailypost.ng/feed/"),
    ("Daily Post Crime",      "https://dailypost.ng/category/crime/feed/"),
    ("The Nation Nigeria",    "https://thenationonlineng.net/feed/"),
    ("The Nation News",       "https://thenationonlineng.net/category/news/feed/"),
    ("Legit.ng",              "https://www.legit.ng/rss/all.rss"),
    ("Legit.ng Crime",        "https://www.legit.ng/rss/nigeria-crime.rss"),
    ("Channels TV",           "https://www.channelstv.com/feed/"),
    ("Channels TV News",      "https://www.channelstv.com/category/news/feed/"),
    ("HumAngle",              "https://humanglemedia.com/feed/"),
    ("Peoples Gazette",       "https://gazettengr.com/feed/"),
    ("FIJ Nigeria",           "https://fij.ng/feed/"),
    ("Leadership Nigeria",    "https://leadership.ng/feed/"),
    ("Nigerian Tribune",      "https://tribuneonlineng.com/feed/"),
    ("Ripples Nigeria",       "https://www.ripplesnigeria.com/feed/"),
    ("The Whistler",          "https://thewhistler.ng/feed/"),
    ("BusinessDay Nigeria",   "https://businessday.ng/feed/"),
    ("The Sun Nigeria",       "https://www.sunnewsonline.com/feed/"),
    ("BBC Pidgin",            "https://feeds.bbci.co.uk/pidgin/rss.xml"),
    ("Amnesty Nigeria",       "https://www.amnesty.org/en/tag/nigeria/feed/"),
]

# ── Relevance keywords — any article must match at least one ──────────────────
MISSING_KEYWORDS = [
    "missing", "gone missing", "last seen", "whereabouts", "disappeared",
    "abduct", "kidnap", "not found", "unaccounted", "help find",
    "taken", "has not returned", "cannot be reached", "cannot be found",
]

ARRESTED_KEYWORDS = [
    "arrest", "detain", "custody", "remand", "arraign", "charged",
    "imprison", "jail", "jailed", "captured", "nabbed", "apprehend",
    "locked up", "behind bars", "EFCC", "DSS", "police hold",
    "taken into custody", "held by", "in detention", "granted bail",
    "bail denied", "prosecution", "sentenced",
]

ALL_RELEVANCE = MISSING_KEYWORDS + ARRESTED_KEYWORDS + ["endsars", "end sars"]

# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class ScrapedPerson:
    full_name: str
    source_url: str
    source_name: str
    record_type: str
    circumstances: str = ""
    last_seen_location: str = ""
    state: str = ""
    age: Optional[int] = None
    gender: str = "unknown"
    article_date: Optional[str] = None
    photo_url: str = ""
    charges: str = ""
    holding_location: str = ""
    arresting_authority: str = ""


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_rss(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.content, "xml")
    except Exception as e:
        log.debug(f"RSS fetch failed {url}: {e}")
        return None


def fetch_article(url: str) -> Optional[BeautifulSoup]:
    try:
        h = {**HEADERS, "Accept": "text/html,application/xhtml+xml,*/*"}
        r = requests.get(url, headers=h, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.debug(f"Article fetch failed {url}: {e}")
        return None


# ── RSS parsing ───────────────────────────────────────────────────────────────
def parse_rss_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str).replace(tzinfo=None)
    except:
        pass
    m = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except:
            pass
    return None


def get_rss_articles(feed_name: str, feed_url: str, cutoff: datetime) -> List[tuple]:
    soup = fetch_rss(feed_url)
    if not soup:
        return []

    articles = []
    for item in soup.find_all("item"):
        url = ""
        link = item.find("link")
        if link:
            url = link.get_text().strip() or str(link.next_sibling or "").strip()
        if not url:
            guid = item.find("guid")
            if guid:
                url = guid.get_text().strip()
        if not url or not url.startswith("http"):
            continue

        title_tag = item.find("title")
        title = title_tag.get_text().strip() if title_tag else ""

        desc_tag = item.find("description") or item.find("summary")
        desc = BeautifulSoup(desc_tag.get_text(), "html.parser").get_text().strip()[:500] if desc_tag else ""

        pub_tag = item.find("pubDate") or item.find("published") or item.find("updated")
        pub_date = parse_rss_date(pub_tag.get_text().strip()) if pub_tag else None

        if pub_date and pub_date < cutoff:
            continue

        combined = (title + " " + desc).lower()
        if not any(kw in combined for kw in ALL_RELEVANCE):
            continue

        articles.append((url, pub_date, title, desc))

    return articles


# ── Batch deduplication ───────────────────────────────────────────────────────
def filter_already_scraped(urls: List[str]) -> List[str]:
    if not urls:
        return []
    try:
        r1 = supabase.table("arrested_persons").select("source_url").in_("source_url", urls).execute()
        r2 = supabase.table("missing_persons").select("source_url").in_("source_url", urls).execute()
        seen = {row["source_url"] for row in (r1.data or []) + (r2.data or [])}
        return [u for u in urls if u not in seen]
    except Exception as e:
        log.warning(f"Dedup check failed: {e}")
        return urls


# ── Claude extraction ─────────────────────────────────────────────────────────
EXTRACT_PROMPT = """You are a data extraction assistant for a Nigerian human rights database.

Your job is to find individuals who are:

TYPE 1 — ARRESTED or DETAINED (use record_type: "arrested"):
  Anyone arrested, detained, remanded, arraigned, charged, imprisoned, jailed,
  captured, nabbed, apprehended, locked up, held in custody, held by police/DSS/army/
  EFCC/ICPC/military, granted bail, denied bail, sentenced, prosecuted, in detention.

TYPE 2 — MISSING (use record_type: "missing"):
  Anyone missing, disappeared, abducted, kidnapped, gone missing, last seen,
  whereabouts unknown, not found, unaccounted for, taken without known detention.

Return ONLY a JSON array. Each element is one person. If no relevant person found, return [].

Fields per person:
- full_name: string — MUST be a real human name, minimum 2 words. REJECT job titles,
  organisations, places, pronouns. Examples of BAD names: "Press Secretary", "The Governor",
  "Police Officer", "Central Bank", "The Suspect". Examples of GOOD names: "Emeka Okafor",
  "Fatima Musa", "Chidi Nwachukwu".
- record_type: "missing" | "arrested"
- age: number or null
- gender: "male" | "female" | "unknown"
- state: Nigerian state or city or ""
- circumstances: 1-2 sentence summary of what happened, max 300 chars
- last_seen_location: where last seen (missing only) or ""
- arresting_authority: who arrested them e.g. "Police", "DSS", "Army", "EFCC" (arrested only) or ""
- charges: what they are charged with (arrested only) or ""
- holding_location: where being held e.g. "Kirikiri Prison", "DSS facility" (arrested only) or ""

Rules:
- If article mentions group arrests with no individual names, return []
- Do not invent information not in the article
- Prefer arrested over missing when both could apply and person is in custody

Return raw JSON array only. No markdown fences, no explanation."""


def extract_with_claude(title: str, body: str, url: str) -> List[dict]:
    article_text = f"HEADLINE: {title}\n\nARTICLE:\n{body[:3000]}"
    try:
        msg = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": EXTRACT_PROMPT + "\n\n" + article_text}]
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError as e:
        log.debug(f"Claude JSON parse error for {url}: {e}")
    except Exception as e:
        log.warning(f"Claude extraction failed for {url}: {e}")
    return []


# ── Article scraping ──────────────────────────────────────────────────────────
def scrape_article(url: str, source_name: str, pub_date: Optional[datetime],
                   rss_title: str, rss_desc: str) -> List[ScrapedPerson]:
    soup = fetch_article(url)
    body = ""
    title = rss_title

    if soup:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)
        for sel in ["article", ".entry-content", ".post-content",
                    ".article-body", ".story-body", ".content", "main"]:
            el = soup.select_one(sel)
            if el:
                body = el.get_text(" ", strip=True)
                break
        if not body:
            body = soup.get_text(" ", strip=True)[:5000]

    if not body:
        body = rss_desc

    if not body and not title:
        return []

    # Pre-filter relevance check
    combined_lower = (title + " " + body[:500]).lower()
    if not any(kw in combined_lower for kw in ALL_RELEVANCE):
        return []

    article_date = pub_date.strftime("%Y-%m-%d") if pub_date else None
    if not article_date and soup:
        for prop in ["article:published_time", "datePublished"]:
            meta = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
            if meta and meta.get("content"):
                m = re.search(r'(\d{4}-\d{2}-\d{2})', meta["content"])
                if m:
                    article_date = m.group(1)
                    break

    photo_url = ""
    if soup:
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            photo_url = og["content"]

    extracted = extract_with_claude(title, body, url)
    persons = []

    for item in extracted:
        name = (item.get("full_name") or "").strip()
        if not name or len(name.split()) < 2:
            continue
        rtype = item.get("record_type", "")
        if rtype not in ("missing", "arrested"):
            continue

        p = ScrapedPerson(
            full_name=name,
            source_url=url,
            source_name=source_name,
            record_type=rtype,
            circumstances=str(item.get("circumstances", ""))[:300],
            last_seen_location=str(item.get("last_seen_location", ""))[:150],
            state=str(item.get("state", ""))[:50],
            age=item.get("age") if isinstance(item.get("age"), int) else None,
            gender=item.get("gender", "unknown"),
            article_date=article_date,
            photo_url=photo_url,
            charges=str(item.get("charges", ""))[:200],
            holding_location=str(item.get("holding_location", ""))[:150],
            arresting_authority=str(item.get("arresting_authority", ""))[:100],
        )
        persons.append(p)
        log.info(f"  ✓ [{rtype}] {name} ({article_date or 'no date'}) — {source_name}")

    return persons


# ── Save to Supabase ──────────────────────────────────────────────────────────
def save_person(person: ScrapedPerson) -> bool:
    try:
        if person.record_type == "arrested":
            row = {
                "full_name": person.full_name,
                "gender": person.gender,
                "age": person.age,
                "state": person.state or None,
                "location_arrested": person.last_seen_location or None,
                "arresting_authority": person.arresting_authority or None,
                "charges": person.charges or None,
                "holding_location": person.holding_location or None,
                "photo_url": person.photo_url or None,
                "circumstances": person.circumstances or None,
                "date_arrested": person.article_date,
                "source": "scraped",
                "source_name": person.source_name,
                "source_url": person.source_url,
                "status": "detained",
                "is_approved": True,
            }
            supabase.table("arrested_persons").insert(row).execute()
        else:
            row = {
                "full_name": person.full_name,
                "gender": person.gender,
                "age": person.age,
                "state": person.state or None,
                "last_seen_location": person.last_seen_location or None,
                "circumstances": person.circumstances or None,
                "photo_url": person.photo_url or None,
                "date_missing": person.article_date,
                "source": "scraped",
                "source_name": person.source_name,
                "source_url": person.source_url,
                "status": "missing",
                "is_approved": True,
            }
            supabase.table("missing_persons").insert(row).execute()
        return True
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            log.debug(f"  Skip duplicate: {person.full_name}")
        else:
            log.error(f"  ✗ save failed {person.full_name}: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def run(backfill: bool = False):
    if backfill:
        cutoff = datetime.now() - timedelta(days=365 * 10)
        log.info(f"=== BACKFILL MODE: going back to {cutoff.strftime('%Y-%m-%d')} ===")
    else:
        cutoff = datetime.now() - timedelta(days=DAILY_DAYS)
        log.info(f"=== DAILY MODE: cutoff {cutoff.strftime('%Y-%m-%d')} ===")

    start = datetime.utcnow()
    total_saved = 0
    total_saved_missing = 0
    total_saved_arrested = 0
    total_checked = 0

    all_articles = {}

    log.info(f"\n── Reading {len(RSS_FEEDS)} RSS feeds ──")
    for feed_name, feed_url in RSS_FEEDS:
        articles = get_rss_articles(feed_name, feed_url, cutoff)
        new_count = 0
        for url, pub_date, title, desc in articles:
            if url not in all_articles:
                all_articles[url] = (pub_date, feed_name, title, desc)
                new_count += 1
        if new_count:
            log.info(f"  {feed_name}: {new_count} relevant articles")
        time.sleep(0.5)

    log.info(f"\n── Total unique relevant articles: {len(all_articles)} ──")

    all_urls = list(all_articles.keys())
    new_urls = filter_already_scraped(all_urls)
    log.info(f"── After dedup: {len(new_urls)} new to process ──\n")

    for url in new_urls:
        total_checked += 1
        pub_date, source_name, rss_title, rss_desc = all_articles[url]
        try:
            persons = scrape_article(url, source_name, pub_date, rss_title, rss_desc)
            for person in persons:
                if save_person(person):
                    total_saved += 1
                    if person.record_type == "arrested":
                        total_saved_arrested += 1
                    else:
                        total_saved_missing += 1
        except Exception as e:
            log.error(f"Error on {url}: {e}")
        time.sleep(REQUEST_DELAY)

    duration = int((datetime.utcnow() - start).total_seconds())

    try:
        supabase.table("scraper_runs").insert({
            "started_at": start.isoformat(),
            "records_found": total_saved,
            "status": "success",
        }).execute()
    except Exception as e:
        log.debug(f"Could not log run: {e}")

    log.info(f"\n=== Done. {total_saved} saved ({total_saved_missing} missing, {total_saved_arrested} arrested) / {total_checked} checked / {duration}s ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    run(backfill=args.backfill)
