#!/usr/bin/env python3
"""
EndSARSList — Scraper v3 (Backfill + Daily)
=============================================
Two modes:
  python scraper_v3.py            # daily mode — last 2 days only
  python scraper_v3.py --backfill # backfill mode — goes back 10 years

How it works:
  - Searches each news site using their own search endpoint for each keyword
  - Paginates through ALL results (not just homepage)
  - Extracts article publish date → maps to date_missing / date_arrested
  - Deduplicates by source_url
  - All records go live immediately (is_approved=True)

Setup:
  pip install requests beautifulsoup4 supabase python-dotenv

Env vars:
  SUPABASE_URL=https://your-project.supabase.co
  SUPABASE_SERVICE_KEY=your-service-role-key
"""

import os, re, time, logging, hashlib, sys, argparse
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List, Tuple
from urllib.parse import urljoin, urlencode, quote_plus

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Config ────────────────────────────────────────────────────────────────────
BACKFILL_YEARS = 10
DAILY_DAYS = 2        # look back 2 days in daily mode (catches anything missed yesterday)
MAX_PAGES = 50        # max search result pages per keyword per source
REQUEST_DELAY = 1.0   # seconds between requests (be polite)
TIMEOUT = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EndSARSListBot/3.0; +https://endsarslist.com)",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

# ── Search keywords ───────────────────────────────────────────────────────────
# These are sent to each site's search engine
SEARCH_TERMS = [
    "arrested Nigeria",
    "detained Nigeria",
    "missing person Nigeria",
    "abducted Nigeria",
    "kidnapped Nigeria",
    "remanded custody Nigeria",
    "DSS custody Nigeria",
    "police arrest Nigeria",
    "disappeared Nigeria",
    "EndSARS arrested",
    "EndSARS missing",
    "EndSARS detained",
    "protester arrested Nigeria",
    "activist detained Nigeria",
    "journalist arrested Nigeria",
]

# Article-level keywords for classification
ARRESTED_KEYWORDS = [
    "arrested","detained","remanded","arraigned","charged to court",
    "in custody","police custody","DSS custody","military custody",
    "EFCC custody","army custody","grabbed","nabbed","apprehended",
    "picked up by police","picked up by DSS","taken into custody",
    "handed over to police","handed over to DSS","surrendered to police",
    "voluntarily surrendered","turned himself in","turned herself in",
    "bail denied","held without trial","locked up","behind bars",
    "protester arrested","activist arrested","journalist arrested",
    "blogger arrested","EndSARS arrest","detained activist",
]

MISSING_KEYWORDS = [
    "missing","gone missing","last seen","whereabouts unknown",
    "disappeared","cannot be found","help find","missing since",
    "unaccounted for","abducted","kidnapped","missing person",
    "forcibly disappeared","taken away","whereabouts",
    "has not returned","not been seen","cannot be reached",
]

NIGERIAN_STATES = [
    "Abia","Adamawa","Akwa Ibom","Anambra","Bauchi","Bayelsa","Benue","Borno",
    "Cross River","Delta","Ebonyi","Edo","Ekiti","Enugu","Gombe","Imo","Jigawa",
    "Kaduna","Kano","Katsina","Kebbi","Kogi","Kwara","Lagos","Nasarawa","Niger",
    "Ogun","Ondo","Osun","Oyo","Plateau","Rivers","Sokoto","Taraba","Yobe",
    "Zamfara","FCT","Abuja","Port Harcourt","Ibadan","Benin City",
]

AUTHORITIES = [
    "police","DSS","SSS","army","military","EFCC","ICPC","NSCDC",
    "immigration","customs","soldiers","naval","air force",
]

# ── Source definitions ────────────────────────────────────────────────────────
# (name, base_url, search_url_template, result_link_selector, next_page_selector)
# search_url_template uses {query} and {page}

SOURCES = [
    # ── Tier 1 national ──
    {
        "name": "Sahara Reporters",
        "base": "https://saharareporters.com",
        "search": "https://saharareporters.com/search/node/{query}?page={page}",
        "link_sel": "h3.title a, .search-result a",
        "next_sel": "a.pager-next",
        "date_sel": ".date-display-single, time, .field-name-post-date",
    },
    {
        "name": "Punch Nigeria",
        "base": "https://punchng.com",
        "search": "https://punchng.com/?s={query}&paged={page}",
        "link_sel": "h2.post-title a, h3.entry-title a, article h2 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date, .entry-date",
    },
    {
        "name": "Vanguard Nigeria",
        "base": "https://www.vanguardngr.com",
        "search": "https://www.vanguardngr.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time.entry-date, .post-date",
    },
    {
        "name": "Premium Times",
        "base": "https://www.premiumtimesng.com",
        "search": "https://www.premiumtimesng.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date, .entry-date",
    },
    {
        "name": "The Guardian Nigeria",
        "base": "https://guardian.ng",
        "search": "https://guardian.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Daily Trust",
        "base": "https://dailytrust.com",
        "search": "https://dailytrust.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "The Cable",
        "base": "https://www.thecable.ng",
        "search": "https://www.thecable.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "This Day Live",
        "base": "https://www.thisdaylive.com",
        "search": "https://www.thisdaylive.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Daily Post Nigeria",
        "base": "https://dailypost.ng",
        "search": "https://dailypost.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "The Nation Nigeria",
        "base": "https://thenationonlineng.net",
        "search": "https://thenationonlineng.net/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Legit.ng",
        "base": "https://www.legit.ng",
        "search": "https://www.legit.ng/search/?q={query}&p={page}",
        "link_sel": "a.item__title, h2 a, h3 a",
        "next_sel": "a.next, .pagination a[rel=next]",
        "date_sel": "time, .date, .post-date",
    },
    {
        "name": "Ripples Nigeria",
        "base": "https://www.ripplesnigeria.com",
        "search": "https://www.ripplesnigeria.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "The Whistler",
        "base": "https://thewhistler.ng",
        "search": "https://thewhistler.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "HumAngle",
        "base": "https://humanglemedia.com",
        "search": "https://humanglemedia.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Peoples Gazette",
        "base": "https://gazettengr.com",
        "search": "https://gazettengr.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "FIJ Nigeria",
        "base": "https://fij.ng",
        "search": "https://fij.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Leadership Nigeria",
        "base": "https://leadership.ng",
        "search": "https://leadership.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Nigerian Tribune",
        "base": "https://tribuneonlineng.com",
        "search": "https://tribuneonlineng.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Channels TV",
        "base": "https://www.channelstv.com",
        "search": "https://www.channelstv.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "BusinessDay Nigeria",
        "base": "https://businessday.ng",
        "search": "https://businessday.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "The Sun Nigeria",
        "base": "https://www.sunnewsonline.com",
        "search": "https://www.sunnewsonline.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Nairaland Crime",
        "base": "https://www.nairaland.com",
        "search": "https://www.nairaland.com/search/posts?q={query}&board=crime&page={page}",
        "link_sel": "a.post_title, td.subject a",
        "next_sel": "a[href*='page=']",
        "date_sel": ".post_time, span[title]",
    },
    {
        "name": "BBC Pidgin",
        "base": "https://www.bbc.com/pidgin",
        "search": "https://www.bbc.com/pidgin/search?q={query}&page={page}",
        "link_sel": "a[href*='/pidgin/']",
        "next_sel": "a[aria-label='Next Page']",
        "date_sel": "time",
    },
]


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
    article_date: Optional[str] = None   # ISO date string
    photo_url: str = ""
    charges: str = ""
    holding_location: str = ""
    arresting_authority: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────
def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.debug(f"fetch failed {url}: {e}")
        return None


def already_scraped(url: str) -> bool:
    try:
        r1 = supabase.table("arrested_persons").select("id").eq("source_url", url).limit(1).execute()
        r2 = supabase.table("missing_persons").select("id").eq("source_url", url).limit(1).execute()
        return bool(r1.data or r2.data)
    except:
        return False


def parse_date(text: str) -> Optional[str]:
    """
    Try to parse a date string into ISO format YYYY-MM-DD.
    Handles many formats found on Nigerian news sites.
    """
    if not text:
        return None
    text = text.strip()

    formats = [
        "%B %d, %Y",       # January 15, 2021
        "%b %d, %Y",       # Jan 15, 2021
        "%d %B %Y",        # 15 January 2021
        "%d %b %Y",        # 15 Jan 2021
        "%Y-%m-%dT%H:%M:%S%z",  # ISO
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%A, %B %d, %Y",   # Monday, January 15, 2021
        "%A, %d %B %Y",    # Monday, 15 January 2021
    ]

    # Clean up text
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'(st|nd|rd|th),', ',', text)  # remove ordinal suffixes

    for fmt in formats:
        try:
            return datetime.strptime(text[:len(fmt)+5], fmt).strftime("%Y-%m-%d")
        except:
            pass

    # Try extracting just a year-month-day pattern
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        return m.group(0)

    m = re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', text, re.IGNORECASE)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y").strftime("%Y-%m-%d")
        except:
            pass

    return None


def extract_article_date(soup: BeautifulSoup, date_sel: str) -> Optional[str]:
    """Extract publish date from article page."""
    # Try datetime attribute first (most reliable)
    for tag in soup.find_all("time"):
        dt = tag.get("datetime") or tag.get("content") or tag.get_text()
        parsed = parse_date(dt)
        if parsed:
            return parsed

    # Try meta tags
    for meta_prop in ["article:published_time", "datePublished", "date"]:
        m = soup.find("meta", property=meta_prop) or soup.find("meta", attrs={"name": meta_prop})
        if m and m.get("content"):
            parsed = parse_date(m["content"])
            if parsed:
                return parsed

    # Try CSS selectors from source config
    for sel in date_sel.split(","):
        el = soup.select_one(sel.strip())
        if el:
            parsed = parse_date(el.get("datetime") or el.get_text())
            if parsed:
                return parsed

    return None


def is_within_cutoff(date_str: Optional[str], cutoff: datetime) -> bool:
    """Return True if date is after the cutoff (or date is unknown)."""
    if not date_str:
        return True  # include unknown dates
    try:
        return datetime.strptime(date_str, "%Y-%m-%d") >= cutoff
    except:
        return True


def extract_name(text: str) -> Optional[str]:
    patterns = [
        r'\b(?:Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Prof\.?|Chief|Alhaji|Alhaja|Pastor|Rev\.?)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'identified\s+as\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'named\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}),?\s+(?:aged?|years?\s+old)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:was|has\s+been|were)\s+(?:arrested|detained|kidnapped|abducted|missing|nabbed|grabbed)',
        r'([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:of|from)\s+(?:' + '|'.join(NIGERIAN_STATES) + ')',
    ]
    skip = {'The Police','The Army','The Court','The Judge','The Governor',
            'The President','The Minister','Human Rights','Civil Society',
            'Amnesty International','Lagos State','Federal Government',
            'Nigerian Army','Nigerian Police'}
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            name = m.group(1).strip()
            if name not in skip and len(name.split()) >= 2:
                return name
    return None


def extract_age(text: str) -> Optional[int]:
    m = re.search(r'\b(\d{1,3})[- ]?year[s]?[- ]?old\b', text, re.IGNORECASE)
    if m:
        age = int(m.group(1))
        if 1 <= age <= 110:
            return age
    m = re.search(r'aged?\s+(\d{1,3})', text, re.IGNORECASE)
    if m:
        age = int(m.group(1))
        if 1 <= age <= 110:
            return age
    return None


def extract_gender(text: str) -> str:
    t = text.lower()
    male = sum(t.count(w) for w in [' he ',' his ',' him ',' man ',' boy ',' mr '])
    female = sum(t.count(w) for w in [' she ',' her ',' woman ',' girl ',' mrs ',' ms '])
    if female > male: return "female"
    if male > female: return "male"
    return "unknown"


def extract_state(text: str) -> str:
    for state in NIGERIAN_STATES:
        if re.search(r'\b' + re.escape(state) + r'\b', text, re.IGNORECASE):
            return state
    return ""


def extract_authority(text: str) -> str:
    t = text.lower()
    for auth in AUTHORITIES:
        if auth in t:
            return auth.upper() if len(auth) <= 4 else auth.title()
    return ""


def extract_charges(text: str) -> str:
    for pat in [
        r'charged\s+(?:with|for)\s+([^.]{5,80})',
        r'accused\s+of\s+([^.]{5,80})',
        r'count[s]?\s+of\s+([^.]{5,80})',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:200]
    return ""


def extract_holding(text: str) -> str:
    for pat in [
        r'(?:held|detained|remanded|kept)\s+(?:at|in)\s+([^,.]{5,60}(?:prison|facility|station|barracks|cell|custody))',
        r'(?:Kirikiri|Ikoyi|Kuje|Panti|Alagbon)[^,.\s]*',
        r'(\w+\s+(?:prison|correctional|detention|remand)[^,.]{0,40})',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()[:150]
    return ""


def classify(title: str, body: str) -> Optional[str]:
    combined = (title + " " + body[:2000]).lower()
    a = sum(1 for kw in ARRESTED_KEYWORDS if kw.lower() in combined)
    m = sum(1 for kw in MISSING_KEYWORDS if kw.lower() in combined)
    if a == 0 and m == 0:
        return None
    return "arrested" if a >= m else "missing"


# ── Search result pagination ──────────────────────────────────────────────────
def get_search_urls(source: dict, query: str, cutoff: datetime, backfill: bool) -> List[str]:
    """
    Paginate through search results for a given query on a source.
    Returns list of article URLs to scrape.
    Stop paginating when we hit articles older than cutoff.
    """
    urls = []
    max_pages = MAX_PAGES if backfill else 3

    for page in range(1, max_pages + 1):
        search_url = source["search"].format(
            query=quote_plus(query),
            page=page
        )
        soup = fetch(search_url)
        if not soup:
            break

        links = soup.select(source["link_sel"])
        if not links:
            break

        page_urls = []
        hit_old = False

        for a in links:
            href = a.get("href", "")
            if not href:
                continue
            if href.startswith("/"):
                href = source["base"] + href
            if not href.startswith("http"):
                continue
            if href in urls or href == source["base"]:
                continue

            # Check date of this result if shown in listing
            # (full date check happens when we fetch the article)
            page_urls.append(href)

        if not page_urls:
            break

        urls.extend(page_urls)
        log.debug(f"  {source['name']} '{query}' page {page}: {len(page_urls)} links")

        # Check if next page exists
        if not backfill:
            break
        next_btn = soup.select_one(source.get("next_sel", "a.next"))
        if not next_btn:
            break

        time.sleep(REQUEST_DELAY)

    return list(dict.fromkeys(urls))  # deduplicate preserving order


# ── Article scraping ──────────────────────────────────────────────────────────
def scrape_article(url: str, source: dict, cutoff: datetime) -> Optional[ScrapedPerson]:
    soup = fetch(url)
    if not soup:
        return None

    # Get title
    title = ""
    for t in [soup.find("h1"), soup.find("title")]:
        if t:
            title = t.get_text(" ", strip=True)
            break

    # Get body
    body = ""
    for sel in ["article", ".entry-content", ".post-content", ".article-body",
                ".story-body", ".content", "main"]:
        el = soup.select_one(sel)
        if el:
            body = el.get_text(" ", strip=True)
            break
    if not body:
        body = soup.get_text(" ", strip=True)

    # Get article date
    article_date = extract_article_date(soup, source.get("date_sel", "time"))

    # Check if within cutoff
    if not is_within_cutoff(article_date, cutoff):
        log.debug(f"  Skipping old article ({article_date}): {url}")
        return None

    # Classify
    record_type = classify(title, body)
    if not record_type:
        return None

    combined = title + " " + body[:3000]
    name = extract_name(combined)
    if not name:
        return None

    # Get photo
    photo_url = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        photo_url = og["content"]

    snippet = re.sub(r'\s+', ' ', body[:400]).strip()

    person = ScrapedPerson(
        full_name=name,
        source_url=url,
        source_name=source["name"],
        record_type=record_type,
        circumstances=snippet,
        state=extract_state(combined),
        age=extract_age(combined),
        gender=extract_gender(combined),
        article_date=article_date,
        photo_url=photo_url,
    )

    if record_type == "arrested":
        person.arresting_authority = extract_authority(combined)
        person.charges = extract_charges(combined)
        person.holding_location = extract_holding(combined)
    else:
        m = re.search(r'last\s+seen\s+(?:at|in|near)?\s+([^,.]{5,60})', combined, re.IGNORECASE)
        if m:
            person.last_seen_location = m.group(1).strip()[:150]

    return person


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
                "date_arrested": person.article_date,   # article date = arrest date
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
                "date_missing": person.article_date,    # article date = date missing reported
                "source": "scraped",
                "source_name": person.source_name,
                "source_url": person.source_url,
                "status": "missing",
                "is_approved": True,
            }
            supabase.table("missing_persons").insert(row).execute()

        log.info(f"  ✓ [{person.record_type}] {person.full_name} ({person.article_date or 'no date'}) — {person.source_name}")
        return True
    except Exception as e:
        log.error(f"  ✗ save failed {person.full_name}: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def run(backfill: bool = False):
    if backfill:
        cutoff = datetime.now() - timedelta(days=365 * BACKFILL_YEARS)
        log.info(f"=== BACKFILL MODE: going back to {cutoff.strftime('%Y-%m-%d')} ===")
    else:
        cutoff = datetime.now() - timedelta(days=DAILY_DAYS)
        log.info(f"=== DAILY MODE: last {DAILY_DAYS} days ===")

    start = datetime.utcnow()
    total_saved = 0
    total_checked = 0

    for source in SOURCES:
        log.info(f"\n── {source['name']} ──")
        source_saved = 0

        for query in SEARCH_TERMS:
            log.info(f"  Searching: '{query}'")
            article_urls = get_search_urls(source, query, cutoff, backfill)
            log.info(f"  Found {len(article_urls)} candidate URLs")

            for url in article_urls:
                total_checked += 1
                if already_scraped(url):
                    continue
                person = scrape_article(url, source, cutoff)
                if person:
                    if save_person(person):
                        source_saved += 1
                        total_saved += 1
                time.sleep(REQUEST_DELAY)

            time.sleep(REQUEST_DELAY * 2)  # pause between search terms

        log.info(f"  → {source['name']}: {source_saved} new records saved")
        time.sleep(2)  # pause between sources

    duration = (datetime.utcnow() - start).seconds

    # Log run
    try:
        supabase.table("scraper_runs").insert({
            "started_at": start.isoformat(),
            "duration_seconds": duration,
            "sources_checked": len(SOURCES),
            "records_found": total_saved,
            "errors": 0,
            "status": "success",
        }).execute()
    except Exception as e:
        log.error(f"Could not log run: {e}")

    log.info(f"\n=== Done. {total_saved} saved from {total_checked} checked in {duration}s ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true", help="Run backfill (10 years)")
    args = parser.parse_args()
    run(backfill=args.backfill)
