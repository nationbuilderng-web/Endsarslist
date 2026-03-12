#!/usr/bin/env python3
"""
EndSARSList — Web Scraper v2
============================
Scrapes 100+ Nigerian news sites for:
  - ARRESTED / DETAINED persons
  - MISSING persons

Arrested keywords: arrested, detained, grabbed, handed over to police,
                   volunteered into custody, DSS custody, remanded, charged

Missing keywords: missing, gone missing, last seen, disappeared, abducted,
                  kidnapped, whereabouts unknown

All scraped records go into Supabase with is_approved=False so you can
review them in the Supabase dashboard before they appear on the site.

Setup:
  pip install requests beautifulsoup4 supabase-py python-dotenv

Env vars (.env or GitHub Actions secrets):
  SUPABASE_URL=https://your-project.supabase.co
  SUPABASE_SERVICE_KEY=your-service-role-key

Run manually:
  python scraper_v2.py

Scheduled via GitHub Actions (see scraper.yml): daily at 6am UTC (7am WAT)
"""

import os, re, time, logging, hashlib
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Keywords ──────────────────────────────────────────────────────────────────

ARRESTED_KEYWORDS = [
    # Core arrest/detention
    "arrested", "detained", "remanded", "arraigned", "charged to court",
    "in custody", "police custody", "DSS custody", "military custody",
    "EFCC custody", "ICPC custody", "army custody",
    # Colloquial / Nigerian usage
    "grabbed", "nabbed", "apprehended", "picked up by police",
    "picked up by DSS", "picked up by soldiers", "taken into custody",
    "handed over to police", "handed over to DSS", "handed over to authorities",
    "handed to police", "surrendered to police", "surrendered to DSS",
    "turned himself in", "turned herself in", "voluntary surrender",
    "voluntarily surrendered", "reported himself to", "reported herself to",
    "walked into police", "walked into DSS",
    # Outcomes
    "bail denied", "held without trial", "locked up", "thrown in jail",
    "incarcerated", "imprisoned", "behind bars",
    # Roles
    "protester arrested", "activist arrested", "journalist arrested",
    "blogger arrested", "student arrested", "youth arrested",
    "detained activist", "detained journalist", "detained protester",
    "EndSARS arrest", "EndSARS detainee",
]

MISSING_KEYWORDS = [
    "missing", "gone missing", "last seen", "whereabouts unknown",
    "disappeared", "cannot be found", "help find", "missing since",
    "unaccounted for", "abducted", "kidnapped", "missing person",
    "forcibly disappeared", "taken away", "whereabouts",
    "has not returned", "not been seen", "cannot be reached",
    "missing child", "missing woman", "missing man",
]

NIGERIAN_STATES = [
    "Abia","Adamawa","Akwa Ibom","Anambra","Bauchi","Bayelsa","Benue","Borno",
    "Cross River","Delta","Ebonyi","Edo","Ekiti","Enugu","Gombe","Imo","Jigawa",
    "Kaduna","Kano","Katsina","Kebbi","Kogi","Kwara","Lagos","Nasarawa","Niger",
    "Ogun","Ondo","Osun","Oyo","Plateau","Rivers","Sokoto","Taraba","Yobe",
    "Zamfara","FCT","Abuja","Port Harcourt","Ibadan","Benin City","Enugu",
]

AUTHORITIES = [
    "police","DSS","SSS","army","military","EFCC","ICPC","NSCDC",
    "immigration","customs","soldiers","naval","air force",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EndSARSListBot/2.0; +https://endsarslist.com)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class ScrapedPerson:
    full_name: str
    source_url: str
    source_name: str
    record_type: str                # "arrested" or "missing"
    circumstances: str = ""
    last_seen_location: str = ""
    location_arrested: str = ""
    state: str = ""
    age: Optional[int] = None
    gender: str = "unknown"
    date_str: Optional[str] = None
    photo_url: str = ""
    charges: str = ""
    holding_location: str = ""
    arresting_authority: str = ""
    distinguishing_features: str = ""

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(url: str, timeout=15) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.warning(f"fetch failed {url}: {e}")
        return None


def url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def already_scraped(url: str) -> bool:
    """Check both tables for this source_url."""
    h = url_hash(url)
    try:
        r1 = supabase.table("arrested_persons").select("id").eq("source_url", url).limit(1).execute()
        r2 = supabase.table("missing_persons").select("id").eq("source_url", url).limit(1).execute()
        return bool(r1.data or r2.data)
    except:
        return False


def extract_name(text: str) -> Optional[str]:
    """
    Extract a Nigerian person's name from article text.
    Looks for patterns like:
      - Mr/Mrs/Dr/Prof Firstname Lastname
      - A man/woman identified as Firstname Lastname
      - Firstname Lastname, aged X
      - Firstname Lastname was arrested
    """
    patterns = [
        r'\b(?:Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Prof\.?|Chief|Alhaji|Alhaja|Pastor|Rev\.?)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'identified\s+as\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'named\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}),?\s+(?:aged?|years?\s+old)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:was|has\s+been|were)\s+(?:arrested|detained|kidnapped|abducted|missing|nabbed|grabbed)',
        r'([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:of|from)\s+(?:' + '|'.join(NIGERIAN_STATES) + ')',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            name = m.group(1).strip()
            # Filter out obvious non-names
            skip = {'The Police','The Army','The Court','The Judge','The Governor',
                    'The President','The Minister','Human Rights','Civil Society',
                    'Amnesty International','Lagos State'}
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
    male_score = sum(t.count(w) for w in [' he ',' his ',' him ',' man ',' boy ',' mr ','mr.'])
    female_score = sum(t.count(w) for w in [' she ',' her ',' woman ',' girl ',' mrs ',' ms ','mrs.','ms.'])
    if female_score > male_score:
        return "female"
    if male_score > female_score:
        return "male"
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
    patterns = [
        r'charged\s+(?:with|for)\s+([^.]{5,80})',
        r'accused\s+of\s+([^.]{5,80})',
        r'alleged\s+(?:offence|crime|charge)[s]?\s+of\s+([^.]{5,80})',
        r'count[s]?\s+of\s+([^.]{5,80})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:200]
    return ""


def extract_holding(text: str) -> str:
    patterns = [
        r'(?:held|detained|remanded|kept)\s+(?:at|in)\s+([^,.]{5,60}(?:prison|facility|station|barracks|cell|custody))',
        r'(?:Kirikiri|Ikoyi|Kuje|Panti|Alagbon|Abuja Remand|Borstal)[^,.\s]*',
        r'(\w+\s+(?:prison|correctional|detention|remand)[^,.]{0,40})',
        r'(?:at|in)\s+([A-Z][a-z]+\s+(?:police\s+station|barracks|headquarters))',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()[:150]
    return ""


def classify_article(title: str, body: str) -> Optional[str]:
    """Return 'arrested', 'missing', or None."""
    combined = (title + " " + body[:2000]).lower()
    arrested_hits = sum(1 for kw in ARRESTED_KEYWORDS if kw.lower() in combined)
    missing_hits = sum(1 for kw in MISSING_KEYWORDS if kw.lower() in combined)
    if arrested_hits == 0 and missing_hits == 0:
        return None
    if arrested_hits >= missing_hits:
        return "arrested"
    return "missing"


def get_article_links(soup: BeautifulSoup, base_url: str, limit=20) -> List[str]:
    """Extract article links from a news homepage."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = base_url.rstrip("/") + href
        if href.startswith("http") and base_url.split("/")[2] in href:
            if len(href) > len(base_url) + 5:
                links.add(href)
        if len(links) >= limit * 3:
            break
    return list(links)[:limit]


def scrape_article(url: str, source_name: str) -> Optional[ScrapedPerson]:
    """Scrape a single article and extract a person record if relevant."""
    soup = fetch(url)
    if not soup:
        return None

    title = ""
    t = soup.find("h1")
    if t:
        title = t.get_text(" ", strip=True)
    if not title:
        t = soup.find("title")
        if t:
            title = t.get_text(" ", strip=True)

    # Get body text
    body = ""
    for tag in ["article", "div.entry-content", "div.post-content",
                "div.article-body", "div.story-body", "div.content"]:
        el = soup.select_one(tag)
        if el:
            body = el.get_text(" ", strip=True)
            break
    if not body:
        body = soup.get_text(" ", strip=True)

    # Classify
    record_type = classify_article(title, body)
    if not record_type:
        return None

    # Extract person info
    combined = title + " " + body[:3000]
    name = extract_name(combined)
    if not name:
        return None

    # Get photo
    photo_url = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        photo_url = og["content"]

    # Build snippet for circumstances (first 300 chars of body)
    snippet = re.sub(r'\s+', ' ', body[:400]).strip()

    person = ScrapedPerson(
        full_name=name,
        source_url=url,
        source_name=source_name,
        record_type=record_type,
        circumstances=snippet,
        state=extract_state(combined),
        age=extract_age(combined),
        gender=extract_gender(combined),
        photo_url=photo_url,
    )

    if record_type == "arrested":
        person.arresting_authority = extract_authority(combined)
        person.charges = extract_charges(combined)
        person.holding_location = extract_holding(combined)
    else:
        # Try to extract last seen location
        m = re.search(r'last\s+seen\s+(?:at|in|near)?\s+([^,.]{5,60})', combined, re.IGNORECASE)
        if m:
            person.last_seen_location = m.group(1).strip()[:150]

    return person


def save_person(person: ScrapedPerson):
    """Save to Supabase. All scraped records are is_approved=False."""
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
                "distinguishing_features": person.distinguishing_features or None,
                "circumstances": person.circumstances or None,
                "photo_url": person.photo_url or None,
                "source": "scraped",
                "source_name": person.source_name,
                "source_url": person.source_url,
                "status": "missing",
                "is_approved": True,
            }
            supabase.table("missing_persons").insert(row).execute()

        log.info(f"  ✓ saved [{person.record_type}] {person.full_name}")
    except Exception as e:
        log.error(f"  ✗ save failed for {person.full_name}: {e}")


# ── News sources ──────────────────────────────────────────────────────────────
# Each entry: (source_name, homepage_url)

SOURCES = [
    # Tier 1 — national
    ("Sahara Reporters",        "https://saharareporters.com"),
    ("Punch Nigeria",           "https://punchng.com"),
    ("Vanguard Nigeria",        "https://www.vanguardngr.com"),
    ("Premium Times",           "https://www.premiumtimesng.com"),
    ("The Guardian Nigeria",    "https://guardian.ng"),
    ("Daily Trust",             "https://dailytrust.com"),
    ("This Day Live",           "https://www.thisdaylive.com"),
    ("The Cable",               "https://www.thecable.ng"),
    ("BusinessDay Nigeria",     "https://businessday.ng"),
    ("The Nation Nigeria",      "https://thenationonlineng.net"),
    ("The Sun Nigeria",         "https://www.sunnewsonline.com"),
    ("Leadership Nigeria",      "https://leadership.ng"),
    ("Nigerian Tribune",        "https://tribuneonlineng.com"),
    ("Daily Post Nigeria",      "https://dailypost.ng"),
    ("Naija.com",               "https://www.naija.com"),
    ("Legit.ng",                "https://www.legit.ng"),
    ("Pulse Nigeria",           "https://www.pulse.ng"),
    ("Nigerian Monitor",        "https://www.nigerianmonitor.com"),
    ("Independent Nigeria",     "https://independent.ng"),
    ("Blueprint Nigeria",       "https://www.blueprint.ng"),
    ("New Telegraph",           "https://newtelegraphng.com"),
    ("Nigeria Tribune",         "https://tribuneonlineng.com"),
    ("Ripples Nigeria",         "https://www.ripplesnigeria.com"),
    ("SaharaTV",                "https://saharareporters.com"),
    ("Arise News",              "https://www.arise.tv"),
    ("Channels TV",             "https://www.channelstv.com"),
    ("TVC News",                "https://www.tvcnews.tv"),
    ("NTA",                     "https://www.nta.ng"),
    # Tier 2 — regional & specialised
    ("Osun Defender",           "https://www.osundefender.com"),
    ("Nairaland",               "https://www.nairaland.com/crime"),
    ("YNaija",                  "https://ynaija.com"),
    ("BellaNaija",              "https://www.bellanaija.com"),
    ("Information Nigeria",     "https://www.informationng.com"),
    ("Instablog9ja",            "https://www.instablog9ja.com"),
    ("Nigerian Bulletin",       "https://www.nigerianbulletin.com"),
    ("The Whistler",            "https://thewhistler.ng"),
    ("HumAngle",                "https://humanglemedia.com"),
    ("Peoples Gazette",         "https://gazettengr.com"),
    ("FIJ Nigeria",             "https://fij.ng"),
    ("Dubawa",                  "https://dubawa.org"),
    ("Daily Nigerian",          "https://dailynigerian.com"),
    ("Morning Post",            "https://morningpost.ng"),
    ("The Pointer",             "https://thepointer.ng"),
    ("Delta Post",              "https://www.deltapost.com.ng"),
    ("Tori News",               "https://www.torinews.ng"),
    ("Veo News",                "https://veonews.ng"),
    ("Nigerian Pilot",          "https://www.nigerianpilot.com"),
    ("Ekiti Post",              "https://ekitipost.com.ng"),
    ("Osun State Tribune",      "https://osunstateonline.com"),
    ("Abuja Inquirer",          "https://abujainquirer.com"),
    ("Ogun State Tribune",      "https://ogunsun.com"),
    ("Imo State Tribune",       "https://imostateblog.com"),
    ("Enugu Metro",             "https://www.enugumetro.com"),
    ("Port Harcourt Metro",     "https://portharcourt.metro.ng"),
    ("Kano Focus",              "https://kanofocus.com"),
    ("Kaduna Focus",            "https://kadunafocus.com"),
    ("Arewa Agenda",            "https://arewaagenda.com"),
    # International covering Nigeria
    ("BBC Pidgin",              "https://www.bbc.com/pidgin"),
    ("Al Jazeera Africa",       "https://www.aljazeera.com/africa"),
    ("Reuters Africa",          "https://www.reuters.com/world/africa"),
    # Human rights
    ("Amnesty Nigeria",         "https://www.amnesty.org.ng"),
    ("Socio-Economic Rights",   "https://serap-nigeria.org"),
    ("HURIWA Nigeria",          "https://huriwanigeria.com"),
]


# ── Main scrape loop ──────────────────────────────────────────────────────────

def scrape_source(source_name: str, homepage: str) -> int:
    """Scrape one source. Returns count of new records saved."""
    log.info(f"Scraping {source_name} ({homepage})")
    soup = fetch(homepage)
    if not soup:
        return 0

    links = get_article_links(soup, homepage, limit=25)
    saved = 0

    for url in links:
        if already_scraped(url):
            continue
        person = scrape_article(url, source_name)
        if person:
            save_person(person)
            saved += 1
        time.sleep(0.5)   # polite delay

    log.info(f"  → {source_name}: {saved} new records")
    return saved


def run():
    start = datetime.utcnow()
    log.info(f"=== EndSARSList scraper started {start.isoformat()} ===")

    total = 0
    errors = 0
    for source_name, homepage in SOURCES:
        try:
            total += scrape_source(source_name, homepage)
        except Exception as e:
            log.error(f"Source failed {source_name}: {e}")
            errors += 1
        time.sleep(1)   # pause between sources

    duration = (datetime.utcnow() - start).seconds

    # Log run to Supabase
    try:
        supabase.table("scraper_runs").insert({
            "started_at": start.isoformat(),
            "duration_seconds": duration,
            "sources_checked": len(SOURCES),
            "records_found": total,
            "errors": errors,
            "status": "success" if errors < len(SOURCES) / 2 else "partial",
        }).execute()
    except Exception as e:
        log.error(f"Could not log scraper run: {e}")

    log.info(f"=== Done. {total} new records from {len(SOURCES)} sources in {duration}s ===")


if __name__ == "__main__":
    run()
