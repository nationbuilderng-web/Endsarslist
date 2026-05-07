#!/usr/bin/env python3
"""
EndSARSList - Scraper v5
========================
Daily mode keeps the lightweight recent-news path.
Backfill mode adds archive discovery from trusted publisher sitemaps.
"""

import argparse
import gzip
import io
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, quote_plus, urlparse, urlunparse

import anthropic
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def clean_env(name: str, default: str = "") -> str:
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else default


API_BASE_URL = clean_env("API_BASE_URL", "https://endsarslist-api.damidude.workers.dev").rstrip("/")
ADMIN_API_TOKEN = (
    clean_env("D1_ADMIN_TOKEN")
    or clean_env("SUPABASE_SERVICE_KEY")
    or clean_env("SUPABASE_KEY")
)
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


BACKFILL_YEARS = 10
DAILY_DAYS = 2
REQUEST_DELAY = 1.5
TIMEOUT = 20
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
ARTICLE_TEXT_LIMIT = 3000
BACKFILL_TEXT_LIMIT = 6000
BACKFILL_MAX_SITEMAPS = 40
BACKFILL_MAX_URLS_PER_DOMAIN = 80
BACKFILL_MAX_TOTAL_URLS = 1200

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

API_HEADERS = {
    "Content-Type": "application/json",
    "x-admin-token": ADMIN_API_TOKEN or "",
}

RELEVANCE_WORDS = [
    "arrested", "detained", "missing", "abducted", "kidnapped",
    "disappeared", "remanded", "custody", "endsars", "whereabouts",
    "has been missing", "taken by", "taken into custody", "apprehended",
    "nabbed", "rescued victim", "declared missing", "wanted person",
    "suspect", "suspects", "held hostage", "released after ransom",
]

URL_RELEVANCE_HINTS = (
    "arrest", "detain", "missing", "abduct", "kidnap", "remand",
    "custody", "endsars", "whereabout", "suspect", "victim",
    "hostage", "ransom", "rescue", "disappear",
)

SEARCH_QUERIES = [
    "arrested Nigeria",
    "detained Nigeria",
    "missing person Nigeria",
    "abducted Nigeria",
    "kidnapped Nigeria",
    "remanded custody Nigeria",
    "DSS custody Nigeria",
    "disappeared Nigeria",
    "has been missing Nigeria",
    "taken into custody Nigeria",
    "EndSARS arrested",
    "EndSARS missing",
    "EndSARS detained",
    "protester arrested Nigeria",
