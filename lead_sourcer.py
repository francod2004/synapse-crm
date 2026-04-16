#!/usr/bin/env python3
"""
Unify Lead Sourcer Agent v3.0
================================
YellowPages.ca-only lead sourcing with manual-work scoring.

v3.0 Changes:
  - Dropped all dead sources (Google, Yelp, Bing, BBB, 411.ca)
  - Narrowed to Dental & Medical + Trades verticals
  - Added manual-work scoring (0-10) per prospect via homepage + Places API
  - Score < 3 -> skip; 3-5 -> medium priority; 6+ -> high priority
  - Removed AI gap descriptions (opp field no longer set at source time)
  - Removed owner-name hard filter (no name is fine if phone/email exists)
  - Dedup by slug ID instead of name
  - Circuit breaker + retry for YellowPages
  - New SMS format with priority breakdown

Usage:
    python lead_sourcer.py                             # Dental + Trades (default)
    python lead_sourcer.py --vertical "Dental & Medical" --area "Brampton, ON"
    python lead_sourcer.py --dry-run                   # Preview without DB writes
    python lead_sourcer.py --max 10                    # Max results per YP search

Requires env vars: SUPABASE_URL, SUPABASE_KEY, TWILIO_*, GOOGLE_PLACES_API_KEY (optional)
"""

import os, sys, re, json, time, random, argparse
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# -- Configuration ------------------------------------------------------------

def load_env(path=".env"):
    """Load key=value pairs from .env file if it exists."""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

load_env()

SUPABASE_URL          = os.getenv("SUPABASE_URL", "https://alfzjwzeccqswtytcylo.supabase.co")
SUPABASE_KEY          = os.getenv("SUPABASE_KEY", "")
TWILIO_SID            = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN          = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM           = os.getenv("TWILIO_FROM", "")
FRANCO_PHONE          = os.getenv("FRANCO_PHONE", "")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

# -- Search Parameters --------------------------------------------------------

VERTICALS = {
    "Dental & Medical": [
        "dentist", "dental clinic", "chiropractor", "physiotherapy clinic",
        "optometrist", "veterinary clinic", "walk-in clinic",
        "dermatologist", "orthodontist", "massage therapy clinic",
    ],
    "Trades": [
        "plumber", "electrician", "HVAC contractor", "roofing contractor",
        "landscaping company", "painting contractor", "general contractor",
        "handyman service", "fence installer", "garage door repair",
        "pest control", "tree service", "pool company", "paving contractor",
        "foundation repair", "waterproofing company", "septic service",
    ],
    # v3 narrow focus: dental + trades only
    # "Restaurants": [
    #     "restaurant", "cafe", "bakery", "pizzeria", "sushi restaurant",
    #     "bar and grill", "catering", "food truck", "diner", "bistro",
    #     "brunch spot", "steakhouse", "thai restaurant", "indian restaurant",
    #     "italian restaurant", "mexican restaurant", "bbq restaurant",
    # ],
    # v3 narrow focus: dental + trades only
    # "Retail": [
    #     "boutique", "clothing store", "gift shop", "jewelry store",
    #     "pet store", "florist", "furniture store", "shoe store",
    #     "home decor store", "sporting goods store", "vintage shop",
    #     "bridal shop", "optical store", "luggage store",
    # ],
    # v3 narrow focus: dental + trades only
    # "Salons & Spas": [
    #     "hair salon", "barbershop", "nail salon", "med spa",
    #     "beauty salon", "tanning salon", "day spa", "waxing studio",
    #     "lash studio", "tattoo shop",
    # ],
    # v3 narrow focus: dental + trades only
    # "Professional Services": [
    #     "law firm", "accounting firm", "insurance agency",
    #     "real estate agency", "mortgage broker", "financial advisor",
    #     "tax preparation", "notary public", "immigration consultant",
    # ],
    # v3 narrow focus: dental + trades only
    # "Fitness & Wellness": [
    #     "gym", "fitness studio", "yoga studio", "pilates studio",
    #     "crossfit gym", "martial arts studio", "personal training",
    #     "dance studio", "swimming school",
    # ],
    # v3 narrow focus: dental + trades only
    # "Auto Services": [
    #     "auto repair shop", "car detailing", "tire shop",
    #     "auto body shop", "oil change", "car wash",
    #     "transmission repair", "muffler shop",
    # ],
    # v3 narrow focus: dental + trades only
    # "Cleaning & Property": [
    #     "cleaning company", "janitorial service", "carpet cleaning",
    #     "window cleaning company", "property management company",
    #     "moving company", "junk removal", "storage facility",
    # ],
}

# Active verticals for v3
ACTIVE_VERTICALS = ["Dental & Medical", "Trades"]

# -- Smart Area Selection per Vertical ----------------------------------------
# Both active verticals use FULL (all 55 areas) in v3.

GTA_AREAS_FULL = [
    # Core Toronto
    "Toronto, ON", "Scarborough, ON", "Etobicoke, ON", "North York, ON",
    # Peel Region
    "Brampton, ON", "Mississauga, ON", "Caledon, ON", "Bolton, ON",
    # York Region
    "Vaughan, ON", "Markham, ON", "Richmond Hill, ON",
    "Newmarket, ON", "Aurora, ON", "Stouffville, ON", "King City, ON",
    # Halton Region
    "Oakville, ON", "Burlington, ON", "Milton, ON", "Georgetown, ON",
    "Halton Hills, ON", "Acton, ON",
    # Durham Region
    "Ajax, ON", "Pickering, ON", "Oshawa, ON", "Whitby, ON",
    "Clarington, ON", "Bowmanville, ON", "Uxbridge, ON",
    # ~80km radius expansions
    "Hamilton, ON", "Stoney Creek, ON", "Ancaster, ON", "Dundas, ON",
    "Grimsby, ON", "St. Catharines, ON", "Niagara Falls, ON",
    "Welland, ON", "Niagara-on-the-Lake, ON",
    "Guelph, ON", "Kitchener, ON", "Waterloo, ON", "Cambridge, ON",
    "Barrie, ON", "Innisfil, ON", "Orillia, ON", "Alliston, ON",
    "Orangeville, ON", "Shelburne, ON",
    "Cobourg, ON", "Port Hope, ON", "Peterborough, ON",
    "Brantford, ON", "Woodstock, ON", "Simcoe, ON",
]

GTA_AREAS_CORE = [
    "Toronto, ON", "Scarborough, ON", "Etobicoke, ON", "North York, ON",
    "Brampton, ON", "Mississauga, ON", "Vaughan, ON", "Markham, ON",
    "Richmond Hill, ON", "Oakville, ON", "Burlington, ON", "Hamilton, ON",
    "Oshawa, ON", "Whitby, ON", "Ajax, ON", "Pickering, ON",
    "Barrie, ON", "Guelph, ON", "Kitchener, ON", "Waterloo, ON",
    "St. Catharines, ON", "Newmarket, ON", "Milton, ON", "Cambridge, ON",
    "Caledon, ON",
]

GTA_AREAS_SMALL = [
    "Toronto, ON", "Scarborough, ON", "Etobicoke, ON", "North York, ON",
    "Brampton, ON", "Mississauga, ON", "Vaughan, ON", "Markham, ON",
    "Hamilton, ON", "Oakville, ON", "Burlington, ON", "Oshawa, ON",
    "Barrie, ON", "Kitchener, ON", "Guelph, ON",
]

# v3: both active verticals use FULL
VERTICAL_AREA_MAP = {
    "Dental & Medical": GTA_AREAS_FULL,
    "Trades": GTA_AREAS_FULL,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# -- Chain / Franchise Blocklist ----------------------------------------------

CHAIN_KEYWORDS = {
    # Fast food / QSR chains
    "mcdonald", "burger king", "wendy", "subway", "tim horton", "tims",
    "starbucks", "dunkin", "popeyes", "chick-fil-a", "kfc", "taco bell",
    "pizza hut", "domino", "little caesars", "papa john", "five guys",
    "pizza nova", "mamma's pizza", "mammas pizza", "cinnabon",
    "chipotle", "panera", "panda express", "arby", "sonic drive",
    "dairy queen", "baskin robbins", "cold stone", "auntie anne",
    "harvey", "mary brown", "swiss chalet", "st-hubert",
    "a&w", "new york fries", "mr. sub", "mr sub",
    # Casual dining chains
    "boston pizza", "east side mario", "montana", "the keg", "milestones",
    "jack astor", "kelsey", "casey", "moxie", "earls", "cactus club",
    "joey restaurant", "red lobster", "olive garden", "applebee",
    "denny", "ihop", "waffle house", "cheesecake factory",
    "the works", "wild wing", "buffalo wild wings", "wingstop",
    "freshii", "qdoba", "nando", "sunset grill", "outback steakhouse",
    "symposium cafe",
    # Coffee chains
    "second cup", "timothy", "balzac", "mccafe",
    # Grocery / retail chains
    "sobeys", "loblaws", "metro", "food basics", "freshco", "no frills",
    "walmart", "costco", "real canadian superstore", "superstore",
    "shoppers drug mart", "rexall", "dollarama", "dollar tree",
    "canadian tire", "home depot", "lowe", "rona", "home hardware",
    "winners", "marshalls", "homesense", "value village",
    "old navy", "gap", "h&m", "zara", "forever 21", "uniqlo",
    "best buy", "staples", "the source", "bed bath",
    "petsmart", "pet valu", "petcetera",
    "indigo", "chapters",
    "lcbo", "beer store",
    "reitmans", "addition elle", "northern reflections",
    "leon's furniture", "leons furniture", "bowring",
    "adonis market", "adonis grocery",
    # Trade chains / big contractors
    "mr. rooter", "mr rooter", "roto-rooter", "roto rooter",
    "mr. electric", "mr electric", "molly maid", "merry maids",
    "servpro", "servicemaster", "home instead",
    # Dental / medical chains
    "dentalcorp", "123 dentist", "appletree medical", "lifemark",
    # Salon / spa chains
    "great clips", "supercuts", "first choice haircutters",
    "sport clips", "fantastic sams", "mastercuts",
    # Fitness chains
    "goodlife fitness", "planet fitness", "anytime fitness",
    "la fitness", "fit4less", "orangetheory", "f45 training",
    "curves", "snap fitness",
    # Auto chains
    "mr. lube", "mr lube", "jiffy lube", "midas", "meineke",
    "speedy auto", "canadian tire auto", "kal tire",
    # Cleaning chains
    "servicemaster clean", "jan-pro", "coverall", "openworks",
    # Insurance / finance chains
    "state farm", "desjardins", "intact insurance",
    "allstate", "sun life", "manulife",
    "remax", "re/max", "royal lepage", "century 21", "keller williams",
    "coldwell banker", "sutton group",
    # Banks / insurance / corporate
    "td bank", "rbc", "bmo", "scotiabank", "cibc",
}

def is_chain_or_franchise(name):
    """Check if a business name matches a known chain or franchise."""
    name_lower = name.lower().strip()
    for keyword in CHAIN_KEYWORDS:
        if keyword in name_lower:
            return True
    franchise_patterns = [
        r'#\d+',
        r'store\s*#?\d+',
        r'location\s*#?\d+',
        r'unit\s*#?\d+',
    ]
    for pattern in franchise_patterns:
        if re.search(pattern, name_lower):
            return True
    return False

# ---- Target market filters (2-25 employees, owner-operated, single/double location) ----

# Signals that a business is too large (3+ locations, corporate, enterprise)
_TOO_LARGE_PATTERNS = [
    r'(\d+)\s*locations?',          # "5 locations", "12 locations"
    r'(\d+)\s*branches?',           # "3 branches"
    r'(\d+)\s*offices?',            # "4 offices"
    r'serving\s+\d+\s*(?:provinces?|countries)',  # "serving 3 provinces"
    r'nation\s*wide',               # "nationwide"
    r'across\s+canada',             # "across Canada"
    r'coast\s+to\s+coast',          # "coast to coast"
]
_TOO_LARGE_KEYWORDS = {
    "corporate office", "corporate headquarters", "head office",
    "enterprise solutions", "enterprise clients",
    "publicly traded", "nasdaq", "tsx",
    "fortune 500", "inc. 5000",
}

# Signals that a business is too small (solo, home-based, no real storefront)
_TOO_SMALL_KEYWORDS = {
    "freelance", "freelancer",
    "independent consultant",
    "home-based", "home based",
    "by appointment only",  # often solo operators
    "one-man", "one man",
    "solo practitioner",
}


def is_too_large(name, address="", notes=""):
    """Check if business signals 3+ locations or corporate scale."""
    text = f"{name} {address} {notes}".lower()
    for kw in _TOO_LARGE_KEYWORDS:
        if kw in text:
            return True
    for pattern in _TOO_LARGE_PATTERNS:
        m = re.search(pattern, text, re.I)
        if m and m.group(0)[0].isdigit():
            count = int(re.search(r'\d+', m.group(0)).group())
            if count >= 3:
                return True
    return False


def is_too_small(name, address="", notes=""):
    """Check if business signals solo operator or home-based."""
    text = f"{name} {address} {notes}".lower()
    for kw in _TOO_SMALL_KEYWORDS:
        if kw in text:
            return True
    return False


def clean_business_name(name):
    """Clean up scraped business name."""
    name = re.sub(r'^\d+', '', name).strip()
    name = re.sub(
        r'\s*-\s*(Toronto|Brampton|Mississauga|Vaughan|Markham|Scarborough|Etobicoke|North York'
        r'|Hamilton|Barrie|Guelph|Kitchener|Waterloo|Cambridge|Oshawa|Burlington|Oakville'
        r'|St\.? Catharines|Niagara Falls|Peterborough|Brantford|Whitby|Ajax|Pickering)\s*$',
        '', name, flags=re.I
    )
    return name.strip()

def slugify(text):
    """Convert text to a URL-safe slug for prospect IDs."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')[:80]


# -- Circuit Breaker (YellowPages) --------------------------------------------

class YPCircuitBreaker:
    """
    Tracks consecutive YP failures. After MAX_ERRORS consecutive failures,
    pauses for 5 minutes, tries once more. If that fails too, signals abort.
    """
    MAX_ERRORS = 3
    PAUSE_SECONDS = 300  # 5 minutes

    def __init__(self):
        self.consecutive_errors = 0
        self.total_calls = 0
        self.total_successes = 0
        self.paused_once = False
        self.should_abort = False

    def record_success(self, result_count):
        self.consecutive_errors = 0
        self.total_calls += 1
        self.total_successes += 1

    def record_failure(self, reason=""):
        self.consecutive_errors += 1
        self.total_calls += 1
        if self.consecutive_errors >= self.MAX_ERRORS:
            if not self.paused_once:
                print(f"   [CIRCUIT BREAKER] YP hit {self.MAX_ERRORS} consecutive failures ({reason})")
                print(f"   [CIRCUIT BREAKER] Pausing {self.PAUSE_SECONDS}s before one more attempt...")
                time.sleep(self.PAUSE_SECONDS)
                self.paused_once = True
                self.consecutive_errors = 0  # reset for the retry attempt
            else:
                print(f"   [CIRCUIT BREAKER] YP failed again after pause -- aborting run")
                self.should_abort = True

    def summary(self):
        status = "ABORTED" if self.should_abort else "OK"
        return f"YP: {self.total_successes}/{self.total_calls} [{status}]"


# -- Supabase Helpers ---------------------------------------------------------

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

def sb_get_existing_ids():
    """Fetch all existing prospect IDs (slug strings) for dedup."""
    url = f"{SUPABASE_URL}/rest/v1/prospects?select=id"
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return {row["id"] for row in r.json()}
    print(f"  Warning: Could not fetch existing prospects: {r.status_code}")
    return set()

def sb_insert_prospects(prospects):
    """Insert a batch of prospects into Supabase. Returns count inserted."""
    if not prospects:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/prospects"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"
    r = requests.post(url, headers=headers, json=prospects, timeout=30)
    if r.status_code in (200, 201):
        return len(prospects)
    print(f"  Warning: Supabase insert error {r.status_code}: {r.text[:200]}")
    return 0

# -- Twilio SMS Helper --------------------------------------------------------

def send_sms(body):
    """Send an SMS via Twilio REST API."""
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, FRANCO_PHONE]):
        print("  Warning: Twilio not configured -- skipping SMS")
        print(f"  Message would be:\n     {body}")
        return False
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    r = requests.post(
        url,
        auth=(TWILIO_SID, TWILIO_TOKEN),
        data={"From": TWILIO_FROM, "To": FRANCO_PHONE, "Body": body[:1600]},
        timeout=15,
    )
    if r.status_code == 201:
        print(f"  SMS sent to {FRANCO_PHONE}")
        return True
    print(f"  Warning: SMS failed ({r.status_code}): {r.text[:200]}")
    return False


# ==============================================================================
# YellowPages.ca Scraper (the ONLY source in v3)
# ==============================================================================

YP_SEARCH_TERMS = {
    "Restaurants": ["Restaurants", "Cafes", "Bakeries", "Pizza", "Catering", "Steakhouse", "Brunch"],
    "Retail": ["Boutiques", "Clothing+Stores", "Gift+Shops", "Pet+Stores", "Florists", "Furniture+Store"],
    "Trades": ["Plumbers", "Electricians", "HVAC", "Roofing", "Landscaping", "Painters", "Pest+Control"],
    "Dental & Medical": ["Dentists", "Dental+Clinic", "Chiropractors", "Physiotherapy", "Veterinarians", "Optometrists"],
    "Salons & Spas": ["Hair+Salons", "Barbershops", "Nail+Salons", "Day+Spas", "Beauty+Salons", "Med+Spa"],
    "Professional Services": ["Law+Firms", "Accounting+Firms", "Insurance+Agency", "Real+Estate+Agency", "Mortgage+Broker"],
    "Fitness & Wellness": ["Gyms", "Fitness+Studio", "Yoga+Studio", "Martial+Arts", "Dance+Studio", "Personal+Training"],
    "Auto Services": ["Auto+Repair", "Car+Detailing", "Tire+Shop", "Auto+Body+Shop", "Car+Wash"],
    "Cleaning & Property": ["Cleaning+Company", "Janitorial+Services", "Property+Management", "Moving+Company", "Junk+Removal"],
}

def scrape_yellowpages(search_term, area, max_results=10):
    """
    Scrape YellowPages.ca for business listings.
    Returns list of dicts with name, address, phone, website, snippet, source.
    On 429/timeout: waits 30-60s random, retries once.
    """
    results = []
    location = area.replace(", ", "+").replace(" ", "+")
    url = f"https://www.yellowpages.ca/search/si/1/{quote_plus(search_term)}/{location}"

    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)

            if r.status_code == 429:
                if attempt < max_attempts - 1:
                    wait = random.uniform(30, 60)
                    print(f"     [YP] 429 rate limited -- waiting {wait:.0f}s and retrying...")
                    time.sleep(wait)
                    continue
                else:
                    print(f"     [YP] 429 rate limited -- giving up after retry")
                    raise Exception("429 Too Many Requests")

            r.raise_for_status()
            html = r.text
            print(f"     [YP] response: {r.status_code}, {len(html)} chars")

            soup = BeautifulSoup(html, "lxml")
            listings = soup.select("div.listing, div.listing__content, div[class*='listing']")
            if not listings:
                listings = soup.select("div.resultList div, div.result")

            for listing in listings[:max_results]:
                name_el = listing.select_one(
                    "a.listing__name--link, h3.listing__name, "
                    "a[class*='listing__name'], span.listing__name, h2 a, h3 a"
                )
                if not name_el:
                    continue
                raw_name = name_el.get_text(strip=True)
                if not raw_name or len(raw_name) < 3:
                    continue

                name = clean_business_name(raw_name)
                if not name or is_chain_or_franchise(name):
                    if name:
                        print(f"   [YP] Filtered chain: {name}")
                    continue

                # Address
                addr_el = listing.select_one(
                    "span.listing__address--full, span[class*='address'], "
                    "div.listing__address, span.adr"
                )
                address = addr_el.get_text(strip=True) if addr_el else ""

                # Phone
                phone = ""
                phone_el = listing.select_one(
                    "a[class*='phone'], span[class*='phone'], "
                    "a[data-phone], a[href^='tel:'], "
                    "span.mlr__sub-text, li.mlr__item--phone, span.listing__phone"
                )
                if phone_el:
                    tel_href = phone_el.get("href", "")
                    if tel_href.startswith("tel:"):
                        phone = tel_href.replace("tel:", "").strip()
                    elif phone_el.get("data-phone"):
                        phone = phone_el.get("data-phone")
                    else:
                        phone = phone_el.get_text(strip=True)
                if not phone:
                    for a in listing.select("a[href^='tel:']"):
                        tel = a.get("href", "").replace("tel:", "").strip()
                        if len(tel) >= 10:
                            phone = tel
                            break
                if not phone:
                    all_text = listing.get_text(" ", strip=True)
                    phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
                    if phone_match:
                        phone = phone_match.group(1)
                phone = re.sub(r'[^\d+()-.\s]', '', phone).strip()

                # Website
                website = ""
                web_el = listing.select_one(
                    "a[class*='website'], a[data-analytics='website'], a.listing__link--website"
                )
                if web_el:
                    website = web_el.get("href", "")
                if not website:
                    for a in listing.select("a[href^='http']"):
                        href = a.get("href", "")
                        if "yellowpages.ca" not in href and "ypcdn" not in href:
                            website = href
                            break

                # Category / snippet
                cat_el = listing.select_one("span[class*='category'], div[class*='category']")
                snippet = cat_el.get_text(strip=True) if cat_el else ""

                results.append({
                    "name": name, "address": address, "phone": phone,
                    "website": website, "snippet": snippet[:200], "source": "YellowPages"
                })

            break  # success, exit retry loop

        except requests.exceptions.Timeout:
            if attempt < max_attempts - 1:
                wait = random.uniform(30, 60)
                print(f"     [YP] timeout -- waiting {wait:.0f}s and retrying...")
                time.sleep(wait)
                continue
            else:
                print(f"  [YP] timeout after retry -- giving up")
        except Exception as e:
            if attempt < max_attempts - 1 and "429" in str(e):
                continue  # already handled above
            print(f"  [YP] scrape error: {e}")
            break

    return results


# ==============================================================================
# Homepage Fetch + Manual-Work Signal Detection
# ==============================================================================

# Signals for Dental & Medical
_DENTAL_BOOKING_KEYWORDS = [
    "book online", "schedule online", "book appointment", "online booking",
    "book now", "schedule now", "request appointment", "book your appointment",
    "schedule your", "online scheduling",
]

_LIVE_CHAT_SCRIPTS = [
    "intercom", "drift", "tidio", "livechat", "hubspot",
    "zendesk", "tawk", "crisp", "olark", "freshchat",
]

_OWNER_DENTIST_PATTERNS = [
    r"dr\.\s+\w+",            # "Dr. Smith"
    r"owner[\s-]operator",
    r"locally\s+owned",
    r"family\s+practice",
    r"family[\s-]owned",
    r"our\s+dentist",
    r"your\s+dentist",
    r"meet\s+the\s+doctor",
    r"meet\s+dr\.",
]

# Signals for Trades
_CALL_FOR_QUOTE_KEYWORDS = [
    "call for quote", "call for estimate", "call for a free",
    "call today for", "call us for", "call now for",
    "phone for a quote", "give us a call",
]

_OWNER_OPERATOR_KEYWORDS = [
    "family-owned", "family owned", "owner-operated", "owner operated",
    "locally owned", "locally-owned", "i've been serving",
    "my team and i", "our family business", "family run",
    "family-run", "we are a family",
]

# Slow-response keywords for Places API review snippets
_SLOW_RESPONSE_KEYWORDS = [
    "hard to reach", "couldn't get through", "could not get through",
    "no answer", "called several times", "never answered",
    "slow to respond", "didn't return my call", "didn't call back",
    "hard to get a hold", "hard to contact", "unreachable",
    "left multiple messages", "never got back",
]


def _fetch_homepage(website_url):
    """
    Fetch homepage HTML with 7s timeout. Returns (html_text, True) on success,
    ("", False) on failure.
    """
    if not website_url:
        return "", False
    # Normalize
    if not website_url.startswith("http"):
        website_url = "https://" + website_url
    try:
        r = requests.get(website_url, headers=HEADERS, timeout=7, allow_redirects=True)
        r.raise_for_status()
        return r.text, True
    except Exception as e:
        print(f"     [HP] fetch failed ({website_url[:50]}): {e}")
        return "", False


def _check_places_reviews(business_name):
    """
    Check Google Places API review snippets for slow-response signals.
    Returns (has_slow_response: bool, review_texts: list[str]).
    Skips silently if API key is not configured.
    """
    if not GOOGLE_PLACES_API_KEY:
        return False, []

    try:
        # Step 1: Text Search -> place_id
        search_resp = requests.post(
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": "places.id",
            },
            json={"textQuery": f"{business_name} Ontario Canada"},
            timeout=10,
        )
        if search_resp.status_code != 200:
            print(f"     [Places] search failed: {search_resp.status_code}")
            return False, []

        places = search_resp.json().get("places", [])
        if not places:
            return False, []
        place_id = places[0].get("id", "")
        if not place_id:
            return False, []

        # Step 2: Place Details with reviews
        detail_resp = requests.get(
            f"https://places.googleapis.com/v1/places/{place_id}",
            headers={
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": "reviews.text",
            },
            timeout=10,
        )
        if detail_resp.status_code != 200:
            return False, []

        reviews = detail_resp.json().get("reviews", [])
        review_texts = []
        for rev in reviews:
            text_obj = rev.get("text", {})
            text = text_obj.get("text", "") if isinstance(text_obj, dict) else str(text_obj)
            if text:
                review_texts.append(text)

        # Check for slow-response keywords
        all_review_text = " ".join(review_texts).lower()
        for kw in _SLOW_RESPONSE_KEYWORDS:
            if kw in all_review_text:
                return True, review_texts

        return False, review_texts

    except Exception as e:
        print(f"     [Places] review check error: {e}")
        return False, []


def compute_manual_work_score(vertical, html_text, homepage_fetched, business_name):
    """
    Compute a 0-10 manual work score based on homepage signals + Places API reviews.

    Returns (score, priority, signal_string).
    - priority: 'high' (6+), 'medium' (3-5), or 'skip' (0-2)
    - signal_string: <100 char human-readable description of top signals
    """
    score = 0
    signals = []  # (points, description)

    html_lower = html_text.lower() if html_text else ""

    if vertical == "Dental & Medical":
        # -- No online booking link (+3) --
        has_booking = False
        for kw in _DENTAL_BOOKING_KEYWORDS:
            if kw in html_lower:
                has_booking = True
                break
        if not has_booking and homepage_fetched:
            score += 3
            signals.append((3, "no online booking system found"))

        # -- Contact form to generic mailer (+2) --
        if homepage_fetched and html_text:
            soup = BeautifulSoup(html_text, "lxml")
            forms = soup.select("form")
            has_generic_form = False
            for form in forms:
                action = (form.get("action") or "").lower()
                form_text = form.get_text(" ", strip=True).lower()
                # Generic contact form indicators (not booking)
                if ("contact" in form_text or "message" in form_text or
                    "mailto:" in action or "formspree" in action or
                    "getform" in action or "netlify" in action):
                    if not any(bk in form_text for bk in ["book", "schedule", "appointment"]):
                        has_generic_form = True
                        break
            if has_generic_form:
                score += 2
                signals.append((2, "contact form without booking integration"))

        # -- No live chat widget (+1) --
        has_chat = False
        for script_kw in _LIVE_CHAT_SCRIPTS:
            if script_kw in html_lower:
                has_chat = True
                break
        if not has_chat and homepage_fetched:
            score += 1
            signals.append((1, "no live chat widget"))

        # -- Outdated site design (+1) --
        if homepage_fetched and html_text:
            has_viewport = 'name="viewport"' in html_lower or "name='viewport'" in html_lower
            has_doctype = html_lower.strip().startswith("<!doctype html")
            # Check for excessive inline styles
            inline_count = html_lower.count('style="')
            is_outdated = (not has_viewport) or (not has_doctype) or (inline_count > 20)
            if is_outdated:
                score += 1
                signals.append((1, "outdated site design"))

        # -- Single location (+1) --
        if homepage_fetched and html_text:
            # Count distinct street address patterns
            addr_matches = re.findall(
                r'\d+\s+[\w\s]+(?:st|ave|rd|dr|blvd|cres|way|ct|lane|pkwy|hwy)',
                html_lower
            )
            unique_addrs = set()
            for a in addr_matches:
                # Normalize for comparison
                norm = re.sub(r'\s+', ' ', a.strip())
                unique_addrs.add(norm)
            if len(unique_addrs) <= 1:
                score += 1
                signals.append((1, "single location"))

        # -- Owner-dentist language (+2) --
        for pattern in _OWNER_DENTIST_PATTERNS:
            if re.search(pattern, html_lower, re.I):
                score += 2
                signals.append((2, "owner-dentist language on site"))
                break

    elif vertical == "Trades":
        # -- "Call for quote" language (+3) --
        has_call_cta = False
        for kw in _CALL_FOR_QUOTE_KEYWORDS:
            if kw in html_lower:
                has_call_cta = True
                break
        # Also check if primary CTA is just a phone number (no form)
        if not has_call_cta and homepage_fetched and html_text:
            soup = BeautifulSoup(html_text, "lxml")
            # Check for prominent tel: links without any quote forms
            tel_links = soup.select("a[href^='tel:']")
            quote_forms = soup.select("form")
            has_quote_form = False
            for f in quote_forms:
                ft = f.get_text(" ", strip=True).lower()
                if any(w in ft for w in ["quote", "estimate", "book", "schedule"]):
                    has_quote_form = True
                    break
            if tel_links and not has_quote_form:
                has_call_cta = True
        if has_call_cta:
            score += 3
            signals.append((3, "call for quote with no online form"))

        # -- No online quote/booking form (+2) --
        if homepage_fetched and html_text:
            soup = BeautifulSoup(html_text, "lxml")
            forms = soup.select("form")
            has_quote_form = False
            for f in forms:
                ft = f.get_text(" ", strip=True).lower()
                if any(w in ft for w in ["quote", "estimate", "book", "schedule", "request"]):
                    has_quote_form = True
                    break
            if not has_quote_form:
                score += 2
                signals.append((2, "no online quote or booking form"))

        # -- No SMS/auto-response mention (+1) --
        has_sms = any(kw in html_lower for kw in [
            "text us", "we'll text back", "auto-reply", "auto reply",
            "sms", "text message",
        ])
        if not has_sms and homepage_fetched:
            score += 1
            signals.append((1, "no SMS or auto-response"))

        # -- Owner-operator language (+2) --
        for kw in _OWNER_OPERATOR_KEYWORDS:
            if kw in html_lower:
                score += 2
                signals.append((2, "owner-operator language on site"))
                break

        # -- Single location (+1) --
        if homepage_fetched and html_text:
            addr_matches = re.findall(
                r'\d+\s+[\w\s]+(?:st|ave|rd|dr|blvd|cres|way|ct|lane|pkwy|hwy)',
                html_lower
            )
            unique_addrs = set()
            for a in addr_matches:
                norm = re.sub(r'\s+', ' ', a.strip())
                unique_addrs.add(norm)
            if len(unique_addrs) <= 1:
                score += 1
                signals.append((1, "single location"))

    # -- Reviews mention slow response (+1) -- applies to both verticals
    has_slow_response, _ = _check_places_reviews(business_name)
    if has_slow_response:
        score += 1
        signals.append((1, "reviews mention slow response"))

    # Cap at 10
    score = min(score, 10)

    # Priority mapping
    if score >= 6:
        priority = "high"
    elif score >= 3:
        priority = "medium"
    else:
        priority = "skip"

    # Build signal string from top signals (highest points first), max 100 chars
    signals.sort(key=lambda x: x[0], reverse=True)
    if not signals:
        signal_str = "no signals detected"
    else:
        # Take signals starting from highest, join with comma until < 100 chars
        parts = []
        total_len = 0
        for _, desc in signals:
            if total_len + len(desc) + 2 > 100:
                break
            parts.append(desc)
            total_len += len(desc) + 2
        signal_str = ", ".join(parts) if parts else signals[0][1][:100]

    # Default score for homepage fetch failures
    if not homepage_fetched and score == 0:
        score = 3
        priority = "medium"
        signal_str = "homepage unreachable, default score assigned"

    return score, priority, signal_str


# -- Prospect Builder ---------------------------------------------------------

def build_prospect(raw, vertical, area, score, priority, signal):
    """
    Convert raw scraped data into a CRM prospect record matching Supabase schema.

    v3 changes:
    - Writes priority, manual_work_score, manual_work_signal columns
    - Writes to website (not website_url)
    - Does NOT write to opp (no AI gap descriptions at source time)
    - Uses business_name.rstrip('.') to prevent double periods for Inc. businesses
    """
    source = raw.get("source", "Unknown")
    city = area.split(",")[0].strip()
    biz_name = raw["name"].rstrip(".")
    slug = slugify(f"{biz_name}-{city}")

    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": slug,
        "name": biz_name[:100],
        "cat": vertical,
        "status": "NOT CONTACTED",
        "address": raw.get("address", "")[:200] or area,
        "phone": raw.get("phone", ""),
        "email": raw.get("email", ""),
        "website": raw.get("website", ""),
        "owner": raw.get("owner", ""),
        "action": "Research & qualify",
        "priority": priority,
        "manual_work_score": score,
        "manual_work_signal": signal[:100],
        "notes": f"[Auto-sourced {datetime.now().strftime('%Y-%m-%d')} via {source}] score={score} {signal[:80]}",
        "last_contact": None,
        "created_at": now,
    }


# -- Main Agent Logic ---------------------------------------------------------

def run_agent(verticals=None, areas=None, max_per_search=5, dry_run=False):
    """
    Main entry point. Searches YellowPages for leads, scores them,
    writes qualified leads to Supabase, and notifies Franco.

    v3 flow:
    1. Fetch existing prospect IDs from Supabase for dedup
    2. For each active vertical (dental, trades):
         For each YP search term:
           For each area (sampled):
             a. Scrape YellowPages
             b. For each result:
                - Dedup check (slug ID)
                - Chain/size filter
                - Fetch homepage (7s timeout, 3-5s delay)
                - Places API review check (inside compute_manual_work_score)
                - Compute manual-work score
                - If score < 3: skip
                - Build prospect record
                - Append to batch
    3. Insert all leads to Supabase
    4. SMS summary
    """
    verticals = verticals or list(ACTIVE_VERTICALS)
    use_smart_areas = areas is None
    areas = areas or GTA_AREAS_FULL

    circuit_breaker = YPCircuitBreaker()

    print("=" * 60)
    print("  Unify Lead Sourcer Agent v3.0 (YP-only + Manual-Work Scoring)")
    print("=" * 60)
    print(f"  Verticals : {', '.join(verticals)}")
    print(f"  Areas     : {'smart per-vertical' if use_smart_areas else f'{len(areas)} locations'}")
    print(f"  Max/search: {max_per_search}")
    print(f"  Source    : YellowPages.ca only")
    print(f"  Scoring   : manual-work 0-10, skip < 3")
    print(f"  Dry run   : {dry_run}")
    print(f"  Filter    : Chains BLOCKED, owner name NOT required")
    print(f"  Supabase  : {'Connected' if SUPABASE_KEY else 'No key'}")
    print(f"  Twilio    : {'Configured' if TWILIO_SID else 'Skipped'}")
    print(f"  Places API: {'Configured' if GOOGLE_PLACES_API_KEY else 'Skipped (no review check)'}")
    print()

    # Step 1: Get existing prospect IDs for deduplication
    existing_ids = set()
    if not dry_run and SUPABASE_KEY:
        print("Fetching existing prospect IDs for dedup...")
        existing_ids = sb_get_existing_ids()
        print(f"   Found {len(existing_ids)} existing prospects\n")

    # Step 2: YP-only scraping with manual-work scoring
    all_leads = []
    searches_done = 0
    skipped_low_score = 0
    skipped_duplicate = 0
    skipped_chain = 0
    skipped_too_large = 0
    skipped_too_small = 0
    skipped_no_contact = 0
    n_high = 0
    n_medium = 0
    signal_counts = {}  # track most common signals

    for v_name in verticals:
        if v_name not in VERTICALS:
            print(f"  Warning: vertical '{v_name}' not recognized, skipping")
            continue

        if use_smart_areas:
            v_areas = VERTICAL_AREA_MAP.get(v_name, GTA_AREAS_FULL)
        else:
            v_areas = areas

        yp_terms = YP_SEARCH_TERMS.get(v_name, [v_name])
        sample_size = min(3, len(yp_terms))
        chosen_terms = random.sample(yp_terms, sample_size)

        print(f"\n{'#'*60}")
        print(f"# VERTICAL: {v_name} ({len(v_areas)} areas, {len(chosen_terms)} terms)")
        print(f"{'#'*60}")

        for search_term in chosen_terms:
            chosen_areas = random.sample(v_areas, min(5, len(v_areas)))

            for area in chosen_areas:
                # Circuit breaker abort check
                if circuit_breaker.should_abort:
                    print("\n   [ABORT] YellowPages failing -- stopping run")
                    break

                print(f"\n{'='*50}")
                print(f"SEARCH: {search_term} in {area}")
                print(f"{'='*50}")

                raw_results = scrape_yellowpages(
                    search_term, area, max_results=max_per_search
                )
                print(f"\n   YP returned {len(raw_results)} results")

                if len(raw_results) > 0:
                    circuit_breaker.record_success(len(raw_results))
                else:
                    circuit_breaker.record_failure("0 results")

                for raw in raw_results:
                    biz_name = raw["name"].rstrip(".")
                    city = area.split(",")[0].strip()
                    slug = slugify(f"{biz_name}-{city}")

                    # Dedup by slug ID
                    if slug in existing_ids:
                        print(f"   SKIP duplicate: {biz_name} ({slug})")
                        skipped_duplicate += 1
                        continue

                    # Chain filter
                    if is_chain_or_franchise(biz_name):
                        print(f"   SKIP chain: {biz_name}")
                        skipped_chain += 1
                        continue

                    # Target market filters
                    raw_addr = raw.get("address", "")
                    if is_too_large(biz_name, raw_addr):
                        print(f"   SKIP (too large): {biz_name}")
                        skipped_too_large += 1
                        continue
                    if is_too_small(biz_name, raw_addr):
                        print(f"   SKIP (too small): {biz_name}")
                        skipped_too_small += 1
                        continue

                    # v3: no owner-name hard filter, but need phone OR email
                    has_email = bool(raw.get("email", "").strip())
                    has_phone = bool(raw.get("phone", "").strip())
                    has_website = bool(raw.get("website", "").strip())
                    if not (has_email or has_phone or has_website):
                        print(f"   SKIP (no contact info at all): {biz_name}")
                        skipped_no_contact += 1
                        continue

                    # Fetch homepage for scoring
                    print(f"   Scoring: {biz_name}...")
                    hp_html, hp_ok = _fetch_homepage(raw.get("website", ""))

                    # 3-5 second delay between homepage fetches
                    time.sleep(random.uniform(3.0, 5.0))

                    # Compute manual-work score
                    score, priority, signal = compute_manual_work_score(
                        v_name, hp_html, hp_ok, biz_name
                    )
                    print(f"     Score: {score}/10 -> {priority} | {signal}")

                    # Skip low-score prospects
                    if priority == "skip":
                        print(f"   SKIP (score {score} < 3): {biz_name}")
                        skipped_low_score += 1
                        continue

                    # Build and collect prospect
                    prospect = build_prospect(raw, v_name, area, score, priority, signal)
                    all_leads.append(prospect)
                    existing_ids.add(slug)

                    # Track priorities
                    if priority == "high":
                        n_high += 1
                    else:
                        n_medium += 1

                    # Track signal frequencies
                    for part in signal.split(", "):
                        part = part.strip()
                        if part:
                            signal_counts[part] = signal_counts.get(part, 0) + 1

                    # Daily cap: stop sourcing once we hit 20 leads
                    if len(all_leads) >= 20:
                        print(f"\n   DAILY CAP REACHED: {len(all_leads)} leads")
                        break

                searches_done += 1

                if len(all_leads) >= 20 or circuit_breaker.should_abort:
                    break

                # Rate limit between search combos
                delay = random.uniform(3.0, 6.0)
                print(f"   Waiting {delay:.1f}s...\n")
                time.sleep(delay)

            if len(all_leads) >= 20 or circuit_breaker.should_abort:
                break

        if len(all_leads) >= 20 or circuit_breaker.should_abort:
            break

    # Step 3: Summary
    print("\n" + "=" * 60)
    print(f"  Unify Lead Sourcer v3.0 -- Run Complete")
    print(f"  {'='*56}")
    print(f"     Searches run       : {searches_done}")
    print(f"     Total leads found  : {len(all_leads)}")
    print(f"     High priority      : {n_high}")
    print(f"     Medium priority    : {n_medium}")
    print(f"     Skipped (low score): {skipped_low_score}")
    print(f"     Skipped (duplicate): {skipped_duplicate}")
    print(f"     Skipped (chain)    : {skipped_chain}")
    print(f"     Skipped (too large): {skipped_too_large}")
    print(f"     Skipped (too small): {skipped_too_small}")
    print(f"     Skipped (no info)  : {skipped_no_contact}")
    print(f"     Source performance : {circuit_breaker.summary()}")
    print("=" * 60)

    # Most common signal
    top_signal = ""
    if signal_counts:
        top_signal = max(signal_counts, key=signal_counts.get)

    # Print preview
    if all_leads:
        print("\n  Preview of new leads:")
        for i, p in enumerate(all_leads[:15], 1):
            email_flag = "E" if p["email"] else " "
            phone_flag = "P" if p["phone"] else " "
            owner_flag = "O" if p["owner"] else " "
            pri = "H" if p["priority"] == "high" else "M"
            score_val = p.get("manual_work_score", 0)
            print(f"   {i:>2}. [{pri}{score_val:>2}] [{email_flag}{phone_flag}{owner_flag}] {p['name'][:35]:<35} | {p['cat']:<18}")
        if len(all_leads) > 15:
            print(f"   ... and {len(all_leads) - 15} more")

    # Step 4: Write to Supabase
    inserted = 0
    if dry_run:
        print("\n  Dry run -- nothing written to database.")
        with open("leads_preview.json", "w") as f:
            json.dump(all_leads, f, indent=2)
        print("  Preview saved to leads_preview.json")
    elif not SUPABASE_KEY:
        print("\n  No SUPABASE_KEY -- saving to leads_export.json instead")
        with open("leads_export.json", "w") as f:
            json.dump(all_leads, f, indent=2)
    elif all_leads:
        print(f"\n  Writing {len(all_leads)} prospects to Supabase...")
        for i in range(0, len(all_leads), 25):
            batch = all_leads[i:i+25]
            count = sb_insert_prospects(batch)
            inserted += count
            if count:
                print(f"     Batch {i//25 + 1}: {count} inserted")
            else:
                print(f"     Batch {i//25 + 1}: failed")
        print(f"\n  Total inserted: {inserted}/{len(all_leads)}")

    # Step 5: ALWAYS notify Franco via SMS (even if 0 new leads)
    if circuit_breaker.should_abort:
        msg = "Unify sourcer: YellowPages failing, manual check needed."
    elif inserted > 0 or (dry_run and all_leads):
        n = inserted if not dry_run else len(all_leads)
        h = n_high
        m = n_medium
        sig_part = f"Top signal: {top_signal}." if top_signal else ""
        msg = (
            f"Unify sourcer: {n} leads, {h} high / {m} medium priority. "
            f"{sig_part} "
            f"Review at https://unify-crm-coral.vercel.app/"
        ).strip()
    elif all_leads and not dry_run:
        msg = (
            f"Unify sourcer: {len(all_leads)} leads found but insert failed. "
            f"Check GitHub Actions logs."
        )
    elif dry_run:
        msg = None  # Don't SMS on dry run
    else:
        msg = (
            f"Unify sourcer: 0 leads, check logs. "
            f"Review at https://unify-crm-coral.vercel.app/"
        )

    if msg:
        print(f"\n  Notifying Franco...")
        send_sms(msg)

    print("\n  Agent run complete.")


# -- CLI ----------------------------------------------------------------------

def main():
    all_vertical_names = [
        "Dental & Medical", "Trades",
        "Restaurants", "Retail", "Salons & Spas",
        "Professional Services", "Fitness & Wellness",
        "Auto Services", "Cleaning & Property",
    ]
    parser = argparse.ArgumentParser(description="Unify Lead Sourcer Agent v3.0")
    parser.add_argument("--vertical", "-v", nargs="+",
                        choices=all_vertical_names,
                        help="Which verticals to search (default: Dental & Medical, Trades)")
    parser.add_argument("--area", "-a", nargs="+",
                        help="Specific areas to search (default: all 55 GTA areas)")
    parser.add_argument("--max", "-m", type=int, default=5,
                        help="Max results per YP search query (default: 5)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview results without writing to DB or sending SMS")
    args = parser.parse_args()

    run_agent(
        verticals=args.vertical,
        areas=args.area,
        max_per_search=args.max,
        dry_run=args.dry_run,
    )

if __name__ == "__main__":
    main()
