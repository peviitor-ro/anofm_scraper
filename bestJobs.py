import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()

SOURCE = "BESTJOBS"
BASE_URL = "https://www.bestjobs.eu/api/proxy/v2/jobs"
JOB_URL = "https://www.bestjobs.eu/loc-de-munca"
LOGO_URL = "https://imgcdn.bestjobs.eu/cdn/el/plain/employer_logo"
PAGE_LIMIT = 100
LAT = 44.957117
LON = 24.947214
REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 2.5
MAX_FETCH_RETRIES = 3
MAX_WORKERS = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
    "Referer": "https://www.bestjobs.eu/locuri-de-munca",
}

COUNTIES = {
    "alba": "Alba",
    "arad": "Arad",
    "arges": "Arges",
    "bacau": "Bacau",
    "bihor": "Bihor",
    "bistrita-nasaud": "Bistrita-Nasaud",
    "botosani": "Botosani",
    "braila": "Braila",
    "brasov": "Brasov",
    "buzau": "Buzau",
    "calarasi": "Calarasi",
    "caras-severin": "Caras-Severin",
    "cluj": "Cluj",
    "constanta": "Constanta",
    "covasna": "Covasna",
    "dambovita": "Dambovita",
    "dolj": "Dolj",
    "galati": "Galati",
    "giurgiu": "Giurgiu",
    "gorj": "Gorj",
    "harghita": "Harghita",
    "hunedoara": "Hunedoara",
    "ialomita": "Ialomita",
    "iasi": "Iasi",
    "ilfov": "Ilfov",
    "maramures": "Maramures",
    "mehedinti": "Mehedinti",
    "mures": "Mures",
    "neamt": "Neamt",
    "olt": "Olt",
    "prahova": "Prahova",
    "salaj": "Salaj",
    "satu mare": "Satu Mare",
    "sibiu": "Sibiu",
    "suceava": "Suceava",
    "teleorman": "Teleorman",
    "timis": "Timis",
    "tulcea": "Tulcea",
    "valcea": "Valcea",
    "vaslui": "Vaslui",
    "vrancea": "Vrancea",
}

COUNTRY_NAMES = {
    "romania": "Romania",
    "roumanie": "Romania",
    "germania": "Germany",
    "germany": "Germany",
    "danemarca": "Denmark",
    "norvegia": "Norway",
    "olanda": "Netherlands",
    "olanda de nord": "Netherlands",
    "netherlands": "Netherlands",
    "belgia": "Belgium",
    "belgium": "Belgium",
    "austria": "Austria",
    "spania": "Spain",
    "spain": "Spain",
    "france": "France",
    "cechia": "Czechia",
    "czechia": "Czechia",
    "moldavia": "Moldova",
    "moldova": "Moldova",
    "marea britanie": "United Kingdom",
    "marea britanie si irlanda de nord": "United Kingdom",
    "ungaria": "Hungary",
    "italia": "Italy",
    "polonia": "Poland",
    "portugalia": "Portugal",
    "elvetia": "Switzerland",
    "suedia": "Sweden",
    "finlanda": "Finland",
}

CITY_ALIASES = {
    "bucharest": "Bucuresti",
}

REMOTE_LABELS = {
    "remote",
    "de la distanta",
    "telemunca",
    "munca la distanta",
    "work from home",
}

COUNTY_PREFIXES = ("jud", "judet", "judetul", "municipiul", "mun")

STREET_TOKENS = (
    "strada", "str.", "str ", "bulevardul", "bulevard", "bulevard ",
    "soseaua", "sos.", "calea", "aleea", "drumul", "drum ",
    "parc industrial", "parcul industrial", "shopping", "mall", "hala",
    "autostrada", "dna", "comuna", "sat ", "sat.",
)

SECTOR_RE = re.compile(r"sector(ul)?\s*\d+", re.IGNORECASE)


def normalize(value):
    return remove_diacritics((value or "").strip()).lower()


def to_amount(value):
    digits = re.sub(r"[^\d]", "", str(value or ""))
    return int(digits) if digits else None


def parse_salary(job):
    raw = (job.get("salary") or job.get("estimatedSalary") or "").strip()

    if not raw:
        return {}

    amounts = [amount for amount in (to_amount(part) for part in raw.split("-")) if amount is not None]

    if not amounts:
        return {}

    if len(amounts) > 1 and amounts[0] > amounts[-1]:
        amounts = [amounts[-1], amounts[0]]

    salary_data = {"salary_min": amounts[0], "salary_currency": "EUR"}

    if len(amounts) > 1:
        salary_data["salary_max"] = amounts[-1]

    return salary_data


def looks_like_address(value):
    if not value or len(value) > 40:
        return True

    if SECTOR_RE.fullmatch(value):
        return False

    if any(char.isdigit() for char in value):
        return True

    return value.lower().startswith(STREET_TOKENS)


def as_county(value):
    key = normalize(value)

    for prefix in COUNTY_PREFIXES:
        if key.startswith(prefix):
            key = key[len(prefix):].strip(" .:")
            break

    if key.endswith(" county"):
        key = key[:-len(" county")].strip()

    return COUNTIES.get(key)


def as_country(value):
    return COUNTRY_NAMES.get(normalize(value))


def strip_country(value):
    key = normalize(value)

    for name in COUNTRY_NAMES:
        if key.endswith(" " + name):
            return value[:len(value) - len(name) - 1].strip()

    return value


def city_candidates(value):
    city = re.sub(r"^\d{4,6}\s+", "", value or "").strip()
    city = re.sub(r"^(municipiul|mun\.?)\s+", "", city, flags=re.IGNORECASE).strip()
    city = re.sub(r"\s+-\s+[^-]+$", "", city).strip()
    city = re.sub(r"\s+\d{3,6}$", "", city).strip()
    city = remove_diacritics(city)

    variants = []

    for variant in (city, strip_country(city)):
        if SECTOR_RE.fullmatch(variant):
            variants.append("Bucuresti")

        if variant and variant not in variants:
            variants.append(variant)

        words = variant.rsplit(" ", 1)

        if len(words) == 2 and normalize(words[1]) in COUNTIES and words[0]:
            trimmed = words[0].strip()

            if trimmed and trimmed not in variants:
                variants.append(trimmed)

    for variant in list(variants):
        aliased = CITY_ALIASES.get(normalize(variant))

        if aliased and aliased not in variants:
            variants.append(aliased)

    return variants


def is_remote_label(value):
    key = normalize(strip_country(value))
    return key in REMOTE_LABELS or any(key.startswith(label) for label in REMOTE_LABELS)


def match_city(segment):
    for candidate in city_candidates(segment):
        if not candidate or looks_like_address(candidate):
            continue

        matched = _counties.get_county(candidate)

        if matched:
            return candidate, matched

    return None, []


def resolve_location(job):
    cities = {}
    counties = {}
    country = None
    remote = []

    def add_county(name):
        key = normalize(name)

        if key in counties:
            return

        canonical = COUNTIES.get(key)
        counties[key] = canonical or name

    for location in job.get("locations") or []:
        if not isinstance(location, dict):
            continue

        segments = [
            segment.strip()
            for segment in (location.get("name") or "").split(",")
            if segment.strip()
        ]

        if not segments:
            continue

        for segment in segments:
            county = as_county(segment)

            if county:
                add_county(county)

            found = as_country(segment)

            if found and not country:
                country = found

            if is_remote_label(segment) and "remote" not in remote:
                remote.append("remote")

        for segment in segments:
            if as_country(segment) or is_remote_label(segment):
                continue

            city, matched = match_city(segment)

            if not city:
                continue

            key = normalize(city)
            current = cities.get(key)

            if current is None or (current.isupper() and not city.isupper()):
                cities[key] = city

            for county in matched:
                add_county(county)

            break

    return list(cities.values()), list(counties.values()), country or "Romania", remote


def fetch_page(session, cursor):
    url = f"{BASE_URL}?limit={PAGE_LIMIT}&_lat={LAT}&_lon={LON}"

    if cursor:
        url = f"{url}&cursor={cursor}"

    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            payload = response.json()
            return payload.get("items") or [], payload.get("nextCursor")
        except (requests.exceptions.RequestException, ValueError) as exc:
            if attempt == MAX_FETCH_RETRIES:
                print(f"Failed to fetch BestJobs page with cursor {cursor}: {exc}")
                return None, None

            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX) * attempt
            print(f"Request failed for cursor {cursor}: {exc}. Retrying in {delay:.2f} second(s)...")
            time.sleep(delay)

    return None, None


def scrape_jobs():
    session = requests.Session()
    session.headers.update(HEADERS)

    seen_slugs = set()
    scraped = []
    cursor = None
    page = 1

    while page <= 400:
        print(f"Fetching BestJobs page {page} (cursor: {cursor})...")
        items, next_cursor = fetch_page(session, cursor)

        if items is None:
            print("Stopping BestJobs pagination because a page could not be fetched.")
            break

        if not items:
            print(f"No items returned on BestJobs page {page}. Stopping pagination.")
            break

        new_jobs = 0

        for item in items:
            slug = item.get("slug")

            if not slug or slug in seen_slugs:
                continue

            seen_slugs.add(slug)
            scraped.append(item)
            new_jobs += 1

        print(f"BestJobs page {page} added {new_jobs} new jobs. Total: {len(scraped)}")

        if new_jobs == 0 or not next_cursor or next_cursor == cursor:
            print(f"Stopping BestJobs pagination after page {page}.")
            break

        cursor = next_cursor
        page += 1

        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        print(f"Sleeping {delay:.2f} second(s) before next BestJobs request...")
        time.sleep(delay)

    return scraped


def company_logo_url(logo):
    if not logo:
        return None

    logo = str(logo).strip()

    if logo.startswith("http"):
        return logo

    return f"{LOGO_URL}/{logo.lstrip('/')}"


def build_companies(scraped):
    companies = {}

    for job in scraped:
        slug = job.get("slug")
        title = (job.get("title") or "").strip()

        if not slug or not title:
            continue

        company = remove_diacritics((job.get("companyName") or "").strip())

        if not company or len(company) <= 2:
            continue

        cities, counties, country, remote = resolve_location(job)

        if company not in companies:
            companies[company] = {
                "name": company,
                "logo": company_logo_url(job.get("companyLogo")),
                "jobs": [],
            }
        elif not companies[company]["logo"]:
            companies[company]["logo"] = company_logo_url(job.get("companyLogo"))

        companies[company]["jobs"].append({
            "job_title": remove_diacritics(title),
            "job_link": f"{JOB_URL}/{slug}",
            **parse_salary(job),
            "country": country,
            "city": cities,
            "county": counties,
            "company": company,
            "remote": remote,
            "source": SOURCE,
        })

    return companies


def start(company, token):
    jobs = company.get("jobs") or []

    if not jobs:
        return

    print(f"Publishing {len(jobs)} jobs for {company.get('name')}...")
    main(jobs, token)

    if company.get("logo"):
        try:
            requests.post(
                "https://api.peviitor.ro/v3/logo/add/",
                headers={"Content-Type": "application/json"},
                json=[{"id": company.get("name"), "logo": company.get("logo")}],
                timeout=30,
            )
        except requests.RequestException as exc:
            print(f"Failed to upload logo for {company.get('name')}: {exc}")


if __name__ == "__main__":
    scraped_jobs = scrape_jobs()
    print(f"Scraped {len(scraped_jobs)} BestJobs jobs.")

    companies = build_companies(scraped_jobs)

    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Prepared {total_jobs} BestJobs jobs across {len(companies)} companies.")

    token = get_token()

    print(f"Starting BestJobs publish with {MAX_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(start, company, token): company["name"]
            for company in companies.values()
        }

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Failed to publish for {futures[future]}: {str(exc)[:200]}")
