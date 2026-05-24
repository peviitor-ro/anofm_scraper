import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_diacritics

BASE_URL = "https://www.anuntul.ro"
START_URL = f"{BASE_URL}/anunturi-locuri-de-munca/"
SOURCE = "ANUNTUL"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

_counties = GetCounty()

SALARY_RE = re.compile(
    r"(?:de la|intre|din|de|\b)\s*(\d[\d.,]*)\s*(?:[-–]|pana la|si|la)\s*(\d[\d.,]*)\s*(lei|ron|eur|euro|\$|€)",
    re.I,
)
SALARY_SINGLE_RE = re.compile(
    r"(\d[\d.,]*)\s*(lei|ron|eur|euro|\$|€)", re.I
)
CLEAN_RE = re.compile(r"[^\d]")


def parse_salary(text):
    if not text:
        return {}
    normalized = remove_diacritics(text.lower())
    has_currency = any(kw in normalized for kw in ("lei", "ron", "eur", "euro"))
    if not has_currency:
        return {}

    match = SALARY_RE.search(normalized)
    if match:
        raw_min, raw_max, curr = match.groups()
        num_min = int(CLEAN_RE.sub("", raw_min))
        num_max = int(CLEAN_RE.sub("", raw_max))
        if num_min > num_max:
            num_min, num_max = num_max, num_min
        currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(
            curr.lower(), "RON"
        )
        return {"salary_min": num_min, "salary_max": num_max, "salary_currency": currency}

    single = SALARY_SINGLE_RE.search(normalized)
    if single:
        raw_num, curr = single.groups()
        num = int(CLEAN_RE.sub("", raw_num))
        currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(
            curr.lower(), "RON"
        )
        return {"salary_min": num, "salary_max": num, "salary_currency": currency}

    return {}


def parse_location(location_text):
    location_text = (location_text or "").strip()
    if not location_text:
        return "", []

    parts = [p.strip() for p in location_text.split(",")]
    city_raw = remove_diacritics(parts[0]) if parts else ""

    county = []
    if city_raw:
        county = _counties.get_county(city_raw) or []

    return city_raw, county


def parse_card(card):
    link_el = card.select_one("a.stretched-link")
    if not link_el:
        return None

    href = link_el.get("href", "")
    if not href:
        return None

    title = remove_diacritics(link_el.get_text(" ", strip=True))
    if not title:
        return None

    if href.startswith("/"):
        href = urljoin(BASE_URL, href)
    job_link = href.split("#")[0]

    salary_el = card.select_one("div.card-text.fs-5.fw-bold.text-red-at")
    salary_data = parse_salary(salary_el.get_text(" ", strip=True)) if salary_el else {}

    tags_text = card.get_text(" ", strip=True)
    normalized_tags = remove_diacritics(tags_text.lower())

    remote = []
    if "telemunca" in normalized_tags or "remote" in normalized_tags:
        remote = ["remote"]
    elif "hibrid" in normalized_tags:
        remote = ["hybrid"]

    location_el = card.select_one("span.float-end")
    location_text = location_el.get_text(" ", strip=True) if location_el else ""
    city_text = location_text.split(",")[0].strip() if location_text else ""
    city, county = parse_location(city_text)

    company = "Anuntul"

    return {
        "job_title": title,
        "job_link": job_link,
        **salary_data,
        "country": "Romania",
        "city": [city] if city else [],
        "county": county if isinstance(county, list) else [county] if county else [],
        "company": company,
        "source": SOURCE,
        "remote": remote,
    }


def fetch_page(page):
    url = f"{START_URL}?page={page}" if page > 1 else START_URL
    print(f"Fetching page {page}: {url}")
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def has_next_page(soup):
    next_link = soup.select_one('link[rel="next"]')
    return next_link is not None


def scrape_anuntul():
    companies = {"Anuntul": {"name": "Anuntul", "logo": None, "jobs": []}}
    seen_links = set()
    page = 1
    max_pages = None

    while True:
        soup = fetch_page(page)

        if max_pages is None:
            total_el = soup.select_one("div.p-1")
            if total_el:
                total_match = re.search(r"(\d[\d]*)", total_el.get_text(strip=True))
                if total_match:
                    total = int(total_match.group(1))
                    cards_per_page = 16
                    max_pages = (total + cards_per_page - 1) // cards_per_page
                    print(f"Total ads: {total}, estimated pages: ~{max_pages}")

        cards = soup.select("div#lista > div.pb-2.bg-white.itm")
        print(f"Found {len(cards)} job cards on page {page}")

        if not cards:
            print(f"No jobs found on page {page}. Stopping.")
            break

        page_added = 0
        for card in cards:
            parsed = parse_card(card)
            if not parsed:
                continue

            link = parsed.get("job_link")
            if link in seen_links:
                continue
            seen_links.add(link)

            companies["Anuntul"]["jobs"].append(parsed)
            page_added += 1

        print(f"Parsed {page_added} new jobs on page {page}")

        if not has_next_page(soup):
            print("No next page link found. Reached last page.")
            break

        if max_pages and page >= max_pages:
            print(f"Reached max pages ({max_pages}). Stopping.")
            break

        page += 1
        time.sleep(1)

    return companies


TOKEN = get_token()


def start(jobs):
    if jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        if len(all_jobs) > 1000:
            batch_size = 100
            total_batches = (len(all_jobs) + batch_size - 1) // batch_size
            print(f"Processing {len(all_jobs)} jobs in {total_batches} batches...")
            for i in range(0, len(all_jobs), batch_size):
                batch = all_jobs[i:i + batch_size]
                batch_num = i // batch_size + 1
                print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
                main(batch, TOKEN, user=True)
                time.sleep(2)
        else:
            main(all_jobs, TOKEN)


if __name__ == "__main__":
    companies = scrape_anuntul()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    if len(companies["Anuntul"]["jobs"]) > 1000:
        from utils import remove_company
        remove_company("Anuntul", TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
