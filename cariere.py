import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

_counties = GetCounty()
BASE_URL = "https://cariere.ro"
START_URL = f"{BASE_URL}/joburi/"
REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 2.5
MAX_FETCH_RETRIES = 3
REQUEST_TIMEOUT = (10, 60)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}
SALARY_RE = re.compile(r"<span>Salariu:\s*(.*?)</span>", re.IGNORECASE | re.DOTALL)


def extract_salary(card):
    for comment in card.find_all(string=lambda value: isinstance(value, Comment)):
        match = SALARY_RE.search(str(comment))
        if match:
            return remove_diacritics(match.group(1).strip())
    return None


def normalize_counties(city_name):
    county_values = _counties.get_county(city_name) or []
    if isinstance(county_values, list):
        return county_values
    return [county_values] if county_values else []


def fetch_partner_page(session, page_url, page):
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            print(f"Fetching cariere partner page {page} (attempt {attempt}/{MAX_FETCH_RETRIES}): {page_url}")
            response = session.get(page_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            if attempt == MAX_FETCH_RETRIES:
                print(f"Failed to fetch cariere partner page {page}: {error}")
                return None

            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX) * attempt
            print(f"Request failed for cariere partner page {page}: {error}. Retrying in {delay:.2f} second(s)...")
            time.sleep(delay)


def parse_partner_card(card, seen_links):
    title_element = card.select_one("h2.loop-item-title > a[href]")
    if not title_element:
        return None

    job_link = title_element.get("href") or ""
    if not job_link:
        return None

    if not job_link.startswith("http"):
        job_link = urljoin(BASE_URL, job_link)

    if job_link in seen_links:
        return None

    title = remove_diacritics(title_element.get_text(" ", strip=True))
    if not title:
        return None

    company_element = card.select_one("p.content-meta > span.job-company")
    company = remove_diacritics(company_element.get_text(" ", strip=True)) if company_element else "Careerjet"
    if not company:
        company = "Careerjet"

    location_element = card.select_one("p.content-meta > span.job-location")
    location_name = remove_diacritics(location_element.get_text(" ", strip=True)).replace("*", "").strip() if location_element else ""

    cities = [location_name] if location_name else []
    counties = normalize_counties(location_name) if location_name else []

    remote = []
    normalized_text = remove_diacritics(card.get_text(" ", strip=True)).lower()
    if "remote" in normalized_text:
        remote = ["remote"]

    job_data = {
        "job_title": title,
        "job_link": job_link,
        "country": "Romania",
        "city": cities,
        "county": counties,
        "company": company,
        "source": "CARIERE",
        "remote": remote,
    }

    date_element = card.select_one("p.content-meta > span.job-date time.entry-date")
    if date_element:
        posted_date = date_element.get("datetime")
        if not posted_date:
            nested_date = date_element.select_one("[itemprop='datePosted']")
            posted_date = nested_date.get_text(" ", strip=True) if nested_date else None
        if posted_date:
            job_data["date"] = posted_date

    salary = extract_salary(card)
    if salary:
        job_data["salary"] = salary

    seen_links.add(job_link)
    return company, job_data


def scrape_cariere_partner_jobs(start_url=START_URL):
    companies = {}
    seen_links = set()
    visited_urls = set()
    page_url = start_url
    page = 1
    session = requests.Session()
    session.headers.update(HEADERS)

    while page_url and page_url not in visited_urls:
        visited_urls.add(page_url)
        print(f"Scraping cariere partner page {page}: {page_url}")

        response = fetch_partner_page(session, page_url, page)
        if response is None:
            print(f"Stopping cariere partner pagination after repeated fetch failures on page {page}.")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        cards = soup.select(
            "h3 + .vc_row .jobs.posts-loop.job-careerjet .posts-loop-content > article.noo_job.job-careerjet-item"
        )
        print(f"Found {len(cards)} partner job cards on page {page}")

        if not cards:
            print(f"No partner cards found on page {page}. Stopping.")
            break

        page_jobs = 0
        for card in cards:
            parsed = parse_partner_card(card, seen_links)
            if not parsed:
                continue

            company, job_data = parsed
            if company not in companies:
                companies[company] = {"name": company, "logo": None, "jobs": []}

            companies[company]["jobs"].append(job_data)
            page_jobs += 1

        print(f"Parsed {page_jobs} new partner jobs on page {page}")

        next_link = soup.select_one(
            ".jobs.posts-loop.job-careerjet > .pagination.list-center > a.next.page-numbers[href*='current_page=']"
        )
        if not next_link:
            break

        next_href = next_link.get("href") or ""
        if not next_href:
            break

        page_url = urljoin(page_url, next_href)
        page += 1

        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        print(f"Sleeping {delay:.2f} second(s) before next cariere partner page...")
        time.sleep(delay)

    return companies


TOKEN = get_token()
companies = scrape_cariere_partner_jobs()


def start(jobs):
    if jobs.get("name") == "Careerjet" and jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        batch_size = 100
        total_batches = (len(all_jobs) + batch_size - 1) // batch_size
        print(f"Processing {len(all_jobs)} jobs in {total_batches} batches for {jobs.get('name')}...")

        for i in range(0, len(all_jobs), batch_size):
            batch = all_jobs[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
            main(batch, TOKEN, user=True)
            time.sleep(2)
    elif jobs.get("jobs"):
        main(jobs.get("jobs"), TOKEN)


total_jobs = sum(len(company["jobs"]) for company in companies.values())
print(f"Total partner jobs parsed: {total_jobs}")

cariere_jobs = len(companies.get("Careerjet", {}).get("jobs", []))
print(f"Careerjet fallback jobs: {cariere_jobs}")

if cariere_jobs > 1000:
    print("Careerjet has more than 1000 jobs. Removing existing company before publish...")
    remove_company("Careerjet", TOKEN)

MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
