import random
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

_counties = GetCounty()
BASE_URL = "https://server.mhe.annakannte.tech"
START_URL = f"{BASE_URL}/jobs?radius=1000&country=RO&limit=100&sortBy=created%20desc&app-source=live"
REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 2.5
MAX_FETCH_RETRIES = 3
REQUEST_TIMEOUT = (10, 60)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def fetch_page(session, page_url, page):
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            print(f"Fetching motionheroes page {page} (attempt {attempt}/{MAX_FETCH_RETRIES}): {page_url}")
            response = session.get(page_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            if attempt == MAX_FETCH_RETRIES:
                print(f"Failed to fetch motionheroes page {page}: {error}")
                return None
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX) * attempt
            print(f"Request failed for motionheroes page {page}: {error}. Retrying in {delay:.2f} second(s)...")
            time.sleep(delay)


def normalize_counties(city_name):
    county_values = _counties.get_county(city_name) or []
    if isinstance(county_values, list):
        return county_values
    return [county_values] if county_values else []


def parse_job(job):
    title = remove_diacritics(job.get("title") or "")
    if not title:
        return None

    company = remove_diacritics(job.get("company_textS") or "") or "MOTIONHEROES"
    city = job.get("city_stringS") or ""
    cities = [remove_diacritics(city)] if city else []
    counties = normalize_counties(city) if city else []

    raw = job.get("rawAdditional_binS") or {}
    created = raw.get("created_at") or job.get("changed")
    salary = remove_diacritics(str(raw.get("salary_information"))) if raw.get("salary_information") else None

    unique_reference = raw.get("uniqueReference") or str(job.get("id", "")).split("-")[-1]
    job_data = {
        "job_title": title,
        "job_link": f"https://www.motion-heroes.com/ro/gaseste-ti-jobul/job/{unique_reference}",
        "country": "Romania",
        "city": cities,
        "county": counties,
        "company": company,
        "source": "MOTIONHEROES",
        "remote": [],
    }
    if created:
        job_data["date"] = created
    if salary:
        job_data["salary"] = salary

    return company, job.get("logo_stringS"), job_data


def scrape_jobs(start_url=START_URL):
    companies = {}
    seen_links = set()
    visited_urls = set()
    page_url = start_url
    page = 1
    session = requests.Session()
    session.headers.update(HEADERS)

    while page_url and page_url not in visited_urls:
        visited_urls.add(page_url)
        print(f"Scraping motionheroes page {page}: {page_url}")

        response = fetch_page(session, page_url, page)
        if response is None:
            print(f"Stopping motionheroes pagination after repeated fetch failures on page {page}.")
            break

        try:
            data = response.json()
        except ValueError:
            print(f"Invalid JSON on page {page}. Stopping.")
            break

        jobs = data.get("jobs") or []
        print(f"Found {len(jobs)} jobs on page {page}")

        if not jobs:
            print(f"No jobs found on page {page}. Stopping.")
            break

        page_jobs = 0
        for job in jobs:
            parsed = parse_job(job)
            if not parsed:
                continue
            company, logo, job_data = parsed
            if job_data["job_link"] in seen_links:
                continue
            seen_links.add(job_data["job_link"])

            if company not in companies:
                companies[company] = {"name": company, "logo": logo, "jobs": []}
            if not companies[company].get("logo") and logo:
                companies[company]["logo"] = logo
            companies[company]["jobs"].append(job_data)
            page_jobs += 1

        print(f"Parsed {page_jobs} new jobs on page {page}")

        pagination = data.get("pagination") or {}
        total = pagination.get("total", 0)
        limit = pagination.get("limit", 100)
        if len(seen_links) >= total or len(jobs) < limit:
            break

        page += 1
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        print(f"Sleeping {delay:.2f} second(s) before next motionheroes page...")
        time.sleep(delay)

    return companies


TOKEN = get_token()
companies = scrape_jobs()


def start(jobs):
    if jobs.get("jobs"):
        main(jobs.get("jobs"), TOKEN)


total_jobs = sum(len(company["jobs"]) for company in companies.values())
print(f"Total motionheroes jobs parsed: {total_jobs}")

for company_name, jobs in companies.items():
    if len(jobs.get("jobs", [])) > 1000:
        print(f"{company_name} has more than 1000 jobs. Removing existing company before publish...")
        remove_company(company_name, TOKEN)

MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
