import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

API_URL = "https://joboreca.ro/wp-json/wp/v2/job_listing"
SOURCE = "JOBORECA"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

_counties = GetCounty()


def clean_text(value):
    return BeautifulSoup(value or "", "html.parser").get_text(" ", strip=True)


def parse_salary(text):
    normalized = remove_diacritics((text or "").lower())
    matches = re.findall(r"(\d[\d\.]*)", normalized)

    if not matches:
        return {}

    amounts = [int(match.replace(".", "")) for match in matches]
    currency = None

    if "lei" in normalized or "ron" in normalized:
        currency = "RON"
    elif "eur" in normalized or "euro" in normalized or "€" in text:
        currency = "EUR"

    if not currency:
        return {}

    if len(amounts) == 1:
        return {
            "salary_min": amounts[0],
            "salary_max": amounts[0],
            "salary_currency": currency,
        }

    return {
        "salary_min": amounts[0],
        "salary_max": amounts[1],
        "salary_currency": currency,
    }


def extract_title(raw_title, content_text, content_html):
    title = clean_text(raw_title)
    if title:
        return title

    soup = BeautifulSoup(content_html or "", "html.parser")
    first_paragraph = soup.find("p")
    first_text = first_paragraph.get_text(" ", strip=True) if first_paragraph else ""
    if first_text:
        return first_text

    return content_text or "JobOReca"


def extract_city_and_county(metas):
    city_raw = metas.get("custom-select-23500585") or ""
    county_values = metas.get("_job_location") or {}
    county_raw = next(iter(county_values.values()), "") if isinstance(county_values, dict) else ""

    city = remove_diacritics(city_raw).strip() if city_raw else ""
    county = []

    if city_raw:
        county = _counties.get_county(city_raw) or []
    if not county and county_raw:
        county = [remove_diacritics(county_raw).strip()]
    elif not isinstance(county, list):
        county = [county]

    return city, county


def parse_job(job):
    content_html = job.get("content", {}).get("rendered", "")
    content_text = clean_text(content_html)
    title = extract_title(job.get("title", {}).get("rendered", ""), content_text, content_html)
    metas = job.get("metas", {})
    city, county = extract_city_and_county(metas)

    remote = []
    normalized_text = remove_diacritics(f"{title} {content_text}".lower())
    if any(keyword in normalized_text for keyword in ("remote", "la distanta", "hibrid", "work from home")):
        remote = ["remote"]

    salary_text = metas.get("_job_salary") or content_text

    return {
        "job_title": title,
        "job_link": job.get("link"),
        **parse_salary(salary_text),
        "country": "Romania",
        "city": [city] if city else [],
        "county": county,
        "company": "JobOReca",
        "source": SOURCE,
        "remote": remote,
    }


def fetch_page(page, per_page=100):
    response = requests.get(
        API_URL,
        params={
            "per_page": per_page,
            "page": page,
            "status": "publish",
            "orderby": "date",
            "order": "desc",
        },
        headers=HEADERS,
        timeout=30,
    )

    if response.status_code == 400:
        return [], 0

    response.raise_for_status()
    total_pages = int(response.headers.get("X-WP-TotalPages", 1))
    return response.json(), total_pages


def scrape_joboreca():
    companies = {"JobOReca": {"name": "JobOReca", "logo": None, "jobs": []}}
    page = 1
    total_pages = None
    seen_links = set()

    while True:
        print(f"Scraping page {page}...")
        jobs, fetched_total_pages = fetch_page(page)

        if total_pages is None:
            total_pages = fetched_total_pages
            print(f"Total pages: {total_pages}")

        if not jobs:
            break

        page_added = 0
        for raw_job in jobs:
            parsed_job = parse_job(raw_job)
            link = parsed_job.get("job_link")

            if not link or link in seen_links:
                continue

            seen_links.add(link)
            companies["JobOReca"]["jobs"].append(parsed_job)
            page_added += 1

        print(f"Found {page_added} new jobs on page {page}")

        if page >= total_pages:
            break

        page += 1
        time.sleep(1)

    return companies


TOKEN = get_token()


def start(jobs):
    if jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        batch_size = 100
        total_batches = (len(all_jobs) + batch_size - 1) // batch_size

        print(f"Processing {len(all_jobs)} jobs in {total_batches} batches...")

        for i in range(0, len(all_jobs), batch_size):
            batch = all_jobs[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
            main(batch, TOKEN, user=True)
            time.sleep(2)


if __name__ == "__main__":
    companies = scrape_joboreca()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    if len(companies["JobOReca"]["jobs"]) > 1000:
        remove_company("JobOReca", TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
