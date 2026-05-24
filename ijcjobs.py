import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_diacritics

API_URL = "https://ijcjobs.com/wp-json/wp/v2/job-listings"
SOURCE = "IJCJOBS"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

_counties = GetCounty()


def clean_text(value):
    if not value:
        return ""
    if "<" in value and ">" in value:
        return BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return value.strip()


SALARY_PATTERNS = [
    re.compile(r"(?:de la|intre|din|de|\b)\s*(\d[\d.,]*)\s*(?:[-–]|pana la|si|la)\s*(\d[\d.,]*)\s*(lei|ron|eur|euro)", re.I),
    re.compile(r"(\d[\d.,]*)\s*(?:[-–])\s*(\d[\d.,]*)\s*(lei|ron|eur|euro)", re.I),
    re.compile(r"(\d[\d.,]*)\s*(lei|ron|eur|euro)", re.I),
    re.compile(r"(lei|ron|eur|euro)\s*(\d[\d.,]*)", re.I),
]


def parse_salary(text):
    if not text:
        return {}

    normalized = remove_diacritics(text.lower())
    has_currency = any(kw in normalized for kw in ("lei", "ron", "eur", "euro"))
    if not has_currency:
        return {}

    for pattern in SALARY_PATTERNS:
        match = pattern.search(normalized)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                raw_min, raw_max, curr = groups
                raw_min = raw_min.replace(",", ".").replace(" ", "")
                raw_max = raw_max.replace(",", ".").replace(" ", "")
                num_min = int(float(raw_min)) if "." in raw_min else int(raw_min)
                num_max = int(float(raw_max)) if "." in raw_max else int(raw_max)
                currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(curr.lower(), "RON")
                if num_min > num_max:
                    num_min, num_max = num_max, num_min
                return {
                    "salary_min": num_min,
                    "salary_max": num_max,
                    "salary_currency": currency,
                }
            elif len(groups) == 2:
                if groups[0].isdigit() or groups[0].replace(",", "").replace(".", "").isdigit():
                    raw_num, curr = groups
                else:
                    curr, raw_num = groups
                raw_num = raw_num.replace(",", ".").replace(" ", "")
                num = int(float(raw_num)) if "." in raw_num else int(raw_num)
                currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(curr.lower(), "RON")
                return {
                    "salary_min": num,
                    "salary_max": num,
                    "salary_currency": currency,
                }

    return {}


def parse_job(job):
    content_html = job.get("content", {}).get("rendered", "")
    content_text = clean_text(content_html)
    title = clean_text(job.get("title", {}).get("rendered", ""))
    metas = job.get("meta", {})

    location_raw = (metas.get("_job_location") or "").strip()
    city = remove_diacritics(location_raw) if location_raw else ""
    county = _counties.get_county(city) or []

    remote_position = metas.get("_remote_position", 0)
    remote = []
    if remote_position == 1:
        remote = ["remote"]
    elif not remote:
        normalized_text = remove_diacritics(f"{title} {content_text}".lower())
        if any(keyword in normalized_text for keyword in ("remote", "la distanta", "hibrid", "work from home")):
            remote = ["remote"]

    salary_data = {}
    salary_text = metas.get("_job_salary") or ""
    if salary_text:
        salary_data = parse_salary(salary_text)
    if not salary_data:
        salary_data = parse_salary(content_text)

    return {
        "job_title": title,
        "job_link": job.get("link"),
        **salary_data,
        "country": "Romania",
        "city": [city] if city else [],
        "county": county if isinstance(county, list) else [county] if county else [],
        "company": "IJCJobs",
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


def scrape_ijcjobs():
    companies = {"IJCJobs": {"name": "IJCJobs", "logo": None, "jobs": []}}
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
            link = raw_job.get("link", "")
            if not link or link in seen_links:
                continue

            seen_links.add(link)
            parsed_job = parse_job(raw_job)
            companies["IJCJobs"]["jobs"].append(parsed_job)
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
    companies = scrape_ijcjobs()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    if len(companies["IJCJobs"]["jobs"]) > 1000:
        from utils import remove_company
        remove_company("IJCJobs", TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
