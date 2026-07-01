import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import get_token, GetCounty, remove_diacritics, remove_company, publish_jobs

_counties = GetCounty()

MOLDOVA_CITIES = {
    "chisinau", "chișinău", "bălți", "taraclia", "comrat",
    "sîngerei", "fălești", "glodeni",
}

NON_ROMANIA = {
    "germany", "germania", "deutschland", "uk", "united kingdom",
    "england", "spain", "spania", "italy", "italia", "france",
    "franta", "hungary", "ungaria", "austria",
}

SOURCE = "TARGULDECARIERE"
BASE_URL = "https://www.targuldecariere.ro"


def find_city(location_text):
    city = location_text.strip()
    if not city:
        return ""
    normalized = remove_diacritics(city.lower())
    if normalized in MOLDOVA_CITIES:
        return ""
    return city


def find_county(city_name):
    if not city_name:
        return []
    county = _counties.get_county(city_name) or []
    return county if isinstance(county, list) else [county]


def parse_job_card(card):
    title_el = card.select_one(".job_title a")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    link = title_el.get("href", "")
    if not title or not link:
        return None

    company_img = card.select_one(".job_company img")
    company = company_img.get("alt", "").strip() if company_img else ""

    location_el = card.select_one(".job_location li")
    location_raw = location_el.get_text(strip=True) if location_el else ""
    city = find_city(location_raw)

    country = "Romania"
    if not city:
        normalized = remove_diacritics(location_raw.lower())
        if normalized in MOLDOVA_CITIES:
            return None
        for marker in NON_ROMANIA:
            if marker in normalized:
                country = "unknown"
                break

    county = find_county(city) if city else []

    remote = []
    title_lower = remove_diacritics(title.lower())
    if "remote" in title_lower:
        remote = ["remote"]

    return {
        "job_title": title,
        "job_link": link,
        "country": country,
        "city": [city] if city else [],
        "county": county,
        "company": company,
        "source": SOURCE,
        "remote": remote,
    }


def scrape_page(page):
    url = f"{BASE_URL}/cariere?page={page}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch page {page}: {e}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    cards = soup.select(".job_card")
    jobs = []
    for card in cards:
        job = parse_job_card(card)
        if job:
            jobs.append(job)
    return jobs


def scrape_all():
    companies = {}
    seen_links = set()

    for page in range(1, 32):
        print(f"Scraping page {page}/31...")
        jobs = scrape_page(page)
        if not jobs:
            print(f"No jobs on page {page}, stopping.")
            break

        print(f"Found {len(jobs)} jobs on page {page}")
        for job in jobs:
            link = job.get("job_link", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            company_name = job.get("company", "") or "TargulDeCariere"
            if company_name not in companies:
                companies[company_name] = {"name": company_name, "logo": None, "jobs": []}
            companies[company_name]["jobs"].append(job)

        time.sleep(1)

    return companies


def start(company_jobs, token):
    result = publish_jobs(company_jobs, token, user=True)
    if isinstance(result, list):
        print(f"Published {len(result)} jobs for {company_jobs[0].get('company')}")
    else:
        print(f"Failed to publish for {company_jobs[0].get('company')}: {str(result)[:200]}")


if __name__ == "__main__":
    token = get_token()
    print("Token obtained successfully")

    companies = scrape_all()

    total_jobs = sum(len(c["jobs"]) for c in companies.values())
    print(f"Total jobs: {total_jobs} from {len(companies)} companies")

    if total_jobs == 0:
        print("No jobs found, exiting.")
        exit(0)

    MAX_WORKERS = 5
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for company_jobs in companies.values():
            if company_jobs["jobs"]:
                executor.submit(start, company_jobs["jobs"], token)
