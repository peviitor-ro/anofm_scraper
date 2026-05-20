import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

BASE_URL = "https://dreamjobs.ro"
LIST_URL = f"{BASE_URL}/ro/jobs"
SOURCE = "DREAMJOBS"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}

_counties = GetCounty()


def parse_salary_from_text(text):
    normalized = remove_diacritics((text or "").lower())
    matches = re.findall(r"\d[\d\.\s]*", normalized)
    if not matches:
        return {}

    amounts = [int(match.replace(".", "").replace(" ", "")) for match in matches]
    currency = None

    if "eur" in normalized or "euro" in normalized or "€" in (text or ""):
        currency = "EUR"
    elif "lei" in normalized or "ron" in normalized:
        currency = "RON"

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


def normalize_county(city_text):
    county = _counties.get_county(city_text) if city_text else []
    if isinstance(county, list):
        return county
    return [county] if county else []


def parse_location(location_text):
    if not location_text:
        return "", [], []

    normalized = remove_diacritics(location_text.strip())
    normalized_lower = normalized.lower()
    remote = []

    if "lucru de la distanta" in normalized_lower or normalized_lower == "remote":
        return "", [], ["remote"]

    if normalized_lower.startswith("remote"):
        remote = ["remote"]
        normalized = re.sub(r"^remote\s*(\(|-|/)?\s*", "", normalized, flags=re.IGNORECASE).strip(" )-")
    elif normalized_lower.startswith("hybrid"):
        remote = ["remote"]
        normalized = re.sub(r"^hybrid\s*(\(|-|/)?\s*", "", normalized, flags=re.IGNORECASE).strip(" )-")

    if normalized.lower() in {"remote", "romania"}:
        return "", [], remote

    return normalized, normalize_county(normalized), remote


def company_from_link(job_link):
    path_parts = [part for part in urlparse(job_link).path.split("/") if part]
    if len(path_parts) >= 3 and path_parts[0] == "ro" and path_parts[1] == "job":
        return remove_diacritics(path_parts[2].replace("-", " ")).strip().title()
    return "DreamJobs"


def fetch_listing_page(page_number):
    response = requests.get(LIST_URL, params={"page": page_number}, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def parse_listing_cards(soup):
    cards = []
    seen_links = set()

    for card in soup.select("div.group.relative.flex.flex-col.border.border-dj-grey-light.rounded-big"):
        anchor = card.select_one('a[href^="/ro/job/"]')
        if not anchor:
            continue

        href = anchor.get("href") or ""
        if not href.startswith("/ro/job/"):
            continue

        job_link = urljoin(BASE_URL, href)
        if job_link in seen_links:
            continue

        seen_links.add(job_link)

        title = (anchor.get("aria-label") or "").strip()
        texts = list(card.stripped_strings)
        location_text = ""
        salary_text = ""

        for index, text in enumerate(texts):
            if text.lower() == "nou":
                if index + 1 < len(texts):
                    location_text = texts[index + 1].strip()

            if any(currency in text.lower() for currency in ("lei", "ron", "eur", "€")):
                salary_text = text.strip()

        cards.append(
            {
                "job_title": title,
                "job_link": job_link,
                "location_text": location_text,
                "salary_text": salary_text,
            }
        )

    return cards


def fetch_job_details(job_link):
    try:
        response = requests.get(job_link, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as error:
        print(f"Failed to fetch DreamJobs detail page {job_link}: {error}")
        return {}

    soup = BeautifulSoup(response.text, "html.parser")

    for script in soup.select('script[type="application/ld+json"]'):
        text = script.string or script.get_text(strip=True)
        if not text:
            continue

        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict) and payload.get("@type") == "JobPosting":
            return payload

    return {}


def build_job(listing_job, detail_payload):
    title = (detail_payload.get("title") or listing_job.get("job_title") or "").strip()
    company = detail_payload.get("hiringOrganization", {}).get("name") or company_from_link(listing_job["job_link"])

    address = detail_payload.get("jobLocation", {}).get("address", {})
    city_text = address.get("addressLocality") or listing_job.get("location_text") or ""
    city, county, remote = parse_location(city_text)

    salary = detail_payload.get("baseSalary") or {}
    salary_text = listing_job.get("salary_text") or ""
    salary_data = {}

    if isinstance(salary, dict):
        value = salary.get("value") or {}
        salary_currency = salary.get("currency") or salary.get("currencyCode")
        salary_min = value.get("minValue")
        salary_max = value.get("maxValue")

        if salary_currency and (salary_min is not None or salary_max is not None):
            salary_data = {
                **({"salary_min": int(float(salary_min))} if salary_min is not None else {}),
                **({"salary_max": int(float(salary_max))} if salary_max is not None else {}),
                "salary_currency": salary_currency,
            }

    if not salary_data:
        salary_data = parse_salary_from_text(salary_text)

    description = remove_diacritics(BeautifulSoup(detail_payload.get("description") or "", "html.parser").get_text(" ", strip=True).lower())
    if not remote and any(keyword in description for keyword in ("remote", "la distanta", "hibrid", "work from home")):
        remote = ["remote"]

    return {
        "job_title": title,
        "job_link": listing_job["job_link"],
        **salary_data,
        "country": "Romania",
        "city": [city] if city else [],
        "county": county,
        "company": remove_diacritics(company).strip() or "DreamJobs",
        "source": SOURCE,
        "remote": remote,
    }


def scrape_dreamjobs():
    companies = {}
    seen_links = set()
    page_number = 1

    while True:
        print(f"Scraping page {page_number}...")
        soup = fetch_listing_page(page_number)
        listing_jobs = parse_listing_cards(soup)

        if not listing_jobs:
            print(f"No jobs found on page {page_number}. Stopping.")
            break

        added = 0

        for listing_job in listing_jobs:
            link = listing_job["job_link"]
            if link in seen_links:
                continue

            detail_payload = fetch_job_details(link)
            job = build_job(listing_job, detail_payload)

            seen_links.add(link)
            company = job["company"]
            if company not in companies:
                companies[company] = {"name": company, "logo": None, "jobs": []}

            companies[company]["jobs"].append(job)
            added += 1
            time.sleep(0.3)

        print(f"Found {added} new jobs on page {page_number}")
        if added == 0:
            print("No new jobs on this page. Stopping.")
            break

        # DreamJobs pages after the last page expose fewer items and then empty results.
        if len(listing_jobs) < 20:
            break

        page_number += 1
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
    companies = scrape_dreamjobs()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    for company_name, company_data in companies.items():
        if len(company_data["jobs"]) > 1000:
            remove_company(company_name, TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
