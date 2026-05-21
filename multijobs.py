import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from utils import get_token, main, remove_company, remove_diacritics

BASE_URL = "https://multijobs.ro"
LIST_URL = f"{BASE_URL}/locuri-de-munca"
SOURCE = "MULTIJOBS"
DETAIL_WORKERS = 8

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}


def parse_salary(text):
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


def parse_location(location_text):
    if not location_text:
        return None

    normalized = remove_diacritics(location_text.strip())

    if "," not in normalized:
        return None

    city_text, county_text = normalized.split(",", 1)
    city_text = city_text.strip()
    county_text = re.sub(r"\s*\+\d+\s*$", "", county_text).strip()

    if not city_text or not county_text:
        return None

    return city_text, [county_text], []


def fetch_html(page, url):
    page.goto(url, wait_until="load", timeout=120000)
    page.wait_for_timeout(1500)
    return page.content()


def session_from_context(context):
    session = requests.Session()
    session.headers.update(HEADERS)

    for cookie in context.cookies():
        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain"),
            path=cookie.get("path"),
        )

    return session


def fetch_detail_html(session, url):
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def build_job_from_listing(session, listing_job):
    detail_html = fetch_detail_html(session, listing_job["job_link"])
    return parse_detail_page(detail_html, listing_job)


def fetch_total_pages(page):
    html = fetch_html(page, f"{LIST_URL}?p=1")
    soup = BeautifulSoup(html, "html.parser")

    pages = []
    for anchor in soup.select('a[href*="/locuri-de-munca?p="]'):
        href = anchor.get("href") or ""
        match = re.search(r"[?&]p=(\d+)", href)
        if match:
            pages.append(int(match.group(1)))

    if pages:
        return max(pages)

    text = soup.get_text(" ", strip=True)
    match = re.search(r"([\d\.]+)\s+de locuri de munca disponibile", remove_diacritics(text.lower()))
    if match:
        total_jobs = int(match.group(1).replace(".", ""))
        return max((total_jobs + 19) // 20, 1)

    return 1


def parse_listing_page(html):
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    seen_links = set()

    for card in soup.select("div.mj-jobs-card-2"):
        title_anchor = card.select_one("a.mj-jobs-card-2-title")
        company_anchor = card.select_one("a.mj-jobs-card-2-company")
        location_anchor = card.select_one("a.mj-jobs-card-2-location")
        type_div = card.select_one("div.mj-jobs-card-2-type")

        if not title_anchor:
            continue

        job_link = urljoin(BASE_URL, title_anchor.get("href") or "")
        if not job_link or job_link in seen_links:
            continue

        seen_links.add(job_link)
        jobs.append(
            {
                "job_title": title_anchor.get_text(" ", strip=True),
                "job_link": job_link,
                "company": company_anchor.get_text(" ", strip=True) if company_anchor else "MultiJobs",
                "location_text": location_anchor.get_text(" ", strip=True) if location_anchor else "",
                "job_type": type_div.get_text(" ", strip=True) if type_div else "",
            }
        )

    return jobs


def parse_detail_page(html, listing_job):
    soup = BeautifulSoup(html, "html.parser")
    parsed_location = parse_location(listing_job.get("location_text", ""))
    if not parsed_location:
        return None

    city, county, remote = parsed_location

    salary_text = ""
    in_salary_section = False
    for marker in soup.stripped_strings:
        normalized_marker = remove_diacritics(marker.lower())
        if "venit net oferit" in normalized_marker:
            in_salary_section = True
            continue

        if in_salary_section and any(currency in normalized_marker for currency in ("lei", "ron", "eur", "€")):
            salary_text = marker
            break

        if in_salary_section and any(section in normalized_marker for section in ("descriere job", "descriere companie", "aplica acum")):
            break

    return {
        "job_title": listing_job["job_title"],
        "job_link": listing_job["job_link"],
        **parse_salary(salary_text),
        "country": "Romania",
        "city": [city] if city else [],
        "county": county,
        "company": remove_diacritics(listing_job["company"]).strip() or "MultiJobs",
        "source": SOURCE,
        "remote": remote,
    }


def scrape_multijobs():
    companies = {}
    seen_links = set()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        total_pages = fetch_total_pages(page)
        print(f"Total pages: {total_pages}")
        session = session_from_context(context)

        for page_number in range(1, total_pages + 1):
            print(f"Scraping page {page_number}/{total_pages}...")
            listing_html = fetch_html(page, f"{LIST_URL}?p={page_number}")
            listing_jobs = parse_listing_page(listing_html)

            if not listing_jobs:
                print(f"No jobs found on page {page_number}. Stopping.")
                break

            pending_jobs = [listing_job for listing_job in listing_jobs if listing_job["job_link"] not in seen_links]
            added = 0

            with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
                future_to_listing = {
                    executor.submit(build_job_from_listing, session, listing_job): listing_job
                    for listing_job in pending_jobs
                }

                for future in as_completed(future_to_listing):
                    listing_job = future_to_listing[future]
                    link = listing_job["job_link"]

                    try:
                        job = future.result()
                    except Exception as error:
                        print(f"Failed to fetch MultiJobs detail page {link}: {error}")
                        continue

                    if not job:
                        continue

                    seen_links.add(link)
                    company = job["company"]
                    if company not in companies:
                        companies[company] = {"name": company, "logo": None, "jobs": []}

                    companies[company]["jobs"].append(job)
                    added += 1

            print(f"Found {added} new jobs on page {page_number}")

        browser.close()

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
    companies = scrape_multijobs()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    for company_name, company_data in companies.items():
        if len(company_data["jobs"]) > 1000:
            remove_company(company_name, TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
