import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

BASE_URL = "https://www.undelucram.ro"
LIST_URL = f"{BASE_URL}/ro/locuri-de-munca"
JOBS_SITEMAP = f"{BASE_URL}/sitemaps/jobs-sitemap-ro-1.xml"
SOURCE = "UNDELUCRAM"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}

_counties = GetCounty()


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
        return "", [], []

    normalized = remove_diacritics(location_text.strip())
    remote = []

    if normalized.lower().startswith("remote"):
        remote = ["remote"]
        city_text = re.sub(r"^remote\s*(\(|-|/)?\s*", "", normalized, flags=re.IGNORECASE).strip(" )-")
    elif normalized.lower().startswith("hybrid"):
        remote = ["remote"]
        city_text = re.sub(r"^hybrid\s*(\(|-|/)?\s*", "", normalized, flags=re.IGNORECASE).strip(" )-")
    else:
        city_text = normalized

    if city_text.lower() in {"remote", "romania"}:
        city_text = ""

    county = _counties.get_county(city_text) if city_text else []
    if not isinstance(county, list):
        county = [county] if county else []

    return city_text, county, remote


def fetch_total_pages():
    try:
        response = requests.get(JOBS_SITEMAP, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            pages = re.findall(r"/ro/locuri-de-munca\?page=(\d+)", response.text)
            total_pages = max((int(page) for page in pages), default=1)
            if total_pages > 1:
                return total_pages

        print(f"Sitemap unavailable for Undelucram ({response.status_code}). Falling back to listing page pagination.")
    except requests.RequestException as error:
        print(f"Failed to fetch Undelucram sitemap: {error}. Falling back to listing page pagination.")

    response = requests.get(LIST_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    pages = []

    for anchor in soup.select('a[href*="/ro/locuri-de-munca?page="]'):
        href = anchor.get("href") or ""
        match = re.search(r"[?&]page=(\d+)", href)
        if match:
            pages.append(int(match.group(1)))

    if pages:
        return max(pages)

    text = soup.get_text(" ", strip=True)
    match = re.search(r"din\s+(\d+)\s+rezultate", text, flags=re.IGNORECASE)
    if match:
        total_results = int(match.group(1))
        return max((total_results + 9) // 10, 1)

    return 1


def parse_page(page_number):
    response = requests.get(
        LIST_URL,
        params={"page": page_number},
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    jobs = []
    for card in soup.select("div.jobs-item"):
        title_tag = card.select_one('a[href*="/ro/locuri-de-munca/"] h4')
        title_anchor = title_tag.find_parent("a") if title_tag else None
        if not title_anchor:
            continue

        job_url = title_anchor.get("href") or ""
        if not re.search(r"/ro/locuri-de-munca/.+/\d+$", job_url):
            continue

        title = title_tag.get_text(" ", strip=True) if title_tag else ""
        company_tag = card.select_one("h5")
        company = company_tag.get_text(" ", strip=True) if company_tag else "Undelucram"

        info_spans = card.select("div.other-info-label span")
        location_text = info_spans[0].get_text(" ", strip=True) if info_spans else ""

        salary_text = ""
        for text_node in card.stripped_strings:
            if any(currency in text_node.lower() for currency in ("lei", "ron", "eur", "€")):
                salary_text = text_node
                break

        city, county, remote = parse_location(location_text)
        jobs.append(
            {
                "job_title": title or job_url.rstrip("/").split("/")[-2].replace("-", " ").title(),
                "job_link": job_url,
                **parse_salary(salary_text),
                "country": "Romania",
                "city": [city] if city else [],
                "county": county,
                "company": company or "Undelucram",
                "source": SOURCE,
                "remote": remote,
            }
        )

    return jobs


def scrape_undelucram():
    total_pages = fetch_total_pages()
    print(f"Total pages: {total_pages}")

    companies = {}
    seen_links = set()

    for page_number in range(1, total_pages + 1):
        print(f"Scraping page {page_number}/{total_pages}...")
        page_jobs = parse_page(page_number)
        added = 0

        for job in page_jobs:
            link = job.get("job_link")
            if not link or link in seen_links:
                continue

            seen_links.add(link)
            company = job["company"]

            if company not in companies:
                companies[company] = {"name": company, "logo": None, "jobs": []}

            companies[company]["jobs"].append(job)
            added += 1

        print(f"Found {added} new jobs on page {page_number}")
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
    companies = scrape_undelucram()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    for company_name, company_data in companies.items():
        if len(company_data["jobs"]) > 1000:
            remove_company(company_name, TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
