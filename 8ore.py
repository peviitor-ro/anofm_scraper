import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()
BASE_URL = "https://8ore.ro"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://8ore.ro/",
    "Connection": "keep-alive",
}


def parse_salary(text):
    normalized = remove_diacritics(text or "")
    match = re.search(r"(\d[\d\.]*)\s*-\s*(\d[\d\.]*)\s*(LEI|RON|EUR)", normalized, flags=re.IGNORECASE)
    if match:
        currency = match.group(3).upper()
        if currency == "LEI":
            currency = "RON"
        return {
            "salary_min": int(match.group(1).replace(".", "")),
            "salary_max": int(match.group(2).replace(".", "")),
            "salary_currency": currency,
        }

    single = re.search(r"(\d[\d\.]*)\s*(LEI|RON|EUR)", normalized, flags=re.IGNORECASE)
    if single:
        currency = single.group(2).upper()
        if currency == "LEI":
            currency = "RON"
        return {
            "salary_min": int(single.group(1).replace(".", "")),
            "salary_currency": currency,
        }

    return {}


companies = {}
seen_links = set()
next_page_url = f"{BASE_URL}/locuri-de-munca"
page = 1

while next_page_url:
    print(f"Scraping 8ore page {page}: {next_page_url}")
    response = requests.get(next_page_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.select("div.jobs-category-grid > div.product-card.clickable-card[data-href]")
    print(f"Found {len(cards)} cards on page {page}")

    if not cards:
        print(f"No cards found on page {page}. Stopping.")
        break

    page_jobs = 0

    for card in cards:
        href = card.get("data-href") or ""
        if not href:
            link_element = card.select_one(".product-button a[href]")
            href = (link_element.get("href") if link_element else "") or ""

        if not href:
            continue

        job_link = urljoin(BASE_URL, href)
        if job_link in seen_links:
            continue

        title_element = card.select_one(".title-wrapper > p")
        company_element = card.select_one(".company-wrapper > p")
        location_element = card.select_one(".job-card-location")
        salary_element = card.select_one(".product-specs > .badge-salary")
        detail_elements = card.select(".product-specs > span:not(.badge-salary)")

        title = remove_diacritics(title_element.get_text(" ", strip=True)) if title_element else ""
        company = remove_diacritics(company_element.get_text(" ", strip=True)) if company_element else "8ORE"
        location_text = remove_diacritics(location_element.get_text(" ", strip=True)) if location_element else ""
        salary_text = remove_diacritics(salary_element.get_text(" ", strip=True)) if salary_element else ""
        details_text = " ".join(remove_diacritics(item.get_text(" ", strip=True)) for item in detail_elements)

        if not title:
            continue

        seen_links.add(job_link)
        page_jobs += 1

        location_parts = [part.strip() for part in location_text.split(",") if part.strip()]
        city = location_parts[0] if location_parts else ""
        county = [location_parts[1]] if len(location_parts) > 1 else (_counties.get_county(city) or [] if city else [])

        remote = []
        details_lower = details_text.lower()
        if "remote" in details_lower:
            remote.append("remote")
        elif "hibrid" in details_lower:
            remote.append("hybrid")

        if company not in companies:
            companies[company] = {"name": company, "logo": None, "jobs": []}

        companies[company]["jobs"].append(
            {
                "job_title": title,
                "job_link": job_link,
                **parse_salary(salary_text),
                "country": "Romania",
                "city": [city] if city else [],
                "county": county,
                "company": company,
                "source": "8ORE",
                "remote": remote,
            }
        )

    if page_jobs == 0:
        print(f"No new jobs parsed on page {page}. Stopping.")
        break

    print(f"Parsed {page_jobs} jobs on page {page}")
    next_link = soup.select_one(".pagination li.next:not(.disabled) a[href]")
    next_page_url = urljoin(BASE_URL, next_link.get("href")) if next_link else None
    page += 1

TOKEN = get_token()


def start(jobs):
    main(jobs.get("jobs"), TOKEN)


MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
