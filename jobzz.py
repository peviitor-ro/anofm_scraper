import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

BASE_URL = "https://jobzz.ro"
START_URL = f"{BASE_URL}/locuri-de-munca-in-romania.html"
SOURCE = "JOBZZ"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

_counties = GetCounty()

COMPANY_RE = re.compile(
    r"(SC\s+.+?(?:SRL|SA)|S\.?C\.?.+?(?:SRL|SA)|.+?(?:SRL|SA))",
    re.I,
)


def extract_company(title):
    match = COMPANY_RE.search(title)
    if match:
        return match.group(1).strip().rstrip(",")
    return ""


def parse_relative_date(text):
    text = (text or "").strip().lower()
    if "azi" in text:
        return ""
    if "acum" in text:
        if "minut" in text:
            return ""
        if "ora" in text:
            return ""
        if "zi" in text:
            match = re.search(r"acum\s+(\d+)\s+", text)
            if match:
                days = int(match.group(1))
                return ""
    if "ieri" in text:
        return ""
    return ""


def parse_salary(text):
    text = (text or "").strip().lower()
    if "confiden" in text or "negot" in text or "nepecificat" in text:
        return {}
    match = re.search(r"(\d[\d.]*)\s*(?:[-–]|la|\s)\s*(\d[\d.]*)\s*(lei|ron|eur|euro)", text, re.I)
    if match:
        raw_min, raw_max, curr = match.groups()
        num_min = int(raw_min.replace(".", ""))
        num_max = int(raw_max.replace(".", ""))
        if num_min > num_max:
            num_min, num_max = num_max, num_min
        currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(curr.lower(), "RON")
        return {"salary_min": num_min, "salary_max": num_max, "salary_currency": currency}
    single = re.search(r"(\d[\d.]*)\s*(lei|ron|eur|euro)", text, re.I)
    if single:
        raw_num, curr = single.groups()
        num = int(raw_num.replace(".", ""))
        currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(curr.lower(), "RON")
        return {"salary_min": num, "salary_max": num, "salary_currency": currency}
    return {}


def parse_location(location_text):
    location_text = (location_text or "").strip()
    if not location_text:
        return "", []

    parts = [p.strip() for p in location_text.split(",")]
    city = parts[0] if parts else ""

    county = []
    if city:
        county = _counties.get_county(city) or []

    return city, county


def scrape_page(page_url):
    print(f"Fetching: {page_url}")
    response = requests.get(page_url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    items = soup.select('a.main_items[href*="-anunt_"]')
    page_jobs = []

    for item in items:
        link = item.get("href", "")
        if not link or not link.startswith("http"):
            continue

        title_el = item.select_one("span.title")
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)

        info_details = item.select_one("div.info_details")
        info_text = info_details.get_text(" ", strip=True) if info_details else ""

        salary_el = item.select_one("span.price")
        salary_raw = salary_el.get_text(" ", strip=True) if salary_el else ""
        salary_data = parse_salary(salary_raw)

        location_el = item.select_one("span.location")
        location_raw = location_el.get_text(" ", strip=True) if location_el else ""
        city, county = parse_location(location_raw)

        date_el = item.select_one("span.date")
        date_raw = date_el.get_text(" ", strip=True) if date_el else ""

        remote = ["remote"] if "remote" in title.lower() else []

        company = extract_company(title)
        if not company:
            company = "JobZZ"

        page_jobs.append((company, {
            "job_title": title,
            "job_link": link,
            **salary_data,
            "country": "Romania",
            "city": [city] if city else [],
            "county": county,
            "company": company,
            "source": SOURCE,
            "remote": remote,
        }))

    return page_jobs


def scrape_jobzz():
    companies = {}
    seen_links = set()
    total_pages = 5

    for page in range(1, total_pages + 1):
        if page == 1:
            url = START_URL
        else:
            url = f"{BASE_URL}/locuri-de-munca-in-romania_{page}.html"

        page_jobs = scrape_page(url)
        if not page_jobs:
            print(f"No jobs found on page {page}, stopping")
            break

        print(f"Page {page}: {len(page_jobs)} jobs")

        for company_name, job_data in page_jobs:
            link = job_data.get("job_link", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            if company_name not in companies:
                companies[company_name] = {"name": company_name, "logo": None, "jobs": []}
            companies[company_name]["jobs"].append(job_data)

    return companies


TOKEN = get_token()


def start(jobs):
    if jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        if len(all_jobs) > 1000:
            batch_size = 100
            total_batches = (len(all_jobs) + batch_size - 1) // batch_size
            print(f"Processing {len(all_jobs)} jobs in {total_batches} batches...")
            remove_company(jobs.get("name"), TOKEN)
            for i in range(0, len(all_jobs), batch_size):
                batch = all_jobs[i:i + batch_size]
                batch_num = i // batch_size + 1
                print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
                main(batch, TOKEN, user=True)
                time.sleep(2)
        else:
            main(all_jobs, TOKEN)


if __name__ == "__main__":
    companies = scrape_jobzz()

    print(f"\nTotal companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    for company_name, data in companies.items():
        if len(data["jobs"]) > 1000:
            print(f"{company_name}: {len(data['jobs'])} jobs (will remove company)")
        else:
            print(f"{company_name}: {len(data['jobs'])} jobs")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
