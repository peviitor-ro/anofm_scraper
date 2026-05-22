import re
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

BASE_URL = "https://oferimdemunca.ro"
LIST_URL = f"{BASE_URL}/job/"
SOURCE = "OFERIMDEMUNCA"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}

_counties = GetCounty()


def parse_salary(text):
    normalized = remove_diacritics((text or "").lower())
    amounts = re.findall(r"[\d]+(?:[\.,\s]*[\d]+)*", normalized)
    if not amounts:
        return {}

    amounts = [int(re.sub(r"[^\d]", "", a)) for a in amounts]
    amounts = [a for a in amounts if a > 0]

    if not amounts:
        return {}

    currency = None
    if "eur" in normalized or "euro" in normalized:
        currency = "EUR"
    elif "lei" in normalized or "ron" in normalized:
        currency = "RON"

    if not currency:
        return {}

    if len(amounts) == 1:
        return {"salary_min": amounts[0], "salary_max": amounts[0], "salary_currency": currency}

    return {"salary_min": amounts[0], "salary_max": amounts[1], "salary_currency": currency}


def parse_location(location_text):
    if not location_text:
        return "", [], []

    remote = []
    normalized = location_text.strip()

    city_text = re.sub(r"\s*-\s*Sector\s+\d+", "", normalized).strip()

    if city_text.lower() in {"remote", "romania", ""}:
        city_text = ""

    county = _counties.get_county(city_text) if city_text else []
    if not isinstance(county, list):
        county = [county] if county else []

    return city_text, county, remote


def fetch_total_pages(session):
    response = session.get(LIST_URL, timeout=120)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    total_text = soup.select_one("span.t-24.fw-normal")
    if total_text:
        match = re.search(r"(\d+)", total_text.get_text())
        if match:
            total = int(match.group(1))
            return max((total + 19) // 20, 1)
    return 1


def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    seen_links = set()

    for card in soup.select("div.bg-white.border-40.box-shadow.mb-3"):
        title_tag = card.select_one("div.t-24.fw-bold a")
        if not title_tag:
            continue

        job_url = title_tag.get("href") or ""
        if not job_url or job_url in seen_links:
            continue

        seen_links.add(job_url)
        job_title = title_tag.get_text(" ", strip=True)

        company_tag = card.select_one('a[href*="/company/"]')
        company_url = company_tag.get("href") if company_tag else ""
        if company_url:
            slug = company_url.rstrip("/").split("/")[-1]
            company = slug.replace("-", " ").title().strip()
        else:
            company = "Oferimdemunca"

        location_text = ""
        salary_text = ""

        for row in card.select("div.row.mt-2.t-14 > div"):
            label = row.select_one("span.fw-bold.text-nowrap")
            if label:
                label_text = label.get_text(" ", strip=True).lower()
                row_text = row.get_text(" ", strip=True)
                value = row_text.replace(label.get_text(" ", strip=True), "", 1).strip()
                if "loca" in label_text:
                    location_text = value
                elif "salariu" in label_text:
                    salary_text = value

        city, county, remote = parse_location(location_text)

        if not remote and "remote" in job_title.lower():
            remote = ["remote"]

        jobs.append(
            {
                "job_title": job_title,
                "job_link": job_url,
                **parse_salary(salary_text),
                "country": "Romania",
                "city": [city] if city else [],
                "county": county,
                "company": company,
                "source": SOURCE,
                "remote": remote,
            }
        )

    return jobs


def scrape_oferimdemunca():
    companies = {}
    seen_links = set()

    with requests.Session() as session:
        session.headers.update(HEADERS)

        total_pages = fetch_total_pages(session)
        print(f"Total pages: {total_pages}")

        for page_number in range(1, total_pages + 1):
            print(f"Scraping page {page_number}/{total_pages}...")
            url = LIST_URL if page_number == 1 else f"{LIST_URL}?job-page={page_number}"
            response = session.get(url, timeout=120)
            response.raise_for_status()
            page_jobs = parse_page(response.text)
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
            if page_number < total_pages:
                import time
                time.sleep(1)

            if page_number == 5:  # Limit to first 5 pages for testing
                print("Reached page limit for testing. Stopping.")
                break

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
                import time
                time.sleep(2)
        else:
            main(all_jobs, TOKEN)


if __name__ == "__main__":
    companies = scrape_oferimdemunca()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    for company_name, company_data in companies.items():
        if len(company_data["jobs"]) > 1000:
            remove_company(company_name, TOKEN)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
