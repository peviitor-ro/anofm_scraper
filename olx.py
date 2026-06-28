import time
from concurrent.futures import ThreadPoolExecutor

import requests

from utils import GetCounty, get_token, main, remove_diacritics, remove_company

_counties = GetCounty()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}
BASE_URL = "https://www.olx.ro/api/v1/offers/?offset=0&limit=50&category_id=4&currency=RON&filter_refiners=spell_checker&sl=194568119cax1c7511aa"
SOURCE = "OLX"
MAX_RETRIES = 3
RETRY_DELAY = 5


def parse_salary(job):
    salary = next((param.get("value") for param in job.get("params", []) if param.get("key") == "salary"), None)

    if not salary:
        return {}

    salary_data = {}

    if salary.get("from") is not None:
        salary_data["salary_min"] = int(salary.get("from"))

    if salary.get("to") is not None:
        salary_data["salary_max"] = int(salary.get("to"))

    if salary.get("currency") and (salary.get("from") is not None or salary.get("to") is not None):
        salary_data["salary_currency"] = salary.get("currency")

    return salary_data


def fetch_json(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Request failed for {url[:80]} (attempt {attempt}/{MAX_RETRIES}): {e}")
        except ValueError as e:
            print(f"JSON decode failed for {url[:80]} (attempt {attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return None


def scrape_olx():
    companies = {}
    seen_links = set()

    url = BASE_URL
    data = fetch_json(url)
    if not data:
        print("Failed to fetch initial page. Exiting.")
        return companies

    page_count = 0
    last_url = None

    while data:
        jobs_on_page = data.get("data", [])
        if not jobs_on_page:
            print("No jobs on this page. Done.")
            break

        page_count += 1
        print(f"Processing page {page_count} ({len(jobs_on_page)} jobs)...")

        next_page = None
        try:
            next_page = data.get("links", {}).get("next", {}).get("href")
        except Exception:
            pass

        if not next_page or next_page == last_url:
            print("No next page or circular link detected. Done.")
            break

        for job in jobs_on_page:
            link = job.get("url", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            company = job.get("user", {}).get("company_name") or "olx"

            if company not in companies:
                companies[company] = {
                    "logo": job.get("user", {}).get("logo"),
                    "name": company,
                    "jobs": [],
                }

            location = remove_diacritics(job.get("location", {}).get("city", {}).get("name", ""))
            county = _counties.get_county(location) or []

            obj = {
                "job_title": job.get("title", ""),
                "job_link": link,
                **parse_salary(job),
                "country": "Romania",
                "city": [location] if location else [],
                "county": county if isinstance(county, list) else [county] if county else [],
                "company": company,
                "source": SOURCE,
            }
            companies[company]["jobs"].append(obj)

        last_url = next_page
        time.sleep(1)
        data = fetch_json(next_page)

    print(f"Scraped {sum(len(c['jobs']) for c in companies.values())} jobs from {len(companies)} companies in {page_count} pages")
    return companies


if __name__ == "__main__":
    token = get_token()
    print("Token obtained successfully")

    companies = scrape_olx()

    total_jobs = sum(len(c["jobs"]) for c in companies.values())
    if total_jobs == 0:
        print("No jobs found, exiting.")
        exit(0)

    print(f"Total: {total_jobs} jobs from {len(companies)} companies")

    remove_company("OLX", token)

    def start(jobs):
        main(jobs.get("jobs"), token)
        if jobs.get("logo"):
            requests.post(
                "https://api.peviitor.ro/v3/logo/add/",
                headers={"Content-Type": "application/json"},
                json=[{"id": jobs.get("name"), "logo": jobs.get("logo")}],
                timeout=15,
            )

    MAX_WORKERS = 5
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for company_data in companies.values():
            if company_data["jobs"]:
                executor.submit(start, company_data)
