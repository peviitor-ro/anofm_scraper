import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()
API_URL = "https://api.iajob.ro/v3/search"
BASE_URL = "https://www.iajob.ro"
PAGE_SIZE = 20
REQUEST_DELAY = 1
MAX_RETRIES = 3
RETRY_DELAY = 5


def parse_salary(salary_text):
    if not salary_text:
        return {}

    if isinstance(salary_text, dict):
        amount_from = salary_text.get("amount_from")
        amount_to = salary_text.get("amount_to")
        currency = (salary_text.get("currency") or "").upper()

        if currency == "LEI":
            currency = "RON"

        salary_data = {}
        if amount_from is not None:
            salary_data["salary_min"] = int(float(amount_from))
        if amount_to is not None:
            salary_data["salary_max"] = int(float(amount_to))
        if salary_data and currency in {"RON", "EUR"}:
            salary_data["salary_currency"] = currency

        return salary_data

    normalized = remove_diacritics(salary_text or "")
    match = re.search(r"(\d[\d\.]*)\s*-\s*(\d[\d\.]*)\s*(RON|EUR|LEI)", normalized, flags=re.IGNORECASE)
    if match:
        currency = match.group(3).upper()
        if currency == "LEI":
            currency = "RON"
        return {
            "salary_min": int(match.group(1).replace(".", "")),
            "salary_max": int(match.group(2).replace(".", "")),
            "salary_currency": currency,
        }

    single = re.search(r"(\d[\d\.]*)\s*(RON|EUR|LEI)", normalized, flags=re.IGNORECASE)
    if single:
        currency = single.group(2).upper()
        if currency == "LEI":
            currency = "RON"
        return {
            "salary_min": int(single.group(1).replace(".", "")),
            "salary_currency": currency,
        }

    return {}


def fetch_jobs(offset):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params={"offset": offset}, timeout=30)

            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"{response.status_code} Server Error",
                    response=response,
                )

            response.raise_for_status()
            data = response.json() or {}
            return data.get("jobs") or []
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None

            if status_code and status_code < 500:
                raise

            print(
                f"iajob request failed for offset {offset} "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
            )
        except requests.RequestException as exc:
            print(
                f"iajob request error for offset {offset} "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
            )

        if attempt < MAX_RETRIES:
            print(f"Retrying iajob offset {offset} in {RETRY_DELAY} second(s)...")
            time.sleep(RETRY_DELAY)

    return None


companies = {}
offset = 0
seen_jobs = set()

while True:
    print(f"Scraping iajob offset {offset}...")
    jobs = fetch_jobs(offset)
    if jobs is None:
        print(f"Stopping iajob scrape after repeated errors at offset {offset}.")
        break

    print(f"Found {len(jobs)} jobs at offset {offset}")

    if not jobs:
        print(f"No jobs found at offset {offset}. Stopping.")
        break

    new_jobs_on_page = 0
    for job in jobs:
        job_key = job.get("uuid") or job.get("slug_id")
        if job_key in seen_jobs:
            continue

        company = remove_diacritics((job.get("employer_name") or "").strip())
        title = remove_diacritics((job.get("title") or "").strip())
        slug_id = job.get("slug_id")
        locality_name = remove_diacritics((job.get("locality_name") or "").strip())
        county_name = remove_diacritics((job.get("county_name") or "").strip())

        if not company or not title or not slug_id:
            continue

        if company not in companies:
            companies[company] = {"name": company, "logo": None, "jobs": []}

        city = [locality_name] if locality_name else []
        county = [county_name] if county_name else (_counties.get_county(locality_name) or [] if locality_name else [])

        salary_value = job.get("salary") or ""
        remote = []
        job_type = remove_diacritics((job.get("job_type") or "").strip()).lower()
        if "remote" in job_type:
            remote.append("remote")
        elif "hibrid" in job_type:
            remote.append("hybrid")

        job_link = f"{BASE_URL}/locuri-de-munca/{slug_id}"
        job_data = {
            "job_title": title,
            "job_link": job_link,
            **parse_salary(salary_value),
            "country": "Romania",
            "city": city,
            "county": county,
            "company": company,
            "source": "IAJOB",
            "remote": remote,
        }

        posting_date = (job.get("posting_date") or "").strip()
        if posting_date:
            job_data["date"] = posting_date

        companies[company]["jobs"].append(job_data)
        seen_jobs.add(job_key)
        new_jobs_on_page += 1

    if new_jobs_on_page == 0:
        print(f"No new jobs found at offset {offset}. Stopping.")
        break

    print(f"Sleeping {REQUEST_DELAY} second(s) before next iajob batch...")
    time.sleep(REQUEST_DELAY)
    offset += PAGE_SIZE

TOKEN = get_token()


def start(jobs):
    main(jobs.get("jobs"), TOKEN)


MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)

total_jobs = sum(len(company["jobs"]) for company in companies.values())
print(f"Total jobs parsed: {total_jobs}")
