import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

from utils import GetCounty, get_token, main

load_dotenv()

_counties = GetCounty()
API_URL = os.getenv("IAJOB_API_URL", "https://api.iajob.ro/v4/search")
BASE_URL = os.getenv("IAJOB_BASE_URL", "https://www.iajob.ro")
LOGO_API_URL = os.getenv("LOGO_API_URL", "https://api.peviitor.ro/v3/logo/add/")
SOURCE = os.getenv("IAJOB_SOURCE", "IAJOB")
COUNTRY = os.getenv("JOB_COUNTRY", "Romania")
PAGE_SIZE = int(os.getenv("IAJOB_PAGE_SIZE", "100"))
REQUEST_DELAY = float(os.getenv("IAJOB_REQUEST_DELAY", "1"))
MAX_RETRIES = int(os.getenv("IAJOB_MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("IAJOB_RETRY_DELAY", "5"))
MAX_WORKERS = int(os.getenv("IAJOB_MAX_WORKERS", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
HEADERS = {
    "User-Agent": os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ),
    "Accept": "application/json",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}


def to_amount(value):
    if value is None or value == "":
        return None

    if isinstance(value, str):
        cleaned = re.sub(r"[^\d,.-]", "", value)
        if not cleaned:
            return None

        if re.fullmatch(r"\d{1,3}([.,]\d{3})+", cleaned):
            cleaned = cleaned.replace(",", "").replace(".", "")
        elif re.fullmatch(r"\d+,\d{1,2}", cleaned):
            cleaned = cleaned.replace(",", ".")

        value = cleaned

    try:
        amount = int(float(value))
    except (TypeError, ValueError):
        return None

    return amount if amount > 0 else None


def parse_salary(salary):
    if not salary:
        return {}

    if isinstance(salary, dict):
        currency = str(salary.get("currency") or "").upper()
        if currency == "LEI":
            currency = "RON"
        if currency not in {"RON", "EUR"}:
            return {}

        salary_data = {}
        amount_from = to_amount(salary.get("min") or salary.get("salary_min") or salary.get("from"))
        amount_to = to_amount(salary.get("max") or salary.get("salary_max") or salary.get("to"))
        if amount_from is not None:
            salary_data["salary_min"] = amount_from
        if amount_to is not None:
            salary_data["salary_max"] = amount_to
        if salary_data:
            salary_data["salary_currency"] = currency
        return salary_data

    normalized = str(salary)
    match = re.search(r"(\d[\d\.]*)\s*-\s*(\d[\d\.]*)\s*(RON|EUR|LEI)", normalized, flags=re.IGNORECASE)
    if match:
        currency = match.group(3).upper()
        if currency == "LEI":
            currency = "RON"
        salary_data = {"salary_currency": currency}
        amount_from = to_amount(match.group(1).replace(".", ""))
        amount_to = to_amount(match.group(2).replace(".", ""))
        if amount_from is not None:
            salary_data["salary_min"] = amount_from
        if amount_to is not None:
            salary_data["salary_max"] = amount_to
        return salary_data if len(salary_data) > 1 else {}

    single = re.search(r"(\d[\d\.]*)\s*(RON|EUR|LEI)", normalized, flags=re.IGNORECASE)
    if single:
        currency = single.group(2).upper()
        if currency == "LEI":
            currency = "RON"
        amount = to_amount(single.group(1).replace(".", ""))
        if amount is not None:
            return {"salary_min": amount, "salary_currency": currency}

    return {}


def fetch_jobs(offset):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                API_URL,
                params={"offset": offset, "limit": PAGE_SIZE},
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(
                    f"{response.status_code} Server Error",
                    response=response,
                )

            response.raise_for_status()
            data = response.json() or {}
            jobs = data.get("jobs")
            if jobs is None:
                jobs = data.get("items") or data.get("data") or []
            return jobs if isinstance(jobs, list) else []
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code != 429 and not (status_code and status_code >= 500):
                print(f"iajob request rejected for offset {offset}: {exc}")
                return None
            print(
                f"iajob request failed for offset {offset} "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
            )
        except (requests.RequestException, ValueError) as exc:
            print(
                f"iajob request error for offset {offset} "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
            )

        if attempt < MAX_RETRIES:
            print(f"Retrying iajob offset {offset} in {RETRY_DELAY} second(s)...")
            time.sleep(RETRY_DELAY)

    return None


def slugify(value):
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return value


def build_job_link(job):
    slug_id = job.get("slug_id") or job.get("uuid") or job.get("id")
    if not slug_id:
        return None

    title_slug = slugify(job.get("title"))
    locality_slug = slugify(job.get("locality_name") or job.get("county_name"))
    parts = [BASE_URL.rstrip("/"), "locuri-de-munca"]

    if title_slug:
        parts.append(f"job-{title_slug}")
    if locality_slug:
        parts.append(locality_slug)

    parts.append(str(slug_id))
    return "/".join(parts)


def scrape_all_jobs():
    companies = {}
    seen_jobs = set()
    offset = 0

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
            uuid = job.get("uuid") or job.get("id") or job.get("slug_id")
            if not uuid or uuid in seen_jobs:
                continue

            company_name = (job.get("employer_name") or job.get("company_name") or "").strip()
            title = (job.get("title") or "").strip()
            if not company_name or not title:
                continue
            if len(company_name) <= 2 or company_name == "-":
                continue

            job_link = build_job_link(job)
            if not job_link:
                continue

            seen_jobs.add(uuid)
            new_jobs_on_page += 1

            logo = job.get("employer_logo_url") or job.get("logo_url")
            company = companies.setdefault(company_name, {"name": company_name, "logo": logo, "jobs": []})
            if not company.get("logo") and logo:
                company["logo"] = logo

            locality_name = (job.get("locality_name") or "").strip()
            county_name = (job.get("county_name") or "").strip()
            city = [locality_name] if locality_name else []
            county = [county_name] if county_name else (_counties.get_county(locality_name) or [] if locality_name else [])

            remote = []
            job_type = str(job.get("job_type") or "").strip().lower()
            if "remote" in job_type:
                remote.append("remote")
            elif "hibrid" in job_type or "hybrid" in job_type:
                remote.append("hybrid")

            company["jobs"].append(
                {
                    "job_title": title,
                    "job_link": job_link,
                    **parse_salary(job.get("salary")),
                    "country": COUNTRY,
                    "city": city,
                    "county": county,
                    "company": company_name,
                    "remote": remote,
                    "source": SOURCE,
                }
            )

        if new_jobs_on_page == 0:
            print(f"No new jobs found at offset {offset}. Stopping.")
            break

        if REQUEST_DELAY > 0:
            print(f"Sleeping {REQUEST_DELAY} second(s) before next iajob batch...")
            time.sleep(REQUEST_DELAY)
        offset += PAGE_SIZE

    total_jobs = sum(len(c["jobs"]) for c in companies.values())
    print(f"Scraped {total_jobs} iajob jobs from {len(companies)} companies.")
    return companies


def publish_company(company, token):
    jobs = company.get("jobs") or []
    if jobs:
        main(jobs, token)

    logo = company.get("logo")
    name = company.get("name")
    if logo and name:
        try:
            requests.post(
                LOGO_API_URL,
                headers={"Content-Type": "application/json"},
                json=[{"id": name, "logo": logo}],
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            print(f"Error publishing logo for {name}: {exc}")


def run():
    companies = scrape_all_jobs()
    total_jobs = sum(len(c.get("jobs") or []) for c in companies.values())
    print(f"Publishing {total_jobs} jobs for {len(companies)} companies...")

    token = get_token()
    if not token:
        print("Failed to obtain token; aborting publish.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(publish_company, company, token) for company in companies.values() if company.get("jobs")]
        for future in as_completed(futures):
            future.result()

    print("iajob scraping finished.")


if __name__ == "__main__":
    run()
