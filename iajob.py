import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from utils import get_token, GetCounty, remove_diacritics, remove_company, publish_jobs

_counties = GetCounty()
API_URL = "https://api.iajob.ro/v4/search"
BASE_URL = "https://www.iajob.ro"
SOURCE = "IAJOB"
PAGE_SIZE = 100
REQUEST_DELAY = 1
MAX_RETRIES = 3
RETRY_DELAY = 5
MAX_WORKERS = 5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}


def to_amount(value):
    if value is None or value == "":
        return None

    if isinstance(value, str):
        cleaned = re.sub(r"[^\d,.-]", "", value)

        if re.fullmatch(r"\d{1,3}([.,]\d{3})+", cleaned):
            cleaned = cleaned.replace(",", "").replace(".", "")
        elif re.fullmatch(r"\d+,\d{1,2}", cleaned):
            cleaned = cleaned.replace(",", ".")

        value = cleaned

    try:
        amount = int(float(value))
    except (TypeError, ValueError):
        return None

    return amount or None


def parse_salary(salary_text):
    if not salary_text:
        return {}

    if isinstance(salary_text, dict):
        currency = (salary_text.get("currency") or "").upper()

        if currency == "LEI":
            currency = "RON"

        if currency not in {"RON", "EUR"}:
            return {}

        salary_data = {}
        amount_from = to_amount(salary_text.get("min"))
        amount_to = to_amount(salary_text.get("max"))

        if amount_from is not None:
            salary_data["salary_min"] = amount_from
        if amount_to is not None:
            salary_data["salary_max"] = amount_to
        if salary_data:
            salary_data["salary_currency"] = currency

        return salary_data

    normalized = remove_diacritics(salary_text or "")
    match = re.search(r"(\d[\d\.]*)\s*-\s*(\d[\d\.]*)\s*(RON|EUR|LEI)", normalized, flags=re.IGNORECASE)
    if match:
        currency = match.group(3).upper()
        if currency == "LEI":
            currency = "RON"
        return {
            "salary_min": to_amount(match.group(1).replace(".", "")),
            "salary_max": to_amount(match.group(2).replace(".", "")),
            "salary_currency": currency,
        }

    single = re.search(r"(\d[\d\.]*)\s*(RON|EUR|LEI)", normalized, flags=re.IGNORECASE)
    if single:
        currency = single.group(2).upper()
        if currency == "LEI":
            currency = "RON"
        return {
            "salary_min": to_amount(single.group(1).replace(".", "")),
            "salary_currency": currency,
        }

    return {}


def fetch_jobs(offset):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                API_URL,
                params={"offset": offset, "limit": PAGE_SIZE},
                headers=HEADERS,
                timeout=30,
            )

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


def scrape_all_jobs():
    companies = {}
    seen_jobs = {}
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
            uuid = job.get("uuid") or job.get("slug_id")
            if uuid and uuid in seen_jobs:
                continue

            company = remove_diacritics((job.get("employer_name") or "").strip())
            title = remove_diacritics((job.get("title") or "").strip())
            slug_id = job.get("slug_id")

            if not company or not title or not slug_id or not uuid:
                continue

            if len(company) <= 2 or company == "-":
                continue

            logo = job.get("employer_logo_url") or job.get("logo_url")

            if company not in companies:
                companies[company] = {"name": company, "logo": logo, "jobs": []}
            elif not companies[company].get("logo") and logo:
                companies[company]["logo"] = logo

            locality_name = remove_diacritics((job.get("locality_name") or "").strip())
            county_name = remove_diacritics((job.get("county_name") or "").strip())

            city = [locality_name] if locality_name else []
            county = [county_name] if county_name else (_counties.get_county(locality_name) or [] if locality_name else [])

            salary_value = job.get("salary") or ""
            remote = []
            job_type = remove_diacritics((job.get("job_type") or "").strip()).lower()
            if "remote" in job_type:
                remote.append("remote")
            elif "hibrid" in job_type or "hybrid" in job_type:
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
                "source": SOURCE,
                "remote": remote,
            }

            companies[company]["jobs"].append(job_data)
            seen_jobs[uuid] = True
            new_jobs_on_page += 1

        if new_jobs_on_page == 0:
            print(f"No new jobs found at offset {offset}. Stopping.")
            break

        print(f"Sleeping {REQUEST_DELAY} second(s) before next iajob batch...")
        time.sleep(REQUEST_DELAY)
        offset += PAGE_SIZE

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

    remove_company("IAJOB", token)
    print("Company IAJOB reset")

    companies = scrape_all_jobs()

    total_jobs = sum(len(c["jobs"]) for c in companies.values())
    print(f"Total jobs parsed: {total_jobs} from {len(companies)} companies")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(start, company_jobs["jobs"], token): company_jobs["name"]
            for company_jobs in companies.values()
            if company_jobs["jobs"]
        }

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Failed to publish for {futures[future]}: {str(exc)[:200]}")
