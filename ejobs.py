import os
import re
from concurrent.futures import ThreadPoolExecutor

import requests
from dotenv import load_dotenv

from citieseJobs import cities
from utils import GetCounty, get_token, main

load_dotenv()

EJOBS_API_URL = os.getenv("EJOBS_API_URL", "https://api.ejobs.ro/jobs")
EJOBS_PAGE_SIZE = int(os.getenv("EJOBS_PAGE_SIZE", "100"))
LOGO_API_URL = os.getenv("LOGO_API_URL", "https://api.peviitor.ro/v3/logo/add/")
MAX_WORKERS = int(os.getenv("EJOBS_MAX_WORKERS", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
SOURCE = os.getenv("EJOBS_SOURCE", "EJOBS")
COUNTRY = os.getenv("JOB_COUNTRY", "Romania")

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    ),
}

_counties = GetCounty()


def parse_salary(salary):
    if not salary:
        return {}

    parts = str(salary).split()
    if len(parts) < 2:
        return {}

    salary_data = {}
    values = " ".join(parts[:-1])
    currency = parts[-1].upper()

    def extract_amount(value):
        match = re.search(r"\d[\d\.,]*", value or "")
        if not match:
            return None
        return int(match.group(0).replace(".", "").replace(",", ""))

    if " - " in values:
        salary_min, salary_max = values.split(" - ", 1)
        parsed_min = extract_amount(salary_min)
        parsed_max = extract_amount(salary_max)
        if parsed_min is not None:
            salary_data["salary_min"] = parsed_min
        if parsed_max is not None:
            salary_data["salary_max"] = parsed_max
    else:
        parsed_value = extract_amount(values)
        if parsed_value is not None:
            salary_data["salary_min"] = parsed_value

    if salary_data:
        salary_data["salary_currency"] = currency

    return salary_data


def fetch_page(page):
    response = requests.get(
        EJOBS_API_URL,
        params={"pageSize": EJOBS_PAGE_SIZE, "page": page},
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json() or {}
    return data.get("jobs") or []


def build_company_logo(logo_url):
    if not logo_url:
        return None
    if str(logo_url).startswith("http://") or str(logo_url).startswith("https://"):
        return logo_url
    return f"https://content.ejobs.ro/{str(logo_url).lstrip('/')}"


def scrape_jobs():
    companies = {}
    page = 1

    while True:
        try:
            jobs_page = fetch_page(page)
        except (requests.RequestException, ValueError) as exc:
            print(f"Error fetching eJobs page {page}: {exc}")
            break

        if not jobs_page:
            break

        for job in jobs_page:
            company = job.get("company") or {}
            company_id = company.get("id")
            if company_id is None:
                continue

            if company_id not in companies:
                companies[company_id] = {
                    "name": company.get("name"),
                    "logo": build_company_logo(company.get("logoUrl")),
                    "jobs": [],
                }

            try:
                location = [
                    cities.get(str(city.get("cityId")))
                    for city in (job.get("locations") or [])
                    if str(city.get("cityId")) in cities
                ]
                location = [city for city in location if city]
            except Exception:
                location = []

            counties = []
            for city in location:
                county = _counties.get_county(city)
                if county:
                    counties.extend(county)

            remote = []
            if any(str(city).lower() == "remote" for city in location):
                remote.append("remote")

            companies[company_id]["jobs"].append(
                {
                    "job_title": job.get("title"),
                    "job_link": f"https://www.ejobs.ro/user/locuri-de-munca/{job.get('slug')}/{job.get('id')}",
                    **parse_salary(job.get("salary")),
                    "country": COUNTRY,
                    "city": location,
                    "county": list(dict.fromkeys(counties)),
                    "company": company.get("name"),
                    "remote": remote,
                    "source": SOURCE,
                }
            )

        page += 1

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
    companies = scrape_jobs()
    token = get_token()
    if not token:
        print("Failed to obtain token; aborting publish.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for company in companies.values():
            executor.submit(publish_company, company, token)


if __name__ == "__main__":
    run()
