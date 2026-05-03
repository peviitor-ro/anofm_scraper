import json
import random
import requests
import time
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()
REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 2.5
MAX_FETCH_RETRIES = 3
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
    "Referer": "https://www.bestjobs.eu/locuri-de-munca",
}
BASE_URL = "https://www.bestjobs.eu/api/proxy/v2/jobs?limit=24&_lat=44.957117&_lon=24.947214"


def parse_salary(job):
    salary = job.get("salary") or job.get("estimatedSalary")

    if not salary:
        return {}

    salary_data = {}

    if " - " in salary:
        salary_min, salary_max = salary.split(" - ", 1)
        salary_data["salary_min"] = int(salary_min.strip())
        salary_data["salary_max"] = int(salary_max.strip())
    else:
        salary_data["salary_min"] = int(salary.strip())

    salary_data["salary_currency"] = "EUR"

    return salary_data


json = []
page = 1
session = requests.Session()
session.headers.update(HEADERS)
seen_slugs = set()
next_cursor = None

while True:
    items = []
    page_cursor = next_cursor

    if page_cursor:
        url = f"{BASE_URL}&cursor={page_cursor}"
    else:
        url = BASE_URL

    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            print(f"Fetching BestJobs page {page} from {url} (attempt {attempt}/{MAX_FETCH_RETRIES})...")
            response = session.get(url, timeout=30)
            response.raise_for_status()
            payload = response.json()
            items = payload.get("items") or []
            next_cursor = payload.get("nextCursor")
            print(f"BestJobs page {page} returned {len(items)} raw items. Next cursor: {next_cursor}")
            break
        except (requests.exceptions.RequestException, requests.exceptions.JSONDecodeError) as e:
            if attempt == MAX_FETCH_RETRIES:
                print(f"Failed to fetch jobs on page {page}: {e}")
                items = None
                next_cursor = None
                break

            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX) * attempt
            print(f"Request failed on page {page}: {e}. Retrying in {delay:.2f} second(s)...")
            time.sleep(delay)

    if items is None:
        print("Stopping BestJobs pagination because the page could not be fetched.")
        break

    if not items:
        print(f"No items found on BestJobs page {page}. Stopping pagination.")
        break

    page_new_jobs = 0
    for item in items:
        slug = item.get("slug")
        if not slug or slug in seen_slugs:
            continue

        seen_slugs.add(slug)
        json.append(item)
        page_new_jobs += 1

    print(f"Fetched {page_new_jobs} new jobs on page {page}. Total so far: {len(json)}")

    if page_new_jobs == 0:
        print(f"No new jobs found on BestJobs page {page}. Stopping pagination.")
        break

    if not next_cursor:
        print(f"No next cursor found after BestJobs page {page}. Stopping pagination.")
        break

    page += 1
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    print(f"Sleeping {delay:.2f} second(s) before next BestJobs request...")
    time.sleep(delay)

companies = {}

print(f"Building company payloads from {len(json)} scraped BestJobs jobs...")

for job in json:
    if job.get("companyName") not in companies and job.get("companyName") != None:
        companies[job.get("companyName")] = {
            "logo": job.get("companyLogo"),
            "name": job.get("companyName"),
            "jobs": []
        }

    try:
        location = [
            remove_diacritics(city.get("name")) for city in job.get("locations")
        ]
    except:
        location = []

    counties = []

    for city in location:
        county = _counties.get_county(city)
        if county:
            counties.extend(county)

    remote = ["remote"] if "De la distanta" in location else []

    if job.get("companyName") in companies:
        obj = {
            "job_title": job.get("title"),
            "job_link": f"https://www.bestjobs.eu/loc-de-munca/{job.get('slug')}",
            **parse_salary(job),
            "country": "Romania",
            "city": location,
            "county": list(set(counties)),
            "company": job.get("companyName"),
            "remote": remote,
            "source": "BESTJOBS"
        }
        companies[job.get("companyName")]["jobs"].append(obj)

total_jobs = sum(len(company.get("jobs", [])) for company in companies.values())
print(f"Prepared {total_jobs} BestJobs jobs across {len(companies)} companies.")

TOKEN = get_token()

def start(jobs):
    print(f"Publishing {len(jobs.get('jobs', []))} jobs for {jobs.get('name')}...")
    main(jobs.get("jobs"), TOKEN)

    if jobs.get("logo"):
        content_type = "application/json"
        requests.post(
            "https://api.peviitor.ro/v3/logo/add/",
            headers={"Content-Type": content_type},
            json=[{"id": jobs.get("name"), "logo": jobs.get("logo")}],
        )

MAX_WORKERS = 5
print(f"Starting BestJobs publish with {MAX_WORKERS} workers...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
