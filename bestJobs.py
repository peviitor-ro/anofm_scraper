import random
import requests
import time
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()
REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 2.5
PAGE_SIZE = 200


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
offset = 0

while True:
    url = f"https://api.bestjobs.eu/v1/jobs?offset={offset}&limit={PAGE_SIZE}&"

    try:
        print(f"Fetching BestJobs page with offset={offset}, limit={PAGE_SIZE}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        items = response.json().get("items") or []
    except (requests.exceptions.RequestException, requests.exceptions.JSONDecodeError) as e:
        print(f"Failed to fetch jobs at offset {offset}: {e}")
        break

    if not items:
        break

    json.extend(items)
    print(f"Fetched {len(items)} jobs at offset {offset}. Total so far: {len(json)}")

    if len(items) < PAGE_SIZE:
        break

    offset += PAGE_SIZE
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    print(f"Sleeping {delay:.2f} second(s) before next BestJobs request...")
    time.sleep(delay)

companies = {}

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

TOKEN = get_token()

def start(jobs):
    main(jobs.get("jobs"), TOKEN)

    if jobs.get("logo"):
        content_type = "application/json"
        requests.post(
            "https://api.peviitor.ro/v3/logo/add/",
            headers={"Content-Type": content_type},
            json=[{"id": jobs.get("name"), "logo": jobs.get("logo")}],
        )

MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
