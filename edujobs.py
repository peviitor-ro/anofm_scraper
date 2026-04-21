import requests
from bs4 import BeautifulSoup
from utils import get_token, GetCounty, main, remove_diacritics, remove_company
from concurrent.futures import ThreadPoolExecutor
from math import ceil
import time

_counties = GetCounty()

url = "https://back-edujobs.feel-it-services.com/job-postings/query?favoritesFirst=false"

payload = {"page": 1, "limit": 99}


response = requests.request("POST", url, data=payload).json()

pages = ceil(response.get("totalScrapedJobs", 0)
             / payload["limit"])

company_jobs = {}


def ensure_company(company_name, logo=None):
    if company_name not in company_jobs:
        company_jobs[company_name] = {
            "logo": logo,
            "name": company_name,
            "jobs": []
        }
    elif logo and not company_jobs[company_name].get("logo"):
        company_jobs[company_name]["logo"] = logo


while payload["page"] <= pages:
    for job in response.get("scrapedJobs", []):
        job_title = job.get("title")
        job_link = job.get("originalUrl")
        company_name = "Edujobs"
        try:
            city = remove_diacritics(job.get("location").split(",")[0].strip())
            county = _counties.get_county(city)
        except AttributeError:
            city = []
            county = []
        if job_title and job_link:
            job_data = {
                "job_title": job_title,
                "job_link": job_link,
                "country": "Romania",
                "city": city,
                "county": county,
                "company": company_name,
                "source": "EDUJOBS"
            }

            ensure_company(company_name)
            company_jobs[company_name]["jobs"].append(job_data)

    for job in response.get("jobPostings", []):
        job_title = job.get("title")
        job_link = f"https://edujobs.ro/job-page/{job.get('id')}"
        company_name = job.get("company", {}).get("name")
        logo = job.get("logo")

        try:
            city = remove_diacritics(job.get("location").split(",")[0].strip())
            county = _counties.get_county(city)
        except AttributeError:
            city = []
            county = []

        if job_title and company_name:
            job_data = {
                "job_title": job_title,
                "job_link": job_link,
                "country": "Romania",
                "city": city,
                "county": county,
                "company": company_name,
                "source": "EDUJOBS"
            }

            ensure_company(company_name, logo)
            company_jobs[company_name]["jobs"].append(job_data)

    print(f"Page {payload['page']} of {pages} scraped.")
    payload["page"] += 1
    response = requests.request("POST", url, data=payload).json()

TOKEN = get_token()

def start(jobs):
    if jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        batch_size = 100

        total_batches = (len(all_jobs) + batch_size - 1) // batch_size
        print(f"Processing {len(all_jobs)} jobs in {total_batches} batches...")

        for i in range(0, len(all_jobs), batch_size):
            batch = all_jobs[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
            main(batch, TOKEN, user=True)
            time.sleep(2)

    if jobs.get("logo"):
        content_type = "application/json"
        requests.post(
            "https://api.peviitor.ro/v3/logo/add/",
            headers={"Content-Type": content_type},
            json=[{"id": jobs.get("name"), "logo": jobs.get("logo")}],
        )


MAX_WORKERS = 5
remove_company("Edujobs", TOKEN)
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in company_jobs.items():
        executor.submit(start, jobs)
