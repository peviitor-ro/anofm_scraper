import requests
from bs4 import BeautifulSoup
from utils import get_token, GetCounty, main, remove_diacritics, remove_company
from concurrent.futures import ThreadPoolExecutor
from math import ceil

_counties = GetCounty()

url = "https://back-edujobs.feel-it-services.com/job-postings/query?favoritesFirst=false"

payload = {"page": 1, "limit": 99}


response = requests.request("POST", url, data=payload).json()

pages = ceil(response.get("totalScrapedJobs", 0)
             / payload["limit"])

company_jobs = {}
while payload["page"] <= pages:
    for job in response.get("scrapedJobs", []):
        job_title = job.get("title")
        job_link = f"https://edujobs.ro/job-page/{job.get('scrapedJobId')}"
        company_name = "Edujobs"
        try:
            city = remove_diacritics(job.get("location").split(",")[0].strip())
            county = _counties.get_county(city)
        except AttributeError:
            city = []
            county = []
        if job_title:
            job_data = {
                "job_title": job_title,
                "job_link": job_link,
                "country": "Romania",
                "city": city,
                "county": county,
                "company": company_name,
                "source": "EDUJOBS"
            }

            if company_name not in company_jobs:
                company_jobs[company_name] = {
                    "logo": None,
                    "name": company_name,
                    "jobs": []
                }
            company_jobs[company_name]["jobs"].append(job_data)

    print(f"Page {payload['page']} of {pages} scraped.")
    payload["page"] += 1
    response = requests.request("POST", url, data=payload).json()

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
remove_company("Edujobs", TOKEN)
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in company_jobs.items():
        executor.submit(start, jobs)
