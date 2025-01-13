import requests
from threading import Thread
from utils import get_token, GetCounty, main, remove_diacritics
import time

_counties = GetCounty()


url = "https://api.bestjobs.eu/v1/jobs?offset=0&limit=10000&"

json = requests.get(url).json().get("items")

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

    remote = "Remote" if "De la distanta" in location else []

    if job.get("companyName") in companies:
        obj = {
            "job_title": job.get("title"),
            "job_link": f"https://www.bestjobs.eu/loc-de-munca/{job.get('slug')}",
            "country": "Romania",
            "city": location,
            "county": list(set(counties)),
            "company": job.get("companyName"),
            "remote": remote
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

for company_id, jobs in companies.items():
    t = Thread(target=start, args=(jobs,))
    t.start()
    time.sleep(0.5)
