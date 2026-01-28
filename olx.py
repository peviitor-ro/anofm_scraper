import requests
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics, remove_company

_counties = GetCounty()

url = "https://www.olx.ro/api/v1/offers/?offset=0&limit=50&category_id=4&currency=RON&filter_refiners=spell_checker&sl=194568119cax1c7511aa"
json = requests.get(url).json()

try:
    next_page = json.get("links").get("next").get("href")
except:
  next_page = None

companies = {}

while next_page:
    for job in json.get("data"):
        company = job.get("user").get("company_name") or "olx"
        if company not in companies:
            companies[company] = {
                "logo": job.get("user").get("logo"),
                "name": job.get("user").get("company_name"),
                "jobs": []
            }


        location = remove_diacritics(job.get("location").get("city").get("name"))
        county = _counties.get_county(location) or []
  
        obj = {
            "job_title": job.get("title"),
            "job_link": job.get("url"),
            "country": "Romania",
            "city": location,
            "county": county,
            "company": company,
            "source": "OLX"
        }
        companies[company]["jobs"].append(obj)

    json = requests.get(next_page).json()
    try:
        next_page = json.get("links").get("next").get("href")
    except:
        next_page = None

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

remove_company("OLX", TOKEN)
MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)



