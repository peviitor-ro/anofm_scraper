import json
import math
import requests
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()
API_URL = "https://www.randstad.ro/api/search/search-results"
BASE_URL = "https://www.randstad.ro/locuri-de-munca/"
PAGE_SIZE = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://www.randstad.ro",
    "Referer": BASE_URL,
}


def normalize_city(city):
    city = remove_diacritics(city or "").strip()
    if not city:
        return []

    city = city.replace("Bucuresti", "Bucuresti")
    return [city]


def parse_salary(salary):
    if not salary:
        return {}

    salary_min = salary.get("SalaryMin")
    salary_max = salary.get("SalaryMax")
    compensation_type = remove_diacritics(salary.get("CompensationType") or "")

    if compensation_type != "pe luna":
        return {}

    salary_data = {"salary_currency": "RON"}

    if salary_min:
        salary_data["salary_min"] = int(float(salary_min))

    if salary_max:
        salary_data["salary_max"] = int(float(salary_max))

    return salary_data


def build_job_link(source):
    sanitized = source.get("BlueXSanitized") or {}
    job_data = source.get("BlueXJobData") or {}

    title_slug = sanitized.get("Title") or ""
    city_slug = sanitized.get("City") or ""
    job_id = job_data.get("JobId") or source.get("JobId") or job_data.get("ReferenceNumber") or ""

    if title_slug and city_slug and job_id:
        return f"https://www.randstad.ro/locuri-de-munca/{title_slug}_{city_slug}_{job_id}/"

    fallback_url = job_data.get("Url") or source.get("url") or ""
    if fallback_url.startswith("http"):
        return fallback_url
    if fallback_url:
        return "https://www.randstad.ro" + fallback_url
    return ""


def fetch_page(page):
    payload = {
        "data": {
            "currentRoute": {
                "path": "/locuri-de-munca/:searchParams*",
                "routeName": "search",
            },
            "currentLanguage": "ro",
            "searchParams": {
                "page": str(page),
            },
        }
    }

    response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


route_data = fetch_page(1)
search_hits = (route_data.get("searchResults") or {}).get("hits") or {}
hits = search_hits.get("hits") or []
total_hits = search_hits.get("total") or len(hits)

print(f"Randstad page 1: fetched {len(hits)} jobs out of {total_hits}")

total_pages = math.ceil(total_hits / PAGE_SIZE) if total_hits else 1
for page in range(2, total_pages + 1):
    page_route_data = fetch_page(page)
    page_hits = ((((page_route_data.get("searchResults") or {}).get("hits") or {}).get("hits")) or [])
    print(f"Randstad page {page}: fetched {len(page_hits)} jobs")

    if not page_hits:
        break

    hits.extend(page_hits)

print(f"Randstad total jobs collected: {len(hits)}")

companies = {"Randstad Romania": {"name": "Randstad Romania", "logo": None, "jobs": []}}
seen_job_ids = set()
seen_job_links = set()

for hit in hits:
    source = hit.get("_source") or {}
    info = source.get("JobInformation") or {}
    location = source.get("JobLocation") or {}
    salary = source.get("Salary") or {}
    job_id = ((source.get("BlueXJobData") or {}).get("JobId") or source.get("JobId") or (source.get("BlueXJobData") or {}).get("ReferenceNumber"))
    job_link = build_job_link(source)

    if (job_id and job_id in seen_job_ids) or (job_link and job_link in seen_job_links):
        continue

    city_list = normalize_city(location.get("City"))
    counties = []
    for city in city_list:
        county = _counties.get_county(city) or []
        for item in county:
            if item not in counties:
                counties.append(item)

    description = remove_diacritics(info.get("Description") or "")
    remote = ["remote"] if "remote" in description.lower() else []

    companies["Randstad Romania"]["jobs"].append(
        {
            "job_title": info.get("Title"),
            "job_link": job_link,
            **parse_salary(salary),
            "country": "Romania",
            "city": city_list,
            "county": counties,
            "company": "Randstad Romania",
            "source": "RANDSTAD",
            "remote": remote,
        }
    )

    if job_id:
        seen_job_ids.add(job_id)
    if job_link:
        seen_job_links.add(job_link)

TOKEN = get_token()


def start(jobs):
    if jobs.get("jobs"):
        main(jobs.get("jobs"), TOKEN)


MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
