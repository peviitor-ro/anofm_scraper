import requests
import re
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main
from citieseJobs import cities

_counties = GetCounty()


def parse_salary(salary):
    if not salary:
        return {}

    parts = salary.split()
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

# Headers for the requests
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
}
page = 1
url = f"https://api.ejobs.ro/jobs?pageSize=100&page={page}"
json = requests.get(url).json().get("jobs")

companies = {}

while json:
    for company in json:
        if company.get("company").get("id") not in companies and company.get("company").get("id") != None:
            companies[company.get("company").get("id")] = {
                "logo": f'https://content.ejobs.ro/{company.get("company").get("logoUrl")}',
                "name": company.get("company").get("name"),
                "jobs": []
            }
    for job in json:
        try:
            location = [
                cities.get(str(city.get("cityId"))) for city in job.get("locations") if str(city.get("cityId")) in cities
            ]
        except:
            location = []

        counties = []

        for city in location:
            county = _counties.get_county(city)
            if county:
                counties.extend(county)

        remote = ["remote"] if "Remote" in location else []
        if job.get("company").get("id") in companies:
            obj = {
                "job_title": job.get("title"),
                "job_link": f"https://www.ejobs.ro/user/locuri-de-munca/{job.get('slug')}/{job.get('id')}",
                **parse_salary(job.get("salary")),
                "country": "Romania",
                "city": location,
                "county": list(set(counties)),
                "company": job.get("company").get("name"),
                "remote": remote,
                "source": "EJOBS"
            }
            companies[job.get("company").get("id")]["jobs"].append(obj)

    page += 1
    url = f"https://api.ejobs.ro/jobs?pageSize=100&page={page}"
    json = requests.get(url).json().get("jobs")
    
# print(companies)
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
