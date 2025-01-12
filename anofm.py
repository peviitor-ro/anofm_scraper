import requests
from threading import Thread
import time
from utils import remove_diacritics, get_token, main

url = "https://mediere.anofm.ro/api/entity/vw_public_job_posting"
json = requests.get(url).json().get("rows")

# Create a dictionary with the company id as key and an empty list as value
companies = {
    company.get("employer_id"): []
    for company in json
}

# Iterate over the jobs and append them to the corresponding company
for job in json:
    obj = {
        "job_title": job.get("occupation"),
        "job_link": "https://mediere.anofm.ro/app/module/mediere/job/" + str(job.get("id")),
        "country": "Romania",
        "city": remove_diacritics(job.get("address_locality_name").split(">")[-1].strip()).capitalize(),
        "county": remove_diacritics(job.get("address_locality_name").split(">")[0].strip()).replace("Municipiul", "").strip(),
        "company": job.get("employer_name"),
    }

    companies[job.get("employer_id")].append(obj)

TOKEN = get_token()

# Start a thread for each company
for company_id, jobs in companies.items():
    t = Thread(target=main, args=(jobs, TOKEN))
    t.start()
    time.sleep(0.5)
