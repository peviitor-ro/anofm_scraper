import requests
from threading import Thread
import time
from utils import remove_diacritics, get_token

# Headers for the requests
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
}

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

# Publish the jobs to the API
def publish_jobs(lst):
    url = "https://api.peviitor.ro/v5/add/"
    headers["Authorization"] = f"Bearer {TOKEN}"

    response = requests.post(url, json=lst, headers=headers)

    try:
        return response.json()
    except:
        return []

# Main function
def main(obj):
    jobs = publish_jobs(obj)

    if not jobs:
        return
    
    if isinstance(jobs, list):
      for job in jobs:
          job["published"] = True

      url = "https://api.laurentiumarian.ro/jobs/publish/"
      headers["Authorization"] = f"Bearer {TOKEN}"
      restponse = requests.post(url, json=jobs, headers=headers)

      if restponse.status_code == 200:
          print(f"Jobs published successfully for company {obj[0].get('company')}")
      else:
          print(f"Jobs not published for company {obj[0].get('company')}")

# Start a thread for each company
for company_id, jobs in companies.items():
    t = Thread(target=main, args=(jobs,))
    t.start()
    time.sleep(0.5)
