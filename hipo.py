import requests
from threading import Thread
from utils import get_token, GetCounty, main, remove_diacritics
from citieseJobs import cities
from bs4 import BeautifulSoup
import time

_counties = GetCounty()

page = 1
url = f"https://www.hipo.ro/locuri-de-munca/cautajob/"
company_jobs = {}

while True:
    response = requests.get(url + f"{page}")
    soup = BeautifulSoup(response.content, 'html.parser')
    
    job_cards = soup.find_all('div', class_='job-item')

    if len(job_cards) == 3:
        break  # Exit loop if no more job cards are found

    for card in job_cards:
        try:
            job_title = card.find('a', class_='job-title')['title']
            job_link = "https://www.hipo.ro" + card.find('a', class_='job-title')['href']
            company_name = card.find('p', class_='company-name').text.strip()
            locations = card.find('span', class_='badge-type').text.split(',') if card.find('span', class_='badge-type') else []
            cities = []
            counties = []

            for city in locations:
                city = remove_diacritics(city.title().strip())
                county = _counties.get_county(city)
                if county and county not in counties:
                    cities.append(city)
                    for c in county:
                        if c not in counties:
                            counties.append(c)

            if company_name not in company_jobs:
                company_jobs[company_name] = {
                    "logo": card.find('img', class_='img-fluid')['src'] if card.find('img', class_='img-fluid') else None,
                    "name": company_name,
                    "jobs": []
                }

            
            job_data = {
                "job_title": job_title,
                "job_link": job_link,
                "country": "Romania",
                "city": cities,
                "county": counties,
                "company": company_name,
                "source": "HIPO"
            }
            
            company_jobs[company_name]["jobs"].append(job_data)
        except Exception as e:
            continue

    page += 1
    time.sleep(1)  # Be polite and avoid overwhelming the server

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


for company_id, jobs in company_jobs.items():
    t = Thread(target=start, args=(jobs,))
    t.start()
    time.sleep(1)  # To avoid overwhelming the server with requests
    