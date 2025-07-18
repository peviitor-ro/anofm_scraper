import requests
from bs4 import BeautifulSoup
from utils import get_token, GetCounty, main, remove_diacritics
from threading import Thread
import time

_counties = GetCounty()

url = "https://edujobs.ro/jm-ajax/get_listings/"

payload = f"search_keywords=&search_location=&filter_job_type%5B%5D=full-time&filter_job_type%5B%5D=internship&filter_job_type%5B%5D=part-time&filter_job_type%5B%5D=sezonier&filter_job_type%5B%5D=&per_page=15000&orderby=featured&featured_first=true&order=DESC&page=1&remote_position=&show_pagination=false&form_data=search_keywords%3D%26search_location%3D%26search_salary_min%3D%26search_salary_max%3D%26search_rate_min%3D%26search_rate_max%3D%26filter_job_type%255B%255D%3Dfull-time%26filter_job_type%255B%255D%3Dinternship%26filter_job_type%255B%255D%3Dpart-time%26filter_job_type%255B%255D%3Dsezonier%26filter_job_type%255B%255D%3D&job_layout=_list&job_version=3"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'pmpro_visit=1'
}

response = requests.request("POST", url, headers=headers, data=payload).json()

html = response.get("html", "")

soup = BeautifulSoup(html, 'html.parser')

job_cards = soup.find_all('li', class_='job-list')

company_jobs = {}

for card in job_cards:
    job_title = card.find('h2', class_='title').text.strip()
    job_link = card.find('div', class_='job-title').find('a')['href']
    company_name = card.find('span', class_='company').text.replace("Companie:", "").strip()
    city = remove_diacritics(
        card.find('span', class_='location').text.replace("Locație:", "").strip())
    
    county = _counties.get_county(city)

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
            "logo": card.find('img', class_='company_logo')['src'] if card.find('img', class_='company_logo') else None,
            "name": company_name,
            "jobs": []
        }
    company_jobs[company_name]["jobs"].append(job_data)

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
    time.sleep(0.5)  # To avoid overwhelming the server with requests