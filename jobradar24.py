import html
import json
import re
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()


def parse_salary(text):
    normalized = remove_diacritics(text or "")
    match = re.search(r"(\d[\d\.]*)\s*-\s*(\d[\d\.]*)\s*LEI/Luna", normalized, flags=re.IGNORECASE)
    if match:
        return {
            "salary_min": int(match.group(1).replace(".", "")),
            "salary_max": int(match.group(2).replace(".", "")),
            "salary_currency": "RON",
        }

    single = re.search(r"(\d[\d\.]*)\s*LEI/Luna", normalized, flags=re.IGNORECASE)
    if single:
        return {
            "salary_min": int(single.group(1).replace(".", "")),
            "salary_currency": "RON",
        }

    return {}


companies = {}
seen_links = set()
REQUEST_DELAY = 1
page = 1

while True:
    print(f"Scraping Jobradar24 page {page}...")
    response_html = requests.get(f"https://www.jobradar24.ro/locuri-de-munca?page={page}", timeout=30).text
    soup = BeautifulSoup(response_html, "html.parser")
    job_cards = soup.select('div[data-cy="listing-cards-components"]')
    print(f"Found {len(job_cards)} job cards on page {page}")

    if not job_cards:
        print(f"No jobs found on page {page}. Stopping.")
        break

    for card in job_cards:
        link = card.select_one('a[data-cy="listing-title-link"]')
        if not link:
            continue

        ga4_payload = link.get("data-ga4-select-item") or "{}"
        try:
            ga4_data = json.loads(html.unescape(ga4_payload))
        except (json.JSONDecodeError, TypeError):
            ga4_data = {}

        title = link.get_text(" ", strip=True)
        href = link.get("href")

        if not title or not href:
            continue

        if href.startswith("/"):
            href = f"https://www.jobradar24.ro{href}"

        if href in seen_links:
            continue

        seen_links.add(href)

        text = remove_diacritics(card.get_text(" ", strip=True))
        lines = [item.strip() for item in card.get_text("\n", strip=True).split("\n") if item.strip()]
        company = (ga4_data.get("affiliation") or (lines[1] if len(lines) > 1 else "Jobradar24")).strip()

        location_text = remove_diacritics((ga4_data.get("location_id") or "").strip())
        city = location_text.replace("Locatie remote", "").replace("Locatii multiple", "").strip()
        county = _counties.get_county(city) or [] if city else []

        remote = []
        if "remote" in text.lower() or "work from home" in text.lower() or "remote" in location_text.lower():
            remote.append("remote")

        if company not in companies:
            companies[company] = {"name": company, "logo": None, "jobs": []}

        companies[company]["jobs"].append(
            {
                "job_title": title,
                "job_link": href,
                **parse_salary(text),
                "country": "Romania",
                "city": [city] if city else [],
                "county": county,
                "company": company,
                "source": "JOBRADAR24",
                "remote": remote,
            }
        )

    page += 1
    print(f"Finished page {page - 1}. Sleeping {REQUEST_DELAY} second(s)...")
    time.sleep(REQUEST_DELAY)

TOKEN = get_token()

def start(jobs):
    main(jobs.get("jobs"), TOKEN)


MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)