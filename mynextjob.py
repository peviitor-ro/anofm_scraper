import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utils import get_token, GetCounty, main, remove_diacritics

_counties = GetCounty()
BASE_URL = "https://www.mynextjob.ro"


def parse_salary(salary_text):
    if not salary_text:
        return {}

    normalized = remove_diacritics(salary_text)
    amounts = [
        int(match.replace(".", "").replace(",", ""))
        for match in re.findall(r"\d[\d\.,]*", normalized)
    ]

    if not amounts:
        return {}

    salary_currency = None
    upper_text = normalized.upper()
    if "EUR" in upper_text:
        salary_currency = "EUR"
    elif "RON" in upper_text or "LEI" in upper_text:
        salary_currency = "RON"

    if not salary_currency:
        return {}

    salary_data = {
        "salary_min": amounts[0],
        "salary_currency": salary_currency,
    }

    if len(amounts) > 1 and any(token in normalized.lower() for token in ["-", "intre", "to"]):
        salary_data["salary_max"] = amounts[1]

    return salary_data


def extract_salary_text(card):
    description = card.select_one(".job-description")
    if not description:
        return ""

    text = remove_diacritics(description.get_text(" ", strip=True)).replace("\xa0", " ")
    match = re.search(
        r"Salariu\s*:?\s*([\d\.,]+(?:\s*[-–]\s*[\d\.,]+)?\s*(?:EUR|RON|LEI))",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()

    return ""


def extract_company(description_block):
    for anchor in description_block.select("a[href]"):
        href = anchor.get("href") or ""
        if "/locuri-de-munca/" in href:
            continue

        company_name = remove_diacritics(anchor.get_text(" ", strip=True)).strip()
        if company_name:
            return company_name

    return ""


def extract_locations(block):
    locations = []

    for anchor in block.select('a[href*="/locuri-de-munca/"]'):
        location_name = remove_diacritics(anchor.get_text(" ", strip=True)).strip()
        if not location_name or location_name.startswith("mai multe"):
            continue
        if location_name not in locations:
            locations.append(location_name)

    extra_locations = block.select_one('a[data-toggle="popover"][data-content]')
    if extra_locations:
        extra_html = extra_locations.get("data-content") or ""
        extra_soup = BeautifulSoup(extra_html, "html.parser")
        for anchor in extra_soup.select('a[href*="/locuri-de-munca/"]'):
            location_name = remove_diacritics(anchor.get_text(" ", strip=True)).strip()
            if location_name and location_name not in locations:
                locations.append(location_name)

    counties = []
    for location in locations:
        county = _counties.get_county(location) or []
        for item in county:
            if item not in counties:
                counties.append(item)

    return locations, counties


companies = {}
page = 1

while True:
    response = requests.get(f"{BASE_URL}/locuri-de-munca?page={page}", timeout=30)
    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.select("#content .jobs > div.job")

    if not cards:
        break

    for card in cards:
        title_element = card.select_one('.job-description h2 > a[href*="/loc-de-munca/"]')
        if not title_element:
            title_element = card.select_one('h3 > a[rel="canonical"][href*="/loc-de-munca/"]')

        description_block = card.select_one(".job-description .description-block")

        if not title_element or not description_block:
            continue

        company = extract_company(description_block)
        if not company:
            continue

        if company not in companies:
            companies[company] = {"name": company, "logo": None, "jobs": []}

        locations, counties = extract_locations(description_block)
        salary_text = extract_salary_text(card)

        companies[company]["jobs"].append(
            {
                "job_title": remove_diacritics(title_element.get_text(" ", strip=True)),
                "job_link": urljoin(BASE_URL, title_element.get("href") or ""),
                **parse_salary(salary_text),
                "country": "Romania",
                "city": locations,
                "county": counties,
                "company": company,
                "source": "MYNEXTJOB",
            }
        )

    if not soup.select_one('link[rel="next"]'):
        break

    page += 1

TOKEN = get_token()


def start(jobs):
    main(jobs.get("jobs"), TOKEN)


MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for company_id, jobs in companies.items():
        executor.submit(start, jobs)
