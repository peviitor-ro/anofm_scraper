import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

BASE_URL = "https://lajumate.ro"
LIST_URL = f"{BASE_URL}/anunturi/locuri-de-munca"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}

_counties = GetCounty()


def is_job_listing(title):
    text = remove_diacritics((title or "").lower())
    words = set(re.findall(r"[a-z0-9]+", text))

    positive_words = {
        "angajez",
        "angajeaza",
        "angajam",
        "angajare",
        "recrutam",
        "recrutare",
        "job",
        "operator",
        "sofer",
        "muncitor",
        "lucrator",
        "consilier",
        "inginer",
        "electrician",
        "asistent",
        "bucatar",
        "ospatar",
        "curier",
        "manipulant",
        "tehnician",
        "contabil",
        "educatoare",
        "menajera",
        "mecanic",
        "montator",
        "tractorist",
        "buldoexcavatorist",
        "confectioner",
        "consultant",
        "personal",
    }

    positive_phrases = [
        "loc de munca",
        "munca in strainatate",
        "hai in echipa",
        "salariu motivant",
    ]

    negative_words = {
        "ofer",
        "caut",
        "candidat",
        "cautamjob",
        "servicii",
        "ajutor",
        "casnic",
    }

    negative_phrases = [
        "ofer ajutor",
        "ofer servicii",
        "caut loc de munca",
        "caut de munca",
        "barbat 44 ani",
        "barbat matur",
    ]

    positive_score = sum(word in words for word in positive_words)
    positive_score += sum(phrase in text for phrase in positive_phrases)

    negative_score = sum(word in words for word in negative_words)
    negative_score += sum(phrase in text for phrase in negative_phrases)

    return positive_score >= 1 and negative_score == 0


def get_total_results(soup):
    h1 = soup.find("h1")
    if not h1:
        return 0

    title_container = h1.parent
    if not title_container:
        return 0

    text = " ".join(title_container.stripped_strings)
    match = re.search(r"(\d+)\s+anun", text)
    return int(match.group(1)) if match else 0


def get_jobs_container(soup):
    h1 = soup.find("h1")
    if not h1:
        return None

    return h1.find_parent(
        "div",
        class_=lambda classes: classes and "relative" in classes and "w-full" in classes,
    )


def parse_salary(card):
    salary_elem = card.select_one("div.md\\:text-heading-mobile-md")
    salary_text = salary_elem.get_text(" ", strip=True) if salary_elem else ""
    normalized_text = remove_diacritics(salary_text.lower())

    if not salary_text or normalized_text in {"schimb/donatie", "schimb / donatie"}:
        return {}

    amount_match = re.search(r"(\d[\d\.]*)", salary_text)
    if not amount_match:
        return {}

    amount = int(amount_match.group(1).replace(".", ""))
    currency = None

    if "lei" in normalized_text:
        currency = "RON"
    elif "eur" in normalized_text or "euro" in normalized_text:
        currency = "EUR"

    if not currency:
        return {}

    return {
        "salary_min": amount,
        "salary_max": amount,
        "salary_currency": currency,
    }


def parse_listing_card(card):
    title_elem = card.select_one("h2")
    location_elem = card.select_one("div.flex.justify-between.w-full > div")

    title = title_elem.get_text(" ", strip=True) if title_elem else ""
    location_raw = location_elem.get_text(" ", strip=True) if location_elem else ""
    location = remove_diacritics(location_raw) if location_raw else ""

    link = card.get("href")
    if link and not link.startswith("http"):
        link = f"{BASE_URL}{link}"

    county = _counties.get_county(location_raw) if location_raw else []
    if not isinstance(county, list):
        county = [county] if county else []

    remote = ["remote"] if "remote" in remove_diacritics(title.lower()) else []

    if not is_job_listing(title):
        return None

    return {
        "job_title": title,
        "job_link": link,
        **parse_salary(card),
        "country": "Romania",
        "city": [location] if location else [],
        "county": county,
        "company": "Lajumate",
        "source": "LAJUMATE",
        "remote": remote,
    }


def extract_section_jobs(section):
    jobs = []
    seen_links = set()

    cards = section.select('a[href^="/ad/"]')
    for card in cards:
        job = parse_listing_card(card)
        if not job:
            continue

        link = job.get("job_link")

        if not link or link in seen_links:
            continue

        seen_links.add(link)
        jobs.append(job)

    return jobs


def scrape_lajumate(max_pages=None, delay=1):
    companies = {"Lajumate": {"name": "Lajumate", "logo": None, "jobs": []}}
    total_results = None
    current_page = 1
    seen_links = set()

    while True:
        if max_pages and current_page > max_pages:
            break

        print(f"Scraping page {current_page}...")

        response = requests.get(
            LIST_URL,
            params={"page": current_page} if current_page > 1 else None,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        if total_results is None:
            total_results = get_total_results(soup)
            print(f"Total jobs available: {total_results}")

        container = get_jobs_container(soup)
        if not container:
            print("Main jobs container not found")
            break

        sections = []
        for child in container.find_all("div", recursive=False):
            heading = child.find("div")
            heading_text = heading.get_text(" ", strip=True) if heading else ""
            if heading_text in {"Anunțuri", "Anunțuri premium"} and child.find("a", href=re.compile(r"^/ad/")):
                sections.append(child)

        page_jobs = []

        for section in sections:
            page_jobs.extend(extract_section_jobs(section))

        added = 0
        for job in page_jobs:
            link = job["job_link"]
            if link in seen_links:
                continue
            seen_links.add(link)
            companies["Lajumate"]["jobs"].append(job)
            added += 1

        print(f"Found {added} new jobs on page {current_page}")

        next_link = soup.select_one('link[rel="next"]')
        if not next_link or added == 0:
            break

        current_page += 1
        time.sleep(delay)

    return companies


TOKEN = get_token()


def start(jobs):
    if jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        batch_size = 100

        total_batches = (len(all_jobs) + batch_size - 1) // batch_size
        print(f"Processing {len(all_jobs)} jobs in {total_batches} batches...")

        for i in range(0, len(all_jobs), batch_size):
            batch = all_jobs[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
            main(batch, TOKEN, user=True)
            time.sleep(2)


if __name__ == "__main__":
    companies = scrape_lajumate()

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _, jobs in companies.items():
            executor.submit(start, jobs)
