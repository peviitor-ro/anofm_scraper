import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

SOURCE = "FJOBS"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

AJAX_URL = "https://fjobs.ro/wp-admin/admin-ajax.php"
_counties = GetCounty()

JOB_ARG = json.dumps({
    "job_short_counter": 7060,
    "atts": {
        "job_view": "view-listing2",
        "job_filters_count": "yes",
        "job_filters_keyword": "yes",
        "job_filters_loc_view": "dropdowns",
        "job_filters_sector_collapse": "yes",
        "job_feat_jobs_top": "yes",
        "num_of_feat_jobs": "100",
        "job_top_search": "no",
        "job_loc_listing": "state",
        "job_rss_feed": "no",
        "job_per_page": "15",
        "job_alerts": "no",
        "job_cat": "",
        "job_skills": "",
        "job_location": "",
        "featured_only": "",
        "job_sort_by": "",
        "job_title_len": "0",
        "job_excerpt": "20",
        "job_order": "DESC",
        "job_orderby": "date",
        "job_pagination": "yes",
        "job_type": "",
        "job_filters": "yes",
        "job_filters_loc": "yes",
        "job_filters_date": "yes",
        "job_filters_type": "yes",
        "job_filters_sector": "yes",
        "job_custom_fields_switch": "no",
        "job_elem_custom_fields": "",
        "job_deadline_switch": "no",
        "quick_apply_job": "no",
    },
    "content": "",
    "job_map_counter": 94788880,
    "page_id": 154,
    "page_url": "https://fjobs.ro/joburi/",
    "custom_fields": [],
})

COUNTRY_MAP = {
    "germany": "Germany", "germania": "Germany", "deutschland": "Germany",
    "olanda": "Netherlands", "belgia": "Belgium", "anglia": "United Kingdom",
    "uk": "United Kingdom", "england": "United Kingdom", "austria": "Austria",
    "spania": "Spain", "italia": "Italy", "franta": "France", "danemarca": "Denmark",
    "suedia": "Sweden", "norvegia": "Norway", "irlanda": "Ireland", "elvetia": "Switzerland",
    "ungaria": "Hungary", "polonia": "Poland", "cehia": "Czech Republic",
    "bulgaria": "Bulgaria", "grecia": "Greece",
}

COUNTRY_ABBR = {
    "de": "Germany", "at": "Austria", "nl": "Netherlands", "be": "Belgium",
    "uk": "United Kingdom", "gb": "United Kingdom", "ch": "Switzerland",
    "fr": "France", "it": "Italy", "es": "Spain", "dk": "Denmark",
    "se": "Sweden", "no": "Norway", "hu": "Hungary", "pl": "Poland",
}


def parse_salary(text):
    text = (text or "").strip().lower()
    match = re.search(r"€\s*([\d.]+)\s*(?:[-–]\s*€?\s*([\d.]+))?\s*/", text)
    if match:
        raw_min = match.group(1)
        raw_max = match.group(2) or raw_min
        num_min = int(raw_min.replace(".", ""))
        num_max = int(raw_max.replace(".", ""))
        if num_min > num_max:
            num_min, num_max = num_max, num_min
        return {"salary_min": num_min, "salary_max": num_max, "salary_currency": "EUR"}
    match_lei = re.search(r"(\d[\d.]*)\s*(?:[-–]\s*(\d[\d.]*))?\s*(lei|ron)", text, re.I)
    if match_lei:
        raw_min = match_lei.group(1)
        raw_max = match_lei.group(2) or raw_min
        num_min = int(raw_min.replace(".", ""))
        num_max = int(raw_max.replace(".", ""))
        if num_min > num_max:
            num_min, num_max = num_max, num_min
        return {"salary_min": num_min, "salary_max": num_max, "salary_currency": "RON"}
    return {}


def detect_country(title, loc_text, info_text):
    combined = f"{title} {loc_text} {info_text}".lower()
    combined = remove_diacritics(combined)

    for keyword, country in COUNTRY_MAP.items():
        if keyword in combined:
            return country

    title_normalized = remove_diacritics(title.lower())

    match = re.search(r"\(([^)]+)\)", title_normalized)
    if match:
        inside = match.group(1).strip()
        if inside in COUNTRY_ABBR:
            return COUNTRY_ABBR[inside]

    return "Romania"


def is_romanian_city(city_name):
    if not city_name:
        return False
    normalized = remove_diacritics(city_name.lower())
    return normalized in ROMANIAN_CITIES


def parse_job(wrap):
    h2 = wrap.select_one("h2 a")
    if not h2:
        return None

    title = h2.get_text(" ", strip=True)
    link = h2.get("href", "")

    company_el = wrap.select_one("li.job-company-name a")
    company = company_el.get_text(" ", strip=True).replace("@", "").strip() if company_el else ""

    info_text = wrap.get_text(" ", strip=True)
    salary_data = parse_salary(info_text)

    small_el = wrap.select_one("div.careerfy-joblisting-plain-right small")
    loc_text = small_el.get_text(" ", strip=True) if small_el else ""
    
    country = detect_country(title, loc_text, info_text)

    if country != "Romania":
        return None

    city = ""
    county = []
    if loc_text:
        parts = [p.strip() for p in loc_text.split(",")]
        city = parts[0] if parts else ""
        if city:
            county = _counties.get_county(city) or []

    remote = ["remote"] if "remote" in title.lower() or "remote" in info_text.lower() else []

    if not company:
        company = "FJobs"

    return company, {
        "job_title": title,
        "job_link": link,
        **salary_data,
        "country": "Romania",
        "city": [city] if city else [],
        "county": county,
        "company": company,
        "source": SOURCE,
        "remote": remote,
    }


def scrape_page(page):
    data = {
        "action": "jobsearch_jobs_content",
        "job_page": str(page),
        "ajax_filter": "true",
        "posted": "all",
        "sector_cat": "all",
        "view_type": "",
        "job_arg": JOB_ARG,
    }

    response = requests.post(AJAX_URL, headers=HEADERS, data=data, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    wraps = soup.select("div.careerfy-joblisting-plain-wrap")
    jobs = []

    for wrap in wraps:
        parsed = parse_job(wrap)
        if parsed:
            jobs.append(parsed)

    total_text = ""
    for t in soup.find_all(string=True):
        if "Afișate" in t:
            total_text = t.strip()
            break

    print(f"  Page {page}: {len(jobs)} jobs ({total_text})")
    return jobs


def scrape_fjobs():
    companies = {}
    seen_links = set()
    total_pages = 7

    for page in range(1, total_pages + 1):
        page_jobs = scrape_page(page)

        for company_name, job_data in page_jobs:
            link = job_data.get("job_link", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            if company_name not in companies:
                companies[company_name] = {"name": company_name, "logo": None, "jobs": []}
            companies[company_name]["jobs"].append(job_data)

    return companies


TOKEN = get_token()


def start(jobs):
    if jobs.get("jobs"):
        all_jobs = jobs.get("jobs")
        if len(all_jobs) > 1000:
            batch_size = 100
            total_batches = (len(all_jobs) + batch_size - 1) // batch_size
            print(f"Processing {len(all_jobs)} jobs in {total_batches} batches...")
            remove_company(jobs.get("name"), TOKEN)
            for i in range(0, len(all_jobs), batch_size):
                batch = all_jobs[i:i + batch_size]
                batch_num = i // batch_size + 1
                print(f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
                main(batch, TOKEN, user=True)
                time.sleep(2)
        else:
            main(all_jobs, TOKEN)


if __name__ == "__main__":
    companies = scrape_fjobs()

    print(f"\nTotal companies: {len(companies)}")
    total_jobs = sum(len(company["jobs"]) for company in companies.values())
    print(f"Total jobs: {total_jobs}")

    for company_name, data in companies.items():
        if len(data["jobs"]) > 1000:
            print(f"{company_name}: {len(data['jobs'])} jobs (will remove company)")
        else:
            print(f"{company_name}: {len(data['jobs'])} jobs")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for jobs in companies.values():
            executor.submit(start, jobs)
