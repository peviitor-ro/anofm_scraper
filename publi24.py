import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from utils import get_token, GetCounty, main, remove_diacritics, remove_company
import time

_counties = GetCounty()


def parse_publi24_listing(article, base_url="https://www.publi24.ro"):
    try:
        link_elem = article.select_one(
            'h2.article-title a, h3.article-title a')
        if not link_elem:
            link_elem = article.select_one('.article-title a')

        title = link_elem.text.strip() if link_elem else ""

        link = link_elem.get('href') if link_elem else None
        if link and not link.startswith('http'):
            link = base_url + link

        location_elem = article.select_one(
            '.article-location span, .article-location')
        location_raw = location_elem.text.strip() if location_elem else ""

        parts = location_raw.split(',')
        city_raw = parts[0].strip() if len(parts) > 0 else ""
        county_raw = parts[1].strip() if len(parts) > 1 else ""

        city = remove_diacritics(city_raw) if city_raw else ""

        county = _counties.get_county(city_raw) if city_raw else []
        if not isinstance(county, list):
            county = [county] if county else []

        description_elem = article.select_one('.article-description')
        description = description_elem.text.strip() if description_elem else ""

        remote = []
        if "remote" in description.lower() or "de la distanta" in description.lower() or "la domiciliu" in description.lower():
            remote = ["remote"]

        return {
            "job_title": title,
            "job_link": link,
            "country": "Romania",
            "city": [city] if city else [],
            "county": county,
            "company": "Publi24",
            "source": "PUBLI24",
            "remote": remote,
        }
    except Exception as e:
        return None


def get_total_results():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
    }
    response = requests.get(
        "https://www.publi24.ro/anunturi/locuri-de-munca/?pag=1&pagesize=5",
        headers=headers
    )
    return int(response.headers.get('total-results', 0))


def scrape_publi24(page_size=500, max_pages=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
    }

    companies = {"Publi24": {"name": "Publi24", "logo": None, "jobs": []}}
    total_results = get_total_results()
    print(f"Total jobs available: {total_results}")

    if max_pages:
        total_pages = min(max_pages, (total_results // page_size) + 2)
    else:
        total_pages = (total_results // page_size) + 2

    for page in range(1, total_pages + 1):
        print(f"Scraping page {page}/{total_pages}...")

        url = f"https://www.publi24.ro/anunturi/locuri-de-munca/?pag={page}&pagesize={page_size}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.select('.article-item')

        if not articles:
            print(f"No articles found on page {page}")
            break

        for article in articles:
            job_data = parse_publi24_listing(article)
            if not job_data or not job_data.get('job_link'):
                continue

            companies["Publi24"]["jobs"].append(job_data)

        time.sleep(1)
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
            print(
                f"Sending batch {batch_num}/{total_batches} ({len(batch)} jobs)...")
            main(batch, TOKEN, user=True)
            time.sleep(2)


if __name__ == "__main__":
    companies = scrape_publi24(page_size=500)

    print(f"Total companies: {len(companies)}")
    total_jobs = sum(len(c["jobs"]) for c in companies.values())
    print(f"Total jobs: {total_jobs}")

    remove_company("Publi24", TOKEN)

    MAX_WORKERS = 5
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for company_id, jobs in companies.items():
            executor.submit(start, jobs)
