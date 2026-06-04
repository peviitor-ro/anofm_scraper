import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

API_URL = "https://usamvjobs.ro/wp-json/wp/v2/job-listings"
SOURCE = "USAMVJOBS"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

DETAIL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

_counties = GetCounty()

ROMANIAN_CITIES = [
    "Bucuresti", "Arad", "Bacau", "Baia Mare", "Baia-Mare", "Brasov", "Buzau",
    "Cluj-Napoca", "Cluj Napoca", "Constanta", "Craiova", "Galati", "Iasi",
    "Oradea", "Pitesti", "Ploiesti", "Sibiu", "Targu Mures", "Targu-Mures",
    "Targu Mureș", "Targu-Mures", "Timisoara", "Satu Mare", "Satu-Mare",
    "Alba Iulia", "Alba-Iulia", "Sfantu Gheorghe", "Sfantu-Gheorghe",
    "Miercurea Ciuc", "Miercurea-Ciuc", "Bistrita", "Braila", "Botosani",
    "Calarasi", "Caracal", "Deva", "Drobeta Turnu Severin", "Focsani",
    "Giurgiu", "Piatra Neamt", "Piatra-Neamt", "Ramnicu Valcea", "Ramnicu-Valcea",
    "Resita", "Slatina", "Slobozia", "Suceava", "Targoviste", "Targu Jiu",
    "Targu-Jiu", "Tulcea", "Vaslui", "Zalau", "Turda", "Medias", "Campina",
    "Campulung", "Gheorghieni", "Odorheiu Secuiesc", "Odorheiu-Secuiesc",
    "Targu Secuiesc", "Targu-Secuiesc", "Lugoj", "Sighetu Marmatiei",
    "Sighetu-Marmatiei", "Roman", "Hunedoara", "Sighisoara", "Petrosani",
    "Mangalia", "Navodari", "Techirghiol", "Otopeni", "Voluntari", "Pantelimon",
    "Buftea", "Chitila", "Magurele", "Alexandria", "Popesti-Leordeni",
    "Popesti Leordeni", "Barlad", "Falticeni", "Radauti",
    "Campulung Moldovenesc", "Dorohoi", "Tecuci", "Moinesti", "Onesti",
    "Campia-Turzii", "Dej", "Gherla", "Ludus", "Reghin", "Sighisoara", "Sovata",
    "Aiud", "Sebes", "Blaj", "Caransebes", "Simeria", "Orastie", "Petrila",
    "Motru", "Dragasani", "Bals", "Corabia", "Calafat", "Filiasi", "Videle",
    "Rosiori de Vede", "Turnu Magurele", "Urlati", "Mizil", "Boldesti-Scaeni",
    "Moreni", "Pucheni", "Targu Frumos", "Harlau", "Pascani", "Marasesti",
    "Panciu", "Adjud", "Covasna", "Intorsura Buzaului", "Baraolt",
    "Toplita", "Borsec", "Gheorgheni", "Sangeorgiu de Mures",
    "Sangeorgiu-de-Mures", "Sarmasu", "Tarnaveni", "Iernut", "Ungheni",
]

ROMANIA_COUNTIES = {
    "Alba": ["Alba Iulia", "Alba-Iulia", "Aiud", "Sebes", "Blaj"],
    "Arad": ["Arad"],
    "Arges": ["Pitesti", "Campulung"],
    "Bacau": ["Bacau", "Moinesti", "Onesti"],
    "Bihor": ["Oradea"],
    "Bistrita-Nasaud": ["Bistrita"],
    "Botosani": ["Botosani", "Dorohoi"],
    "Brasov": ["Brasov", "Sacele", "Fagaras", "Codlea"],
    "Braila": ["Braila"],
    "Bucuresti": ["Bucuresti", "Bucuresti Ilfov"],
    "Buzau": ["Buzau"],
    "Calarasi": ["Calarasi", "Oltenita"],
    "Caras-Severin": ["Resita", "Caransebes"],
    "Cluj": ["Cluj-Napoca", "Cluj Napoca", "Turda", "Dej", "Gherla", "Campia-Turzii"],
    "Constanta": ["Constanta", "Mangalia", "Navodari", "Techirghiol"],
    "Covasna": ["Sfantu Gheorghe", "Sfantu-Gheorghe", "Targu Secuiesc", "Targu-Secuiesc", "Covasna", "Baraolt"],
    "Dambovita": ["Targoviste", "Moreni", "Pucioasa"],
    "Dolj": ["Craiova", "Calafat", "Bailesti"],
    "Galati": ["Galati", "Tecuci"],
    "Giurgiu": ["Giurgiu"],
    "Gorj": ["Targu Jiu", "Targu-Jiu", "Motru"],
    "Harghita": ["Miercurea Ciuc", "Miercurea-Ciuc", "Gheorgheni", "Odorheiu Secuiesc", "Odorheiu-Secuiesc", "Toplita", "Borsec"],
    "Hunedoara": ["Deva", "Hunedoara", "Petrosani", "Petrila", "Orastie", "Calan", "Simeria", "Lupeni"],
    "Ialomita": ["Slobozia"],
    "Iasi": ["Iasi", "Pascani", "Harlau"],
    "Ilfov": ["Otopeni", "Voluntari", "Pantelimon", "Buftea", "Magurele", "Chitila", "Popesti-Leordeni", "Popesti Leordeni"],
    "Maramures": ["Baia Mare", "Baia-Mare", "Sighetu Marmatiei", "Sighetu-Marmatiei"],
    "Mehedinti": ["Drobeta Turnu Severin"],
    "Mures": ["Targu Mures", "Targu-Mures", "Targu Mureș", "Reghin", "Sighisoara", "Tarnaveni", "Ludus", "Sovata", "Iernut"],
    "Neamt": ["Piatra Neamt", "Piatra-Neamt", "Roman"],
    "Olt": ["Slatina", "Caracal", "Corabia", "Bals"],
    "Prahova": ["Ploiesti", "Campina", "Urlati", "Mizil", "Boldesti-Scaeni"],
    "Salaj": ["Zalau"],
    "Satu Mare": ["Satu Mare", "Satu-Mare", "Carei"],
    "Sibiu": ["Sibiu", "Medias"],
    "Suceava": ["Suceava", "Radauti", "Campulung Moldovenesc", "Falticeni"],
    "Teleorman": ["Alexandria", "Rosiori de Vede", "Turnu Magurele", "Videle"],
    "Timis": ["Timisoara", "Lugoj"],
    "Tulcea": ["Tulcea"],
    "Valcea": ["Ramnicu Valcea", "Ramnicu-Valcea", "Dragasani"],
    "Vaslui": ["Vaslui", "Barlad"],
    "Vrancea": ["Focsani", "Adjud", "Panciu", "Marasesti"],
}


def find_city_in_text(text):
    normalized = remove_diacritics(text.lower())
    for city in ROMANIAN_CITIES:
        normalized_city = remove_diacritics(city.lower())
        if normalized_city in normalized:
            return city.replace("-", " ")
    return ""


def find_county(city_name):
    if not city_name:
        return []
    county = _counties.get_county(city_name) or []
    if county:
        return county if isinstance(county, list) else [county]
    for county_name, cities in ROMANIA_COUNTIES.items():
        normalized_cities = [remove_diacritics(c.lower()) for c in cities]
        if remove_diacritics(city_name.lower()) in normalized_cities:
            return [county_name]
    return []

SALARY_PATTERNS = [
    re.compile(r"(?:de la|intre|din|de|\b)\s*(\d[\d.,]*)\s*(?:[-–]|pana la|si|la)\s*(\d[\d.,]*)\s*(lei|ron|eur|euro)", re.I),
    re.compile(r"(\d[\d.,]*)\s*(?:[-–])\s*(\d[\d.,]*)\s*(lei|ron|eur|euro)", re.I),
    re.compile(r"(\d[\d.,]*)\s*(lei|ron|eur|euro)", re.I),
    re.compile(r"(lei|ron|eur|euro)\s*(\d[\d.,]*)", re.I),
]


def clean_text(value):
    if not value:
        return ""
    if "<" in value and ">" in value:
        return BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return value.strip()


def parse_salary(text):
    if not text:
        return {}

    normalized = remove_diacritics(text.lower())
    has_currency = any(kw in normalized for kw in ("lei", "ron", "eur", "euro"))
    if not has_currency:
        return {}

    for pattern in SALARY_PATTERNS:
        match = pattern.search(normalized)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                raw_min, raw_max, curr = groups
                raw_min = raw_min.replace(",", ".").replace(" ", "")
                raw_max = raw_max.replace(",", ".").replace(" ", "")
                num_min = int(float(raw_min)) if "." in raw_min else int(raw_min)
                num_max = int(float(raw_max)) if "." in raw_max else int(raw_max)
                currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(curr.lower(), "RON")
                if num_min > num_max:
                    num_min, num_max = num_max, num_min
                return {
                    "salary_min": num_min,
                    "salary_max": num_max,
                    "salary_currency": currency,
                }
            elif len(groups) == 2:
                if groups[0].replace(",", "").replace(".", "").isdigit():
                    raw_num, curr = groups
                else:
                    curr, raw_num = groups
                raw_num = raw_num.replace(",", ".").replace(" ", "")
                num = int(float(raw_num)) if "." in raw_num else int(raw_num)
                currency = {"lei": "RON", "ron": "RON", "eur": "EUR", "euro": "EUR"}.get(curr.lower(), "RON")
                return {
                    "salary_min": num,
                    "salary_max": num,
                    "salary_currency": currency,
                }

    return {}


def parse_job_type(class_list):
    for cls in class_list:
        if cls.startswith("job_listing_type-"):
            return cls.replace("job_listing_type-", "").replace("-", " ").title()
    return ""


def fetch_detail(url):
    try:
        time.sleep(random.uniform(1.0, 1.5))
        resp = requests.get(url, headers=DETAIL_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        company_el = soup.select_one("h2.single-job-listing-company-name a")
        company = company_el.get_text(strip=True) if company_el else ""

        location_el = soup.select_one("div.single-job-overview-location")
        location_raw = location_el.get_text(strip=True) if location_el else ""
        location = re.sub(r"[<>]", "", location_raw.replace("Oraș", "")).strip()

        job_type_el = soup.select_one("span.job-type")
        job_type = job_type_el.get_text(strip=True) if job_type_el else ""

        return company, location, job_type
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return "", "", ""


def parse_job(raw, detail_cache):
    title_raw = raw.get("title", {}).get("rendered", "")
    title = clean_text(title_raw.replace("&#8211;", "-").replace("&#8217;", "'").replace("&#038;", "&").replace("&amp;", "&"))
    link = raw.get("link", "")
    content_html = raw.get("content", {}).get("rendered", "")
    content_html = content_html.replace("&#8211;", "-").replace("&#8217;", "'").replace("&#038;", "&").replace("&amp;", "&")
    content_text = clean_text(content_html)
    class_list = raw.get("class_list", [])
    meta = raw.get("meta", {})

    company = ""
    location = ""
    job_type = parse_job_type(class_list)

    if link in detail_cache:
        company, location, detail_job_type = detail_cache[link]
        if not job_type:
            job_type = detail_job_type

    city = find_city_in_text(location) if location else ""
    if not city:
        city = find_city_in_text(title)
    county = find_county(city) if city else []

    content_lower = f"{title} {content_text}"
    remote_position = meta.get("_remote_position", 0)
    remote = []
    if remote_position == 1:
        remote = ["remote"]
    else:
        normalized_text = remove_diacritics(content_lower.lower())
        if any(kw in normalized_text for kw in ("remote", "la distanta", "hibrid", "work from home")):
            remote = ["remote"]

    salary_data = parse_salary(content_text)

    return {
        "job_title": title,
        "job_link": link,
        **salary_data,
        "country": "Romania",
        "city": [city] if city else [],
        "county": county if isinstance(county, list) else [county] if county else [],
        "company": company,
        "source": SOURCE,
        "remote": remote,
    }


def fetch_page(page, per_page=100):
    response = requests.get(
        API_URL,
        params={
            "per_page": per_page,
            "page": page,
            "status": "publish",
            "orderby": "date",
            "order": "desc",
        },
        headers=HEADERS,
        timeout=30,
    )

    if response.status_code == 400:
        return [], 0

    response.raise_for_status()
    total_pages = int(response.headers.get("X-WP-TotalPages", 1))
    return response.json(), total_pages


def scrape_usamvjobs():
    companies = {}
    seen_links = set()

    page = 1
    total_pages = None

    while True:
        print(f"Scraping page {page}...")
        jobs, fetched_total_pages = fetch_page(page)

        if total_pages is None:
            total_pages = fetched_total_pages
            print(f"Total pages: {total_pages}")

        if not jobs:
            break

        print(f"  Fetched {len(jobs)} jobs from API")

        urls_to_fetch = []
        for raw_job in jobs:
            link = raw_job.get("link", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            urls_to_fetch.append((link, raw_job))

        print(f"  Fetching details for {len(urls_to_fetch)} jobs...")

        detail_cache = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_to_link = {executor.submit(fetch_detail, link): link for link, _ in urls_to_fetch}
            for fut in as_completed(fut_to_link):
                link = fut_to_link[fut]
                try:
                    company, location, job_type = fut.result()
                    detail_cache[link] = (company, location, job_type)
                except Exception as e:
                    print(f"  Failed for {link}: {e}")

        for link, raw_job in urls_to_fetch:
            parsed = parse_job(raw_job, detail_cache)
            company_name = parsed["company"] or SOURCE

            if company_name not in companies:
                companies[company_name] = {"name": company_name, "logo": None, "jobs": []}
            companies[company_name]["jobs"].append(parsed)

        if page >= total_pages:
            break

        page += 1
        time.sleep(1)

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
    companies = scrape_usamvjobs()

    print(f"Total companies: {len(companies)}")
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
