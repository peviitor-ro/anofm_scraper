import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

from utils import GetCounty, get_token, main, remove_company, remove_diacritics

RSS_URL = "https://devjob.ro/rss"
SOURCE = "DEVJOBRO"
HEADERS = {
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
    "Buftea", "Popesti-Leordeni", "Chitila", "Magurele", "Alexandria",
    "Barlad", "Falticeni", "Radauti", "Campulung Moldovenesc", "Dorohoi",
    "Tecuci", "Moinesti", "Onesti", "Campia-Turzii", "Dej", "Gherla",
    "Ludus", "Reghin", "Sighisoara", "Sovata", "Aiud", "Sebes", "Blaj",
    "Caransebes", "Simeria", "Orastie", "Petrila", "Motru", "Dragasani",
    "Bals", "Corabia", "Calafat", "Filiasi", "Videle", "Rosiori de Vede",
    "Turnu Magurele", "Urlati", "Mizil", "Boldesti-Scaeni", "Moreni",
    "Pucheni", "Targu Frumos", "Harlau", "Pascani", "Marasesti",
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
    "Ilfov": ["Otopeni", "Voluntari", "Pantelimon", "Buftea", "Magurele", "Chitila"],
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


def parse_item(item):
    title = item.findtext("title", "")
    link = item.findtext("link", "")
    description = item.findtext("description", "")

    if not title or not link:
        return None

    clean_link = link.split("?")[0]

    company = ""
    title_text = title

    company_match = re.search(r"@\s*(.+?)\s*\[", title)
    if company_match:
        company = company_match.group(1).strip()
        title_text = title[: title.index("@")].strip()
    else:
        company_match_no_salary = re.search(r"@\s*(.+)", title)
        if company_match_no_salary:
            company = company_match_no_salary.group(1).strip()
            title_text = title[: title.index("@")].strip()
        else:
            company = "DevJob.ro"

    title_text = re.sub(r"\s*\[.*?\]\s*$", "", title_text).strip()

    salary_data = {}
    salary_match = re.search(r"\[([^\]]+)\]", title)
    if salary_match:
        salary_raw = salary_match.group(1)
        salary_parts = re.findall(r"[\d.]+", salary_raw)
        currency = "RON" if "RON" in salary_raw.upper() else ("EUR" if "EUR" in salary_raw.upper() else "")
        if len(salary_parts) >= 2 and currency:
            num_min = int(salary_parts[0].replace(".", ""))
            num_max = int(salary_parts[1].replace(".", ""))
            if num_min > num_max:
                num_min, num_max = num_max, num_min
            salary_data = {"salary_min": num_min, "salary_max": num_max, "salary_currency": currency}
        elif len(salary_parts) == 1 and currency:
            num = int(salary_parts[0].replace(".", ""))
            salary_data = {"salary_min": num, "salary_max": num, "salary_currency": currency}

    desc_text = remove_diacritics(BeautifulSoup(description or "", "html.parser").get_text(" ", strip=True)).lower()

    remote = []
    if "remote" in title.lower() or "100% remote" in desc_text or "fully remote" in desc_text:
        remote = ["remote"]

    city = find_city_in_text(f"{title} {desc_text}")
    county = find_county(city) if city else []

    country = "Romania"
    if "germany" in desc_text or "germania" in desc_text or "deutschland" in desc_text:
        country = "Germany"
    elif "uk " in desc_text or "united kingdom" in desc_text or "england" in desc_text:
        country = "United Kingdom"

    return company, {
        "job_title": title_text,
        "job_link": clean_link,
        **salary_data,
        "country": country,
        "city": [city] if city else [],
        "county": county,
        "company": company,
        "source": SOURCE,
        "remote": remote,
    }


def scrape_devjobro():
    companies = {}
    seen_links = set()

    print(f"Fetching RSS feed: {RSS_URL}")
    response = requests.get(RSS_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    items = root.findall(".//item")
    print(f"Found {len(items)} jobs in RSS feed")

    for item in items:
        parsed = parse_item(item)
        if not parsed:
            continue

        company_name, job_data = parsed
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
    companies = scrape_devjobro()

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
