import unicodedata
import requests

# Check if a character has diacritics
def has_diacritics(char):
    return any(unicodedata.combining(c) for c in char)

# Remove diacritics from a string
def remove_diacritics(input_string):
    normalized_string = unicodedata.normalize("NFD", input_string)
    return "".join(char for char in normalized_string if not has_diacritics(char))

# Headers for the requests
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
}

# Get the token from the API
def get_token():
    url = "https://api.peviitor.ro/v5/get_token/"
    payload = {
        "email": "contact@laurentiumarian.ro"
    }

    response = requests.post(url, json=payload, headers=headers)

    return response.json().get("access")

# Publish the jobs to the API
def publish_jobs(lst, token):
    url = "https://api.peviitor.ro/v5/add/"
    headers["Authorization"] = f"Bearer {token}"

    response = requests.post(url, json=lst, headers=headers)

    try:
        return response.json()
    except:
        return []
    
  # Main function
def main(obj, token):
    jobs = publish_jobs(obj, token)

    if not jobs:
        return
    
    if isinstance(jobs, list):
      for job in jobs:
          job["published"] = True

      url = "https://api.laurentiumarian.ro/jobs/publish/"
      headers["Authorization"] = f"Bearer {token}"
      response = requests.post(url, json=jobs, headers=headers)

      if response.status_code == 200:
          print(f"{len(jobs)} jobs published successfully for company {obj[0].get('company')}")
      else:
          print(f"Jobs not published for company {obj[0].get('company')}")


def remove_company(company_name: str, token: str):
    """
    Remove a company via the API and return the parsed JSON response.
    Raises requests.RequestException on network/HTTP errors.
    """
    url = "https://api.laurentiumarian.ro/companies/delete/"
    headers_local = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    payload = {"company": company_name}

    try:
        response = requests.post(url, json=payload, headers=headers_local)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error removing company '{company_name}': {e}")

    try:
        return response.json()
    except ValueError:
        # If the response is not JSON, return a simple fallback with status and text
        return {"status_code": response.status_code, "text": response.text}


class GetCounty:
    _counties = []

    def get_county(self, city):

        for county in self.counties:
            if county.get("city") == city:
                return county.get("county")
            
        api_endpoint = f"https://api.laurentiumarian.ro/orase/?search={remove_diacritics(city)}&page_size=50"
        counties_found = []

        response = requests.get(api_endpoint).json()

        while response and response.get("next"):
            counties_found.extend(response.get("results"))
            response = requests.get(response.get("next")).json()
        else:
            if response:
                counties_found.extend(response.get("results"))

        self.counties.append(
            {
                "city": city,
                "county": [
                    item.get("county")
                    for item in counties_found
                    if item.get("name").lower()
                      == remove_diacritics(city.lower())
                ],
            }
        )

        return self.counties[-1].get("county") if self.counties[-1].get("county") else None
    
    @property
    def counties(self):
        return self._counties
    
    @counties.setter
    def counties(self, value):
        self._counties.extend(value)


