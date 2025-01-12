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
