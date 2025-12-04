import requests

url = "http://localhost:8000/generate"

payload = {
    "prompt": "YOU ARE A SQL EXPERT",
    "max_tokens": 150
}

response = requests.post(url, json=payload)
print(response.json()['output'])
