import requests

url = "http://localhost:8000/generate"

payload = {
    "prompt": "Explain transformers in 3 lines.",
    "max_tokens": 150
}

response = requests.post(url, json=payload)
print(response.json()['output'])
