import requests
import json

url = "http://localhost:8888/api/consume"
data = {"topic": "stories"}
response = requests.post(url, json=data)
print(response.json())