import requests
import json

url = "http://localhost:8888/api/produce"
data = {
    "topic": "stories",
    "message": "Hello Kafka-like queue",
    "msg_version": "1.0",
    "priority": 1
}
response = requests.post(url, json=data)
print(response.status_code)