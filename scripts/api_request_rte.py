import requests
import json
from datetime import datetime, timedelta
from config import RTE_API_URL

today = datetime.utcnow().date()
yesterday = today - timedelta(days=1)

params = {
    "where": f"date = '{yesterday}'",
    "limit": 100
}

response = requests.get(RTE_API_URL, params=params)
data = response.json()["results"]

with open("../data_samples/rte_sample.json", "w") as f:
    json.dump(data, f, indent=2)
