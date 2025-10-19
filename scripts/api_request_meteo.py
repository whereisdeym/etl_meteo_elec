import requests
import json
from datetime import datetime
from config import METEO_API_URL, METEO_API_KEY

regions = {
    "Ile-de-France": {"lat": 48.8566, "lon": 2.3522},
    "Occitanie": {"lat": 43.6047, "lon": 1.4442}
}

data = []
for region, coords in regions.items():
    resp = requests.get(
        METEO_API_URL,
        params={"lat": coords["lat"], "lon": coords["lon"], "appid": METEO_API_KEY, "units": "metric"}
    )
    json_data = resp.json()
    data.append({
        "region": region,
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "temperature": json_data["main"]["temp"],
        "humidity": json_data["main"]["humidity"],
        "wind_speed": json_data["wind"]["speed"]
    })

with open("../data_samples/meteo_sample.json", "w") as f:
    json.dump(data, f, indent=2)
