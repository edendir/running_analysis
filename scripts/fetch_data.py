import os
import json
import requests
import datetime
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def get_access_token():
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }
    
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

def get_runs(access_token, max_pages=5):
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    all_runs = []
    
    for page in range(1, max_pages + 1):
        params = {"per_page": 200, "page": 1}

        response = requests.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        activities = response.json()
        runs = [activity for activity in activities if activity["type"] == "Run"]
        all_runs.extend(runs)

        if len(activities) < 200:
            break

    for run in all_runs:
        run["streams"] = get_streams(access_token, run["id"])

    return all_runs

def get_streams(access_token, activity_id):
    url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "keys": "time,latlng,altitude,heartrate,cadence,velocity_smooth",
        "key_by_type": "true"
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def save_to_gcs(data, bucket_name, prefix="raw/strava_runs"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}/runs_{now}.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data, indent=2), content_type='application/json')
    print(f"Data saved to {bucket_name}/{filename}")

if __name__ == "__main__":
    token = get_access_token()
    runs = get_runs(token)
    for run in runs[:5]: 
        print(f"Run on {run['start_date']}: {run['distance']} meters")
    save_to_gcs(runs, GCS_BUCKET_NAME)