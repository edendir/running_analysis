import os
import json
import datetime
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def get_runs_from_storage(bucket_name, prefix="raw/strava_runs"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    #filter for json files
    json_blobs = [blob for blob in blobs if blob.name.endswith(".json")]
    if not json_blobs:
        return []

    #sort by updated time
    json_blobs.sort(key=lambda b: b.updated, reverse=True)
    most_recent_blob = json_blobs[0]
    data = json.loads(most_recent_blob.download_as_string())
    return data


def format_pace(min_per_mi):
    if not min_per_mi:
        return None
    minutes = int(min_per_mi)
    seconds = int((min_per_mi - minutes) * 60)
    return f"{minutes}:{seconds:02d}"


def normalize_run_data(runs):
    normalized_runs = []
    for run in runs:
        normalized_run = {
            "id": run.get("id"),
            "name": run.get("name"),
            "distance_miles": round(run.get("distance") / 1609, 2),
            "moving_time_minutes": round(run.get("moving_time") / 60, 2),
            "elapsed_time_minutes": round(run.get("elapsed_time") / 60, 2),
            "total_elevation_gain_meters": run.get("total_elevation_gain"),
            "start_date_local": run.get("start_date_local"),
            "average_pace_min_per_mi": (
                round(26.844 / run.get("average_speed"), 2)
                if run.get("average_speed")
                else None
            ),
            "average_pace_formatted": (
                format_pace(round(26.844 / run.get("average_speed"), 2))
                if run.get("average_speed")
                else None
            ),
            "max_speed_min_per_mi": (
                round(26.844 / run.get("max_speed"), 2)
                if run.get("max_speed")
                else None
            ),
            "max_speed_formatted": (
                format_pace(round(26.844 / run.get("max_speed"), 2))
                if run.get("max_speed")
                else None
            ),
            "average_heartrate_bpm": run.get("average_heartrate"),
            "max_heartrate_bpm": run.get("max_heartrate"),
            "calories_kcal": run.get("calories"),
        }
        normalized_runs.append(normalized_run)
    return normalized_runs


def save_to_gcs(data, bucket_name, prefix="normalized/strava_runs"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}/runs_{now}.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    print(f"Data saved to {bucket_name}/{filename}")


if __name__ == "__main__":
    runs = get_runs_from_storage(GCS_BUCKET_NAME)
    normalized_runs = normalize_run_data(runs)
    for run in normalized_runs[:5]:
        print(
            f"Run on {run['start_date_local']}: {run['distance_miles']} miles at {run['average_pace_formatted']} min/mi"
        )
    save_to_gcs(normalized_runs, GCS_BUCKET_NAME)
