import os
import json
import datetime
import zones
from dotenv import load_dotenv
from google.cloud import storage


load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
zone_config = zones.load_zone_config()

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
        # date_str = run.get("start_date_local", "").split("T")[0]
        # threshold_pace = zones.get_threshold(zone_config, date_str)
        # run_zones = zones.calc_zones(threshold_pace)
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

def normalize_run_streams(runs, zone_config):
    records = []
    
    for run in runs:
        stream_data = run.get("streams")
        run_id = run.get("id")
        start_time = run.get("start_date_local")
        threshold_pace = zones.get_threshold(zone_config, start_time)
        run_zones = zones.calc_zones(threshold_pace)

        speed_stream = stream_data.get("velocity_smooth", {})
        speeds = speed_stream.get("data", [])
        time_stream = stream_data.get("time", {})
        timestamps = time_stream.get("data", list(range(len(speeds))))

        for t, speed in zip(timestamps, speeds):
            pace_min_per_mi = 26.844 / speed if speed > 0 else None
            pace_zone = zones.classify_pace(pace_min_per_mi, run_zones) if pace_min_per_mi else None
            record = {
                "run_id": run_id,
                "timestamp": t,
                "speed_mph": round(speed * 2.237, 2),  # convert m/s to mph
                "pace_min_per_mi": round(pace_min_per_mi, 2) if pace_min_per_mi else None,
                "pace_zone": pace_zone,
            }
            records.append(record)

    return records

def save_summary_to_gcs(data, bucket_name, prefix="clean/strava_runs"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}/runs_summary_{now}.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    print(f"Data saved to {bucket_name}/{filename}")

def save_stream_to_gcs(data, bucket_name, prefix="clean/strava_runs"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}/runs_stream_{now}.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    print(f"Data saved to {bucket_name}/{filename}")

if __name__ == "__main__":
    runs = get_runs_from_storage(GCS_BUCKET_NAME)
    print(f"Fetched {len(runs)} runs from GCS.")
    normalized_runs = normalize_run_data(runs)
    print(f"Normalized {len(normalized_runs)} run summaries.")
    normalized_streams = normalize_run_streams(runs, zone_config)
    print(f"Normalized run streams for {len(runs)} runs.")
    # for run in normalized_runs[:5]:
    #     print(
    #         f"Run on {run['start_date_local']}: {run['distance_miles']} miles at {run['average_pace_formatted']} min/mi"
    #     )
    save_summary_to_gcs(normalized_runs, GCS_BUCKET_NAME)
    save_stream_to_gcs(normalized_streams, GCS_BUCKET_NAME)
    print("Data transformation complete.")
