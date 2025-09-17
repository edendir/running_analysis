#set 80/20 pace zones. race by the clock, train by the clock.

import json
import datetime

ZONE_BOUNDS = {
    "Z1": (1.30, float('inf')),  #easy
    "Z2": (1.11, 1.299), #bread and butter
    "ZX": (1.00, 1.109), #grey zone
    "Z3": (0.91, 0.999), #tempo
    "ZY": (0.84, 0.909), #threshold
    "Z4": (0.80, 0.839), #interval
    "Z5": (0.0, 0.799) #repetition
    }


def load_zone_config(path="scripts/zone_config.json"):
    with open(path, "r") as f:
        config = json.load(f)
    return config

def get_threshold(config, run_date):
    activity_date = datetime.datetime.strptime(run_date, "%Y-%m-%dT%H:%M:%SZ").date()
    config_sorted = sorted(config, key=lambda x: x["effective_date"], reverse=True)
    for entry in config_sorted:
        effective_date = datetime.datetime.strptime(entry["effective_date"], "%Y-%m-%d").date()
        if activity_date >= effective_date:
            return entry["threshold_pace_min_per_mile"]
    raise ValueError("No applicable threshold found for the given date.")

def calc_zones(threshold_pace_min_per_mile):
    zones = {}
    for zone, (lower_mult, upper_mult) in ZONE_BOUNDS.items():
        lower_bound = round(threshold_pace_min_per_mile * lower_mult, 2)
        upper_bound = round(threshold_pace_min_per_mile * upper_mult, 2) if upper_mult != float('inf') else None
        zones[zone] = {"min_pace": lower_bound, "max_pace": upper_bound}
    return zones

def classify_pace(pace, zones):
    for zone, bounds in zones.items():
        min_pace = bounds["min_pace"]
        max_pace = bounds["max_pace"]
        if (max_pace is None and pace >= min_pace) or (max_pace is not None and min_pace <= pace <= max_pace):
            return zone
    return None