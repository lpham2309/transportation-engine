# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %pip install h3
# MAGIC %pip install googlemaps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %md
# MAGIC
# MAGIC # Boston Reliability Engine - Data Ingestion
# MAGIC
# MAGIC This notebook fetches data from external APIs and writes to Databricks Volume for DLT processing.
# MAGIC
# MAGIC **Schedule:** Run via Databricks Workflows before DLT pipeline
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - MBTA API (predictions, schedules)
# MAGIC - Open-Meteo API (weather)
# MAGIC - Google Maps API (driving routes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
import json
import requests
import h3
from datetime import datetime, timedelta

# Get secrets from Databricks secret scope
MBTA_API_KEY = dbutils.secrets.get(scope="boston-reliability", key="mbta-api-key")
GOOGLE_MAPS_API_KEY = dbutils.secrets.get(scope="boston-reliability", key="google-maps-api-key")

# Databricks Volume Configuration
VOLUME_PATH = "/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine"
RAW_BASE_PATH = f"{VOLUME_PATH}/raw"

# API Endpoints
MBTA_API_BASE_URL = "https://api-v3.mbta.com"
OPENMETEO_API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Boston coordinates for Open-Meteo
BOSTON_LATITUDE = 42.3601
BOSTON_LONGITUDE = -71.0589

# MBTA Routes to track
MBTA_ROUTES = ["Red", "Orange", "Green-B", "Green-C", "Green-D", "Green-E", "Blue", "741", "742", "743"]

# Get execution date from widget or default to today
dbutils.widgets.text("execution_date", datetime.now().strftime("%Y-%m-%d"))
EXECUTION_DATE = dbutils.widgets.get("execution_date")

print(f"Ingesting data for: {EXECUTION_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def write_to_volume(records: list, prefix: str, partition_date: str):
    """Write records to Databricks Volume as newline-delimited JSON (NDJSON)."""
    if not records:
        print(f"No records to write for {prefix}")
        return None

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = f"{RAW_BASE_PATH}/{prefix}/{partition_date}/{timestamp}.json"

    # Convert to NDJSON
    ndjson_content = "\n".join(json.dumps(record, default=str) for record in records)

    # Write using dbutils
    dbutils.fs.put(path, ndjson_content, overwrite=True)

    print(f"Wrote {len(records)} records to {path}")
    return path

# COMMAND ----------

# MAGIC %md
# MAGIC ## MBTA Predictions

# COMMAND ----------

def ingest_mbta_predictions(execution_date: str) -> dict:
    """Fetch MBTA real-time predictions and write to Volume."""
    print(f"\n{'='*60}")
    print(f"INGESTING MBTA PREDICTIONS FOR {execution_date}")
    print(f"{'='*60}")

    url = f"{MBTA_API_BASE_URL}/predictions"
    params = {
        "page[limit]": 500,
        "filter[route]": ",".join(MBTA_ROUTES),
        "include": "stop,route,trip",
        "sort": "-arrival_time"
    }
    if MBTA_API_KEY:
        params["api_key"] = MBTA_API_KEY

    print("Fetching predictions from MBTA API...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    predictions = data.get("data", [])
    included = {item["id"]: item for item in data.get("included", [])}
    print(f"Fetched {len(predictions)} predictions")

    # Process predictions with H3 enrichment
    records = []
    for pred in predictions:
        attrs = pred.get("attributes", {})
        rels = pred.get("relationships", {})

        stop_id = rels.get("stop", {}).get("data", {}).get("id")
        stop_data = included.get(stop_id, {}).get("attributes", {})
        route_id = rels.get("route", {}).get("data", {}).get("id")
        route_data = included.get(route_id, {}).get("attributes", {})

        lat = stop_data.get("latitude")
        lon = stop_data.get("longitude")
        h3_index = h3.latlng_to_cell(lat, lon, 8) if lat and lon else None

        records.append({
            "prediction_id": pred.get("id"),
            "arrival_time": attrs.get("arrival_time"),
            "departure_time": attrs.get("departure_time"),
            "direction_id": attrs.get("direction_id"),
            "schedule_relationship": attrs.get("schedule_relationship"),
            "status": attrs.get("status"),
            "stop_id": stop_id,
            "stop_name": stop_data.get("name"),
            "stop_latitude": lat,
            "stop_longitude": lon,
            "stop_h3_index": h3_index,
            "route_id": route_id,
            "route_long_name": route_data.get("long_name"),
            "route_type": route_data.get("type"),
            "trip_id": rels.get("trip", {}).get("data", {}).get("id"),
            "fetched_at": datetime.utcnow().isoformat(),
            "fetched_date": execution_date
        })

    volume_path = write_to_volume(records, "mbta_predictions", execution_date)
    return {"records": len(records), "volume_path": volume_path}

# COMMAND ----------

# MAGIC %md
# MAGIC ## MBTA Schedules

# COMMAND ----------

def ingest_mbta_schedules(execution_date: str) -> dict:
    """Fetch MBTA scheduled arrivals and write to Volume."""
    print(f"\n{'='*60}")
    print(f"INGESTING MBTA SCHEDULES FOR {execution_date}")
    print(f"{'='*60}")

    url = f"{MBTA_API_BASE_URL}/schedules"
    params = {
        "page[limit]": 500,
        "filter[date]": execution_date,
        "filter[route]": ",".join(MBTA_ROUTES),
        "include": "stop,route,trip",
        "sort": "arrival_time"
    }
    if MBTA_API_KEY:
        params["api_key"] = MBTA_API_KEY

    print("Fetching schedules from MBTA API...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    schedules = data.get("data", [])
    included = {item["id"]: item for item in data.get("included", [])}
    print(f"Fetched {len(schedules)} schedules")

    records = []
    for sched in schedules:
        attrs = sched.get("attributes", {})
        rels = sched.get("relationships", {})

        stop_id = rels.get("stop", {}).get("data", {}).get("id")
        stop_data = included.get(stop_id, {}).get("attributes", {})
        route_id = rels.get("route", {}).get("data", {}).get("id")
        route_data = included.get(route_id, {}).get("attributes", {})

        lat = stop_data.get("latitude")
        lon = stop_data.get("longitude")
        h3_index = h3.latlng_to_cell(lat, lon, 8) if lat and lon else None

        records.append({
            "schedule_id": sched.get("id"),
            "arrival_time": attrs.get("arrival_time"),
            "departure_time": attrs.get("departure_time"),
            "direction_id": attrs.get("direction_id"),
            "drop_off_type": attrs.get("drop_off_type"),
            "pickup_type": attrs.get("pickup_type"),
            "stop_sequence": attrs.get("stop_sequence"),
            "timepoint": attrs.get("timepoint"),
            "stop_id": stop_id,
            "stop_name": stop_data.get("name"),
            "stop_latitude": lat,
            "stop_longitude": lon,
            "stop_h3_index": h3_index,
            "route_id": route_id,
            "route_long_name": route_data.get("long_name"),
            "route_type": route_data.get("type"),
            "trip_id": rels.get("trip", {}).get("data", {}).get("id"),
            "schedule_date": execution_date,
            "fetched_at": datetime.utcnow().isoformat()
        })

    volume_path = write_to_volume(records, "mbta_schedules", execution_date)
    return {"records": len(records), "volume_path": volume_path}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open-Meteo Weather

# COMMAND ----------

def ingest_openmeteo_weather(execution_date: str) -> dict:
    """Fetch Open-Meteo weather data for Boston and write to Volume."""
    print(f"\n{'='*60}")
    print(f"INGESTING OPEN-METEO WEATHER FOR {execution_date}")
    print(f"{'='*60}")

    params = {
        "latitude": BOSTON_LATITUDE,
        "longitude": BOSTON_LONGITUDE,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/New_York",
        "start_date": execution_date,
        "end_date": execution_date
    }

    print("Fetching weather data from Open-Meteo API...")
    response = requests.get(OPENMETEO_API_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    daily = data.get("daily", {})
    print(f"Fetched weather data for {execution_date}")

    # Extract values (Open-Meteo returns arrays, get first element)
    temp_max = daily.get("temperature_2m_max", [None])[0]
    temp_min = daily.get("temperature_2m_min", [None])[0]
    precipitation = daily.get("precipitation_sum", [None])[0]
    snowfall = daily.get("snowfall_sum", [None])[0]
    wind_speed = daily.get("wind_speed_10m_max", [None])[0]

    # Determine weather condition
    snow = snowfall or 0
    precip = precipitation or 0

    if snow > 0:
        weather_condition = "snow"
    elif precip > 0.1:
        weather_condition = "rain"
    else:
        weather_condition = "clear"

    weather_record = {
        "observation_date": execution_date,
        "station_id": "openmeteo_boston",
        "station_name": "Boston (Open-Meteo)",
        "precipitation_inches": precipitation,
        "temp_max_f": temp_max,
        "temp_min_f": temp_min,
        "snow_inches": snowfall,
        "snow_depth_inches": None,  # Not available in Open-Meteo
        "wind_speed_mph": wind_speed,
        "weather_condition": weather_condition,
        "fetched_at": datetime.utcnow().isoformat()
    }

    volume_path = write_to_volume([weather_record], "openmeteo_weather", execution_date)
    return {"records": 1, "volume_path": volume_path}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driving Routes
# MAGIC
# MAGIC Loads routes from Volume config (set via Routes API) and fetches driving times.

# COMMAND ----------

# Local config file path (relative to this notebook)
# This file is used as fallback if no config exists in Volume
LOCAL_CONFIG_PATH = "config/driving_routes.json"

# Volume config path (persisted config that can be modified without code changes)
VOLUME_CONFIG_PATH = f"{VOLUME_PATH}/config/driving_routes.json"


def load_driving_routes() -> list:
    """
    Load driving routes configuration.

    Priority:
    1. Load from Volume config if it exists
    2. If not, load from local config file and upload to Volume for future use
    """
    # Try to load from Volume first
    try:
        content = dbutils.fs.head(VOLUME_CONFIG_PATH, 1000000)
        data = json.loads(content)
        routes = [r for r in data.get("routes", []) if r.get("is_active", True)]
        print(f"Loaded {len(routes)} routes from Volume: {VOLUME_CONFIG_PATH}")
        return routes
    except Exception as e:
        print(f"No config found in Volume: {e}")

    # Fallback: Load from local config file
    print(f"Loading from local config: {LOCAL_CONFIG_PATH}")
    try:
        # Get the directory of the current notebook/script
        import os
        notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else "/Workspace/Repos"
        local_file_path = os.path.join(notebook_dir, LOCAL_CONFIG_PATH)

        # In Databricks, read from workspace files
        with open(local_file_path, "r") as f:
            local_config = json.load(f)
    except Exception:
        # Fallback for Databricks notebook environment
        try:
            local_content = dbutils.fs.head(f"file:{LOCAL_CONFIG_PATH}", 1000000)
            local_config = json.loads(local_content)
        except Exception as local_error:
            print(f"Error loading local config: {local_error}")
            return []

    # Upload local config to Volume for future use
    try:
        local_config["metadata"] = local_config.get("metadata", {})
        local_config["metadata"]["uploaded_at"] = datetime.utcnow().isoformat()
        config_content = json.dumps(local_config, indent=2)
        dbutils.fs.put(VOLUME_CONFIG_PATH, config_content, overwrite=True)
        print(f"Uploaded local config to Volume: {VOLUME_CONFIG_PATH}")
    except Exception as upload_error:
        print(f"Warning: Could not upload config to Volume: {upload_error}")

    routes = [r for r in local_config.get("routes", []) if r.get("is_active", True)]
    print(f"Using {len(routes)} routes from local config")
    return routes

def ingest_driving_routes(execution_date: str) -> dict:
    """Fetch Google Maps driving times for configured routes."""
    print(f"\n{'='*60}")
    print(f"INGESTING DRIVING ROUTES FOR {execution_date}")
    print(f"{'='*60}")

    if not GOOGLE_MAPS_API_KEY:
        print("WARNING: GOOGLE_MAPS_API_KEY not set, skipping driving ingestion")
        return {"records": 0, "skipped": True}

    import googlemaps
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

    routes = load_driving_routes()
    print(f"Processing {len(routes)} routes")

    records = []
    for route in routes:
        route_name = route["name"]
        origin = route.get("origin", {})
        destination = route.get("destination", {})

        orig_lat = origin.get("latitude")
        orig_lon = origin.get("longitude")
        dest_lat = destination.get("latitude")
        dest_lon = destination.get("longitude")

        if not all([orig_lat, orig_lon, dest_lat, dest_lon]):
            print(f"Skipping {route_name}: missing coordinates")
            continue

        print(f"Fetching route: {route_name}")

        try:
            departure_time = datetime.strptime(execution_date, "%Y-%m-%d").replace(hour=8, minute=0)

            result = gmaps.directions(
                origin=(orig_lat, orig_lon),
                destination=(dest_lat, dest_lon),
                mode="driving",
                departure_time=departure_time,
                traffic_model="best_guess"
            )

            if result:
                leg = result[0]["legs"][0]
                duration_in_traffic = leg.get("duration_in_traffic", {}).get("value")

                records.append({
                    "route_name": route_name,
                    "origin_address": origin.get("formatted_address"),
                    "origin_latitude": orig_lat,
                    "origin_longitude": orig_lon,
                    "origin_h3_index": h3.latlng_to_cell(orig_lat, orig_lon, 8),
                    "destination_address": destination.get("formatted_address"),
                    "destination_latitude": dest_lat,
                    "destination_longitude": dest_lon,
                    "destination_h3_index": h3.latlng_to_cell(dest_lat, dest_lon, 8),
                    "distance_meters": leg.get("distance", {}).get("value"),
                    "duration_seconds": leg.get("duration", {}).get("value"),
                    "duration_in_traffic_seconds": duration_in_traffic,
                    "departure_time": departure_time.isoformat(),
                    "fetched_date": execution_date,
                    "fetched_at": datetime.utcnow().isoformat()
                })
        except Exception as e:
            print(f"Error fetching route {route_name}: {e}")
            continue

    volume_path = write_to_volume(records, "driving_routes", execution_date)
    return {"records": len(records), "volume_path": volume_path}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Ingestion

# COMMAND ----------

results = {}

# Ingest all sources
results["mbta_predictions"] = ingest_mbta_predictions(EXECUTION_DATE)
results["mbta_schedules"] = ingest_mbta_schedules(EXECUTION_DATE)
results["openmeteo_weather"] = ingest_openmeteo_weather(EXECUTION_DATE)
results["driving_routes"] = ingest_driving_routes(EXECUTION_DATE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*70)
print("INGESTION COMPLETE")
print("="*70)

for source, stats in results.items():
    status = "✓" if stats.get("records", 0) > 0 else "⚠"
    print(f"  {status} {source}: {stats}")

# Return results for workflow
dbutils.notebook.exit(json.dumps(results))
