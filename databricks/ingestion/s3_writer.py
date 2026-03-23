"""
Boston Reliability Engine - S3 Data Ingestion

Fetches data from MBTA, NOAA, and Google Maps APIs and writes to S3
for consumption by Delta Live Tables Auto Loader.

This module replaces the Snowpark-based ingestion in:
    include/ingestion/scripts/snowpark/load_boston_reliability_data.py
"""
import os
import json
import boto3
import requests
import h3
import googlemaps
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

S3_BUCKET = os.getenv("S3_BUCKET", "boston-reliability-engine")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# API Configuration
MBTA_API_BASE_URL = "https://api-v3.mbta.com"
MBTA_API_KEY = os.getenv("MBTA_API_KEY")
NOAA_API_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Boston Logan Airport station ID for NOAA
BOSTON_STATION_ID = "GHCND:USW00014739"

# MBTA Routes to track
MBTA_ROUTES = ["Red", "Orange", "Green-B", "Green-C", "Green-D", "Green-E",
               "Blue", "741", "742", "743"]

# Routes configuration path in S3 (managed by Routes API)
ROUTES_CONFIG_KEY = "config/driving_routes.json"

# Default routes (fallback if no routes configured via API)
# Format matches the Routes API response structure
DEFAULT_DRIVING_ROUTES = [
    {
        "name": "Cambridge to Financial District",
        "origin": {
            "formatted_address": "Cambridge, MA",
            "latitude": 42.3601,
            "longitude": -71.0589
        },
        "destination": {
            "formatted_address": "Financial District, Boston, MA",
            "latitude": 42.3554,
            "longitude": -71.0603
        }
    },
    {
        "name": "Harvard to Back Bay",
        "origin": {
            "formatted_address": "Harvard Square, Cambridge, MA",
            "latitude": 42.3736,
            "longitude": -71.1097
        },
        "destination": {
            "formatted_address": "Back Bay, Boston, MA",
            "latitude": 42.3601,
            "longitude": -71.0589
        }
    },
]

# Initialize S3 client
s3_client = boto3.client("s3", region_name=AWS_REGION)


def load_driving_routes() -> List[Dict]:
    """
    Load driving routes from S3 configuration (set via Routes API).
    Falls back to default routes if no configuration exists.

    Returns:
        List of route dictionaries with nested origin/destination:
        {
            "name": "Route Name",
            "origin": {"latitude": float, "longitude": float, "formatted_address": str},
            "destination": {"latitude": float, "longitude": float, "formatted_address": str}
        }
    """
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=ROUTES_CONFIG_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        routes = data.get("routes", [])

        # Filter to active routes only
        active_routes = [r for r in routes if r.get("is_active", True)]

        if active_routes:
            print(f"Loaded {len(active_routes)} routes from S3 config")
            return active_routes
        else:
            print("No active routes in config, using defaults")
            return DEFAULT_DRIVING_ROUTES

    except s3_client.exceptions.NoSuchKey:
        print("No routes config found in S3, using defaults")
        return DEFAULT_DRIVING_ROUTES
    except Exception as e:
        print(f"Error loading routes config: {e}, using defaults")
        return DEFAULT_DRIVING_ROUTES


# =============================================================================
# S3 Utilities
# =============================================================================

def write_to_s3(records: List[Dict], prefix: str, partition_date: str) -> str:
    """
    Write records to S3 as newline-delimited JSON for Auto Loader.

    Args:
        records: List of dictionaries to write
        prefix: S3 key prefix (e.g., 'mbta_predictions')
        partition_date: Date partition in YYYY-MM-DD format

    Returns:
        S3 URI of the written file
    """
    if not records:
        print(f"No records to write for {prefix}")
        return None

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    key = f"raw/{prefix}/{partition_date}/{timestamp}.json"

    # Write as newline-delimited JSON (NDJSON)
    body = "\n".join(json.dumps(record, default=str) for record in records)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    s3_uri = f"s3://{S3_BUCKET}/{key}"
    print(f"Wrote {len(records)} records to {s3_uri}")
    return s3_uri


# =============================================================================
# MBTA Ingestion
# =============================================================================

def ingest_mbta_predictions(execution_date: str) -> Dict:
    """
    Fetch MBTA real-time predictions and write to S3.

    Args:
        execution_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with ingestion statistics
    """
    print(f"\n{'='*60}")
    print(f"INGESTING MBTA PREDICTIONS FOR {execution_date}")
    print(f"{'='*60}")

    url = f"{MBTA_API_BASE_URL}/predictions"
    params = {
        "page[limit]": 100,
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

    s3_path = write_to_s3(records, "mbta_predictions", execution_date)
    return {"records": len(records), "s3_path": s3_path}


def ingest_mbta_schedules(execution_date: str) -> Dict:
    """
    Fetch MBTA scheduled arrivals and write to S3.

    Args:
        execution_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with ingestion statistics
    """
    print(f"\n{'='*60}")
    print(f"INGESTING MBTA SCHEDULES FOR {execution_date}")
    print(f"{'='*60}")

    url = f"{MBTA_API_BASE_URL}/schedules"
    params = {
        "page[limit]": 5000,
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

    # Process schedules with H3 enrichment
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

    s3_path = write_to_s3(records, "mbta_schedules", execution_date)
    return {"records": len(records), "s3_path": s3_path}


# =============================================================================
# NOAA Weather Ingestion
# =============================================================================

def ingest_noaa_weather(execution_date: str) -> Dict:
    """
    Fetch NOAA weather data for Boston and write to S3.

    Args:
        execution_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with ingestion statistics
    """
    print(f"\n{'='*60}")
    print(f"INGESTING NOAA WEATHER FOR {execution_date}")
    print(f"{'='*60}")

    if not NOAA_API_TOKEN:
        print("WARNING: NOAA_API_TOKEN not set, skipping weather ingestion")
        return {"records": 0, "skipped": True}

    url = f"{NOAA_API_BASE_URL}/data"
    headers = {"token": NOAA_API_TOKEN}
    params = {
        "datasetid": "GHCND",
        "stationid": BOSTON_STATION_ID,
        "startdate": execution_date,
        "enddate": execution_date,
        "datatypeid": ["PRCP", "TMAX", "TMIN", "SNOW", "SNWD", "AWND"],
        "units": "standard",
        "limit": 1000
    }

    print("Fetching weather data from NOAA API...")
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    results = data.get("results", [])
    print(f"Fetched {len(results)} observations")

    # Aggregate into single record per date
    weather_record = {
        "observation_date": execution_date,
        "station_id": BOSTON_STATION_ID,
        "station_name": "Boston Logan Airport",
        "precipitation_inches": None,
        "temp_max_f": None,
        "temp_min_f": None,
        "snow_inches": None,
        "snow_depth_inches": None,
        "wind_speed_mph": None,
        "weather_condition": None,
        "fetched_at": datetime.utcnow().isoformat()
    }

    for obs in results:
        datatype = obs.get("datatype")
        value = obs.get("value")

        if datatype == "PRCP":
            weather_record["precipitation_inches"] = value
        elif datatype == "TMAX":
            weather_record["temp_max_f"] = value
        elif datatype == "TMIN":
            weather_record["temp_min_f"] = value
        elif datatype == "SNOW":
            weather_record["snow_inches"] = value
        elif datatype == "SNWD":
            weather_record["snow_depth_inches"] = value
        elif datatype == "AWND":
            weather_record["wind_speed_mph"] = value

    # Categorize weather condition
    precip = weather_record["precipitation_inches"] or 0
    snow = weather_record["snow_inches"] or 0

    if snow > 0:
        weather_record["weather_condition"] = "snow"
    elif precip > 0.1:
        weather_record["weather_condition"] = "rain"
    else:
        weather_record["weather_condition"] = "clear"

    s3_path = write_to_s3([weather_record], "noaa_weather", execution_date)
    return {"records": 1, "s3_path": s3_path}


# =============================================================================
# Google Maps Driving Ingestion
# =============================================================================

def ingest_driving_routes(execution_date: str) -> Dict:
    """
    Fetch Google Maps driving times for configured routes and write to S3.

    Routes are loaded from S3 config (managed via Routes API) or fall back
    to default routes if no config exists.

    Args:
        execution_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with ingestion statistics
    """
    print(f"\n{'='*60}")
    print(f"INGESTING DRIVING ROUTES FOR {execution_date}")
    print(f"{'='*60}")

    if not GOOGLE_MAPS_API_KEY:
        print("WARNING: GOOGLE_MAPS_API_KEY not set, skipping driving ingestion")
        return {"records": 0, "skipped": True}

    try:
        gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    except Exception as e:
        print(f"Error initializing Google Maps client: {e}")
        return {"records": 0, "error": str(e)}

    # Load routes from S3 config (or use defaults)
    driving_routes = load_driving_routes()
    print(f"Processing {len(driving_routes)} routes")

    records = []
    for route in driving_routes:
        route_name = route["name"]
        # Handle nested origin/destination structure from Routes API
        origin = route.get("origin", {})
        destination = route.get("destination", {})

        orig_lat = origin.get("latitude")
        orig_lon = origin.get("longitude")
        dest_lat = destination.get("latitude")
        dest_lon = destination.get("longitude")

        if not all([orig_lat, orig_lon, dest_lat, dest_lon]):
            print(f"Skipping route {route_name}: missing coordinates")
            continue

        print(f"Fetching route: {route_name}")

        try:
            departure_time = datetime.strptime(execution_date, "%Y-%m-%d")
            departure_time = departure_time.replace(hour=8, minute=0)

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

    s3_path = write_to_s3(records, "driving_routes", execution_date)
    return {"records": len(records), "s3_path": s3_path}


# =============================================================================
# Main Ingestion Function
# =============================================================================

def ingest_all_sources(execution_date: str, sources: Optional[List[str]] = None) -> Dict:
    """
    Ingest data from all sources (or specified sources) for a given date.

    Args:
        execution_date: Date in YYYY-MM-DD format
        sources: List of sources to ingest. Options:
                 ['mbta_predictions', 'mbta_schedules', 'noaa', 'driving']
                 If None, ingests all sources.

    Returns:
        Dictionary with ingestion statistics for each source
    """
    all_sources = {
        "mbta_predictions": ingest_mbta_predictions,
        "mbta_schedules": ingest_mbta_schedules,
        "noaa": ingest_noaa_weather,
        "driving": ingest_driving_routes
    }

    sources_to_run = sources or list(all_sources.keys())

    print(f"\n{'='*70}")
    print(f"BOSTON RELIABILITY ENGINE - S3 DATA INGESTION")
    print(f"Execution Date: {execution_date}")
    print(f"Sources: {sources_to_run}")
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"{'='*70}")

    results = {}
    for source in sources_to_run:
        if source in all_sources:
            try:
                results[source] = all_sources[source](execution_date)
            except Exception as e:
                print(f"ERROR ingesting {source}: {e}")
                results[source] = {"error": str(e)}

    print(f"\n{'='*70}")
    print("INGESTION COMPLETE")
    print(f"{'='*70}")
    for source, stats in results.items():
        print(f"  {source}: {stats}")

    return results


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    import sys

    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    sources = sys.argv[2].split(",") if len(sys.argv) > 2 else None

    ingest_all_sources(execution_date, sources)
