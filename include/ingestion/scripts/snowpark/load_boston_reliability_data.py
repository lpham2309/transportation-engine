"""
Boston Reliability Engine - Consolidated Data Ingestion Script

Loads data from MBTA, NOAA, Bluebikes, and Google Maps APIs into Snowflake
using Snowpark for all data operations.
"""
import os
import sys

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.insert(0, project_root)

import requests
import h3
import pandas as pd
import googlemaps
import tempfile
import zipfile
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv
from include.ingestion.snowflake_queries import get_snowpark_session, execute_snowflake_query

load_dotenv()

# API Configuration
MBTA_API_BASE_URL = "https://api-v3.mbta.com"
MBTA_API_KEY = os.getenv("MBTA_API_KEY")
NOAA_API_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Boston Logan Airport station ID for NOAA
BOSTON_STATION_ID = "GHCND:USW00014739"


def load_mbta_data(execution_date: str, schema: str):
    """Fetch and load MBTA transit data"""
    print(f"\n{'='*60}")
    print(f"LOADING MBTA DATA FOR {execution_date}")
    print(f"{'='*60}")

    # All MBTA routes
    route_ids = ["Red", "Orange", "Green-B", "Green-C", "Green-D", "Green-E", "Blue", "741", "742", "743"]

    # Fetch predictions
    url = f"{MBTA_API_BASE_URL}/predictions"
    params = {
        "page[limit]": 5000,
        "filter[route]": ",".join(route_ids),
        "include": "stop,route,trip",
        "sort": "-arrival_time"
    }
    if MBTA_API_KEY:
        params["api_key"] = MBTA_API_KEY

    print("Fetching MBTA predictions...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    predictions = data.get("data", [])
    included = {item["id"]: item for item in data.get("included", [])}
    print(f"Fetched {len(predictions)} predictions")

    # Process predictions with H3
    predictions_records = []
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

        predictions_records.append({
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
            "fetched_at": datetime.utcnow().isoformat()
        })

    # Fetch schedules
    url = f"{MBTA_API_BASE_URL}/schedules"
    params = {
        "page[limit]": 5000,
        "filter[date]": execution_date,
        "filter[route]": ",".join(route_ids),
        "include": "stop,route,trip",
        "sort": "arrival_time"
    }
    if MBTA_API_KEY:
        params["api_key"] = MBTA_API_KEY

    print("Fetching MBTA schedules...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    schedules = data.get("data", [])
    included = {item["id"]: item for item in data.get("included", [])}
    print(f"Fetched {len(schedules)} schedules")

    # Process schedules with H3
    schedules_records = []
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

        schedules_records.append({
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

    # Load to Snowflake using Snowpark
    session = get_snowpark_session(schema=schema)

    # Create tables if they don't exist (preserve historical data)
    execute_snowflake_query(f"""
        CREATE TABLE IF NOT EXISTS {schema}.bronze_mbta_predictions (
            prediction_id VARCHAR,
            arrival_time TIMESTAMP,
            departure_time TIMESTAMP,
            direction_id INTEGER,
            schedule_relationship VARCHAR,
            status VARCHAR,
            stop_id VARCHAR,
            stop_name VARCHAR,
            stop_latitude FLOAT,
            stop_longitude FLOAT,
            stop_h3_index VARCHAR,
            route_id VARCHAR,
            route_long_name VARCHAR,
            route_type INTEGER,
            trip_id VARCHAR,
            fetched_at TIMESTAMP
        )
    """)

    execute_snowflake_query(f"""
        CREATE TABLE IF NOT EXISTS {schema}.bronze_mbta_schedules (
            schedule_id VARCHAR,
            arrival_time TIMESTAMP,
            departure_time TIMESTAMP,
            direction_id INTEGER,
            drop_off_type INTEGER,
            pickup_type INTEGER,
            stop_sequence INTEGER,
            timepoint BOOLEAN,
            stop_id VARCHAR,
            stop_name VARCHAR,
            stop_latitude FLOAT,
            stop_longitude FLOAT,
            stop_h3_index VARCHAR,
            route_id VARCHAR,
            route_long_name VARCHAR,
            route_type INTEGER,
            trip_id VARCHAR,
            schedule_date DATE,
            fetched_at TIMESTAMP
        )
    """)

    # Write data
    if predictions_records:
        df = session.create_dataframe(pd.DataFrame(predictions_records))
        df.write.mode("append").save_as_table("bronze_mbta_predictions")
        print(f"✓ Loaded {len(predictions_records)} predictions to Snowflake")

    if schedules_records:
        df = session.create_dataframe(pd.DataFrame(schedules_records))
        df.write.mode("append").save_as_table("bronze_mbta_schedules")
        print(f"✓ Loaded {len(schedules_records)} schedules to Snowflake")

    session.close()
    return {"predictions": len(predictions_records), "schedules": len(schedules_records)}


def load_noaa_weather(execution_date: str, schema: str):
    """Fetch and load NOAA weather data"""
    print(f"\n{'='*60}")
    print(f"LOADING NOAA WEATHER DATA FOR {execution_date}")
    print(f"{'='*60}")

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

    print("Fetching NOAA weather data...")
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    results = data.get("results", [])
    print(f"Fetched {len(results)} observations")

    # Process into single row per date
    weather_data = {
        "observation_date": execution_date,
        "station_id": BOSTON_STATION_ID,
        "station_name": "Boston Logan Airport",
        "precipitation_inches": None,
        "temp_max_f": None,
        "temp_min_f": None,
        "snow_inches": None,
        "snow_depth_inches": None,
        "wind_speed_mph": None,
        "weather_condition": None,  # Will be set below based on precipitation/snow
        "fetched_at": datetime.utcnow().isoformat()
    }

    for obs in results:
        datatype = obs.get("datatype")
        value = obs.get("value")

        if datatype == "PRCP":
            weather_data["precipitation_inches"] = value
        elif datatype == "TMAX":
            weather_data["temp_max_f"] = value
        elif datatype == "TMIN":
            weather_data["temp_min_f"] = value
        elif datatype == "SNOW":
            weather_data["snow_inches"] = value
        elif datatype == "SNWD":
            weather_data["snow_depth_inches"] = value
        elif datatype == "AWND":
            weather_data["wind_speed_mph"] = value

    # Categorize weather
    precip = weather_data["precipitation_inches"] or 0
    snow = weather_data["snow_inches"] or 0

    if snow > 0:
        weather_data["weather_condition"] = "snow"
    elif precip > 0.1:
        weather_data["weather_condition"] = "rain"
    else:
        weather_data["weather_condition"] = "clear"

    # Load to Snowflake
    session = get_snowpark_session(schema=schema)

    # Create table if it doesn't exist (preserve historical data)
    execute_snowflake_query(f"""
        CREATE TABLE IF NOT EXISTS {schema}.bronze_noaa_weather (
            observation_date DATE PRIMARY KEY,
            station_id VARCHAR,
            station_name VARCHAR,
            precipitation_inches FLOAT,
            temp_max_f FLOAT,
            temp_min_f FLOAT,
            snow_inches FLOAT,
            snow_depth_inches FLOAT,
            wind_speed_mph FLOAT,
            weather_condition VARCHAR,
            fetched_at TIMESTAMP
        )
    """)

    # Delete existing record for this date if it exists, then insert
    execute_snowflake_query(f"""
        DELETE FROM {schema}.bronze_noaa_weather
        WHERE observation_date = '{execution_date}'
    """)

    # Insert new data
    df = session.create_dataframe(pd.DataFrame([weather_data]))
    df.write.mode("append").save_as_table("bronze_noaa_weather")
    print(f"✓ Loaded weather data to Snowflake (upserted for {execution_date})")

    session.close()
    return {"observations": 1}


def load_google_maps_driving(execution_date: str, schema: str):
    """Fetch and load Google Maps driving data"""
    print(f"\n{'='*60}")
    print(f"LOADING GOOGLE MAPS DRIVING DATA FOR {execution_date}")
    print(f"{'='*60}")

    if not GOOGLE_MAPS_API_KEY:
        print("⚠️  GOOGLE_MAPS_API_KEY not set, skipping Google Maps ingestion")
        return {"routes": 0, "skipped": True}

    try:
        gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    except Exception as e:
        print(f"⚠️  Error initializing Google Maps client: {e}")
        return {"routes": 0, "error": str(e)}

    # Sample origin-destination pairs in Boston
    od_pairs = [
        (42.3601, -71.0589, 42.3554, -71.0603, "Cambridge_to_Financial_District"),
        (42.3736, -71.1097, 42.3601, -71.0589, "Harvard_to_Back_Bay"),
        (42.3467, -71.0972, 42.3601, -71.0589, "Fenway_to_Back_Bay"),
        (42.3188, -71.0846, 42.3601, -71.0589, "Dorchester_to_Back_Bay"),
    ]

    routes_records = []
    for orig_lat, orig_lon, dest_lat, dest_lon, route_name in od_pairs:
        print(f"Fetching route: {route_name}")

        try:
            departure_time = datetime.strptime(execution_date, "%Y-%m-%d")
            departure_time = departure_time.replace(hour=8, minute=0)  # 8 AM

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

                routes_records.append({
                    "route_name": route_name,
                    "origin_latitude": orig_lat,
                    "origin_longitude": orig_lon,
                    "origin_h3_index": h3.latlng_to_cell(orig_lat, orig_lon, 8),
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
            error_msg = str(e)
            print(f"⚠️  Error fetching route {route_name}: {error_msg}")

            # Check if it's the legacy API error
            if "REQUEST_DENIED" in error_msg or "legacy API" in error_msg:
                print("\n" + "="*70)
                print("⚠️  GOOGLE MAPS API ERROR: Legacy Directions API Not Enabled")
                print("="*70)
                print("\nTo fix this, enable the Directions API in Google Cloud Console:")
                print("1. Go to: https://console.cloud.google.com/apis/library/directions-backend.googleapis.com")
                print("2. Click 'ENABLE' for Directions API")
                print("3. Wait a few minutes for the API to activate")
                print("4. Re-run this task")
                print("\nAlternatively, you can skip Google Maps data by setting:")
                print("  data_source='mbta' or data_source='noaa'")
                print("="*70 + "\n")
                # Return early on API error
                return {"routes": 0, "error": "Directions API not enabled"}
            continue

    print(f"Fetched {len(routes_records)} routes")

    # Load to Snowflake
    session = get_snowpark_session(schema=schema)

    # Create table if it doesn't exist (preserve historical data)
    execute_snowflake_query(f"""
        CREATE TABLE IF NOT EXISTS {schema}.bronze_driving_routes (
            route_name VARCHAR,
            origin_latitude FLOAT,
            origin_longitude FLOAT,
            origin_h3_index VARCHAR,
            destination_latitude FLOAT,
            destination_longitude FLOAT,
            destination_h3_index VARCHAR,
            distance_meters INTEGER,
            duration_seconds INTEGER,
            duration_in_traffic_seconds INTEGER,
            departure_time TIMESTAMP,
            fetched_date DATE,
            fetched_at TIMESTAMP
        )
    """)

    if routes_records:
        df = session.create_dataframe(pd.DataFrame(routes_records))
        df.write.mode("append").save_as_table("bronze_driving_routes")
        print(f"✓ Loaded {len(routes_records)} routes to Snowflake")

    session.close()
    return {"routes": len(routes_records)}


def load_boston_reliability_data(execution_date: str, data_source: str = "all"):
    """
    Main function to load Boston Reliability Engine data.

    Args:
        execution_date: Date in YYYY-MM-DD format
        data_source: Which data source to load (mbta|noaa|google_maps|all)
    """
    schema = os.getenv("STUDENT_SCHEMA") or os.getenv("SCHEMA", "bootcamp")

    print(f"\n{'='*70}")
    print(f"BOSTON RELIABILITY ENGINE - DATA INGESTION")
    print(f"Execution Date: {execution_date}")
    print(f"Data Source: {data_source}")
    print(f"Schema: {schema}")
    print(f"{'='*70}")

    results = {}

    if data_source in ["all", "mbta"]:
        results["mbta"] = load_mbta_data(execution_date, schema)

    if data_source in ["all", "noaa"]:
        results["noaa"] = load_noaa_weather(execution_date, schema)

    if data_source in ["all", "google_maps"]:
        results["google_maps"] = load_google_maps_driving(execution_date, schema)

    print(f"\n{'='*70}")
    print(f"INGESTION COMPLETE")
    print(f"{'='*70}")
    for source, stats in results.items():
        print(f"{source.upper()}: {stats}")

    return results


if __name__ == "__main__":
    # Example usage
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    data_source = sys.argv[2] if len(sys.argv) > 2 else "all"

    load_boston_reliability_data(execution_date, data_source)
