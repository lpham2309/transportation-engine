# Boston Reliability Engine
## Project Proposal

**Author:** Lam Pham
**Date:** March 2026
**Version:** 2.0

---

## Executive Summary

The Boston Reliability Engine is a data analytics platform that measures and compares transportation reliability across multiple modes in the Greater Boston area. By analyzing real-time MBTA transit data, Google Maps driving conditions, and NOAA weather patterns, the platform provides actionable insights for commuters to make informed transportation decisions.

The platform answers the fundamental question: **"What is the most reliable way to get from point A to point B in Boston, given current conditions?"**

---

## Problem Statement

Boston commuters face daily uncertainty when choosing between transportation modes:

- **MBTA riders** experience unpredictable delays, especially during adverse weather
- **Drivers** encounter variable traffic conditions that change by time of day and season
- **No unified platform** exists to compare reliability across transportation modes

Current solutions (Google Maps, Transit apps) focus on real-time ETAs but lack:
- Historical reliability metrics
- Weather impact analysis
- Mode-to-mode comparison for the same route
- Personalized route tracking over time

---

## Solution Architecture

### High-Level Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Sources  │     │  Data Platform  │     │   Consumers     │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ MBTA API        │────▶│ S3 Landing Zone │────▶│ React Dashboard │
│ Google Maps API │────▶│ Delta Live Tbls │────▶│ Routes API      │
│ NOAA API        │────▶│ Databricks      │────▶│ Analytics       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Medallion Architecture (Bronze → Silver → Gold)

| Layer  | Purpose | Tables |
|--------|---------|--------|
| **Bronze** | Raw ingestion from APIs | `bronze_mbta_predictions`, `bronze_mbta_schedules`, `bronze_noaa_weather`, `bronze_driving_routes` |
| **Silver** | Cleaned, joined, enriched | `stg_mbta_performance`, `stg_weather_daily`, `stg_driving_performance` |
| **Gold** | Business metrics & facts | `fct_mbta_route_reliability`, `fct_driving_route_reliability`, `fct_mode_comparison`, `fct_weather_impact` |

---

## Data Sources

### 1. MBTA V3 API
- **Data:** Real-time predictions, scheduled arrivals
- **Routes:** Red, Orange, Blue, Green Lines + Silver Line
- **Frequency:** Every pipeline run (daily batch, future: streaming)
- **Key Fields:** `prediction_id`, `arrival_time`, `stop_id`, `route_id`, `trip_id`

### 2. Google Maps Directions API
- **Data:** Driving duration with traffic
- **Routes:** User-defined origin/destination pairs
- **Frequency:** Daily at peak hours (8 AM departure)
- **Key Fields:** `duration_seconds`, `duration_in_traffic_seconds`, `distance_meters`

### 3. NOAA Climate Data Online (CDO)
- **Data:** Daily weather observations
- **Station:** Boston Logan Airport (GHCND:USW00014739)
- **Frequency:** Daily
- **Key Fields:** `precipitation_inches`, `snow_inches`, `temp_max_f`, `wind_speed_mph`

---

## Key Features

### 1. Reliability Score Calculation

> **TODO:** The reliability formula is under revision. We are evaluating industry-standard metrics:
> - **FHWA Travel Time Index (TTI):** `avg_travel_time / free_flow_time`
> - **FHWA Buffer Time Index (BTI):** `(p95_time - avg_time) / avg_time`
> - **FHWA Planning Time Index (PTI):** `p95_travel_time / free_flow_time`
> - **MBTA On-Time Performance (OTP):** Standard 5-minute threshold

**Current metrics collected:**
- On-time rate (arrivals within 5 minutes of schedule)
- Percentile delays: p50, p90, p95
- Mean and standard deviation of delays

**References:**
- [FHWA Travel Time Reliability Measures](https://ops.fhwa.dot.gov/perf_measurement/reliability_measures/index.htm)
- [Transit Capacity and Quality of Service Manual (TCQSM)](https://www.trb.org/Main/Blurbs/169437.aspx)

### 2. Weather Impact Analysis

Quantifies how weather affects reliability:

| Condition | Typical Impact |
|-----------|----------------|
| Clear | Baseline (0% impact) |
| Light Rain | +5-10% delay |
| Heavy Rain | +15-25% delay |
| Snow | +30-50% delay |
| Extreme Cold (<20°F) | +10-20% delay |

### 3. Time-of-Day Stratification

All metrics are segmented by:
- **AM Peak:** 6:00 AM - 9:00 AM
- **Midday:** 9:00 AM - 4:00 PM
- **PM Peak:** 4:00 PM - 7:00 PM
- **Off-Peak:** 7:00 PM - 6:00 AM

### 4. Mode Comparison

Direct comparison between MBTA and driving for overlapping routes:
- Which mode is faster on average?
- Which mode is more reliable (lower variance)?
- How does weather affect each mode differently?

### 5. Geospatial Analysis (H3 Indexing)

Uses Uber's H3 hexagonal grid system for:
- Aggregating performance by geographic area
- Identifying reliability "hot spots" and "cold spots"
- Enabling location-based queries and visualizations

---

## Technical Implementation

### Technology Stack

| Component | Technology |
|-----------|------------|
| **Orchestration** | Apache Airflow (Astronomer) |
| **Data Lake** | AWS S3 |
| **Processing** | Databricks + Delta Live Tables |
| **Storage Format** | Delta Lake |
| **API** | FastAPI (Python) |
| **Frontend** | React (planned) |
| **Geospatial** | H3 (Uber's hexagonal indexing) |

### Project Structure

```
transportation-engine/
├── dags/
│   └── boston_reliability_databricks_dag.py  # Airflow orchestration
├── databricks/
│   ├── api/                    # Routes Management API
│   │   ├── main.py
│   │   ├── models/             # Pydantic models
│   │   ├── controllers/        # Business logic
│   │   └── resources/          # API endpoints
│   ├── ingestion/
│   │   └── s3_writer.py        # API → S3 ingestion
│   └── pipelines/
│       ├── bronze_layer.py     # DLT bronze tables
│       ├── silver_layer.py     # DLT silver tables
│       └── gold_layer.py       # DLT gold tables
├── docs/
└── requirements.txt
```

### Routes Management API

RESTful API for users to configure routes to track:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/routes` | GET | List all tracked routes |
| `/routes` | POST | Create new route (address or coordinates) |
| `/routes/{id}` | GET | Get route details |
| `/routes/{id}` | DELETE | Remove route |
| `/geocode` | POST | Convert address to coordinates |

**Features:**
- Accepts address input (geocoded via Google Maps)
- Accepts browser geolocation coordinates
- Stores routes in S3 for pipeline consumption

---

## Data Pipeline

### Daily Pipeline Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Airflow   │───▶│  Ingest to  │───▶│  Trigger    │───▶│  Validate   │
│   Trigger   │    │  S3 (JSON)  │    │  DLT        │    │  Output     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
     1 AM              Parallel           Bronze →          Quality
                       MBTA, NOAA,        Silver →          Checks
                       Driving            Gold
```

### Delta Live Tables Pipeline

```python
# Bronze: Raw ingestion with Auto Loader
@dlt.table(name="bronze_mbta_predictions")
def bronze_mbta_predictions():
    return spark.readStream.format("cloudFiles")...

# Silver: Joins and enrichment
@dlt.table(name="stg_mbta_performance")
@dlt.expect_or_drop("valid_delay", "delay_seconds IS NOT NULL")
def stg_mbta_performance():
    # Join predictions with schedules
    # Calculate delay_seconds
    # Enrich with weather data

# Gold: Business metrics
@dlt.table(name="fct_mbta_route_reliability")
def fct_mbta_route_reliability():
    # Aggregate by route, time, weather
    # Calculate reliability scores
```

---

## Sample Queries

### Most Reliable MBTA Routes
```sql
SELECT
    route_id,
    route_long_name,
    reliability_score,
    on_time_rate,
    trip_count
FROM fct_mbta_route_reliability
WHERE day_type = 'weekday'
  AND hour_bucket = 'AM_PEAK'
ORDER BY reliability_score DESC
LIMIT 10;
```

### Weather Impact on Commute
```sql
SELECT
    weather_condition,
    AVG(reliability_score) as avg_reliability,
    AVG(mean_delay_sec) as avg_delay_seconds
FROM fct_mbta_route_reliability
GROUP BY weather_condition
ORDER BY avg_reliability DESC;
```

### MBTA vs Driving Comparison
```sql
SELECT
    h3_index,
    mbta_reliability_score,
    driving_reliability_score,
    CASE
        WHEN mbta_reliability_score > driving_reliability_score THEN 'MBTA'
        ELSE 'Driving'
    END as recommended_mode
FROM fct_mode_comparison
WHERE hour_bucket = 'AM_PEAK';
```

---

## Future Enhancements

### Phase 2: Real-Time Streaming
- Ingest MBTA predictions every 60 seconds
- Near real-time reliability updates
- Live dashboard with current conditions

### Phase 3: AI-Powered Recommendations
- ML model to predict delays based on conditions
- Personalized commute recommendations
- "Leave now" vs "wait 10 minutes" suggestions

### Phase 4: Additional Modes
- Bluebikes bike-share integration
- Walking/pedestrian routes
- Multi-modal journey planning

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Data freshness | < 24 hours for batch, < 5 min for streaming |
| Pipeline reliability | > 99% successful runs |
| API latency | < 200ms p95 |
| Route coverage | All MBTA rapid transit + 100 driving routes |
| User-defined routes | Support unlimited custom routes |

---

## Conclusion

The Boston Reliability Engine transforms raw transportation data into actionable reliability insights. By leveraging modern data engineering practices (Delta Live Tables, medallion architecture, streaming), the platform provides Boston commuters with the information they need to make smarter transportation decisions.

The modular architecture enables future expansion to real-time streaming, AI recommendations, and additional transportation modes while maintaining data quality and pipeline reliability.
