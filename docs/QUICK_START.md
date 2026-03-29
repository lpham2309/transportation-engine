# Boston Reliability Engine - Quick Start Guide

## Overview

The Boston Reliability Engine analyzes public transit reliability by comparing MBTA (subway/bus), driving, and weather data.

## Prerequisites

- Docker & Astro CLI installed
- Snowflake account credentials
- API keys for MBTA, NOAA, and Google Maps

## Step 1: Set Up Environment Variables

### For Local Development:

1. **Copy the example file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` and fill in your credentials:**
   ```bash
   # Required Snowflake credentials
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   STUDENT_SCHEMA=your_schema

   # Required API keys
   MBTA_API_KEY=your_mbta_key
   NOAA_API_TOKEN=your_noaa_token
   GOOGLE_MAPS_API_KEY=your_google_maps_key
   ```

3. **Get API Keys:**
   - **MBTA:** https://api-v3.mbta.com/register (free, instant)
   - **NOAA:** https://www.ncdc.noaa.gov/cdo-web/token (free, takes 1 day)
   - **Google Maps:** https://console.cloud.google.com/google/maps-apis (free tier available)

### For Astronomer Deployment:

See [ASTRONOMER_DEPLOYMENT.md](ASTRONOMER_DEPLOYMENT.md) for instructions on setting environment variables in your deployment.

## Step 2: Start Local Development

```bash
# Start Airflow locally
astro dev start

# Access Airflow UI
open http://localhost:8080
```

**Default credentials:** `admin` / `admin`

## Step 3: Verify Setup

1. Go to Airflow UI
2. Find the DAG: `boston_reliability_engine_ingestion`
3. Check that it has no import errors
4. Toggle it ON
5. Click "Trigger DAG" to run manually

## Step 4: Monitor the Pipeline

The DAG runs these tasks in order:

1. **Parallel ingestion** (runs simultaneously):
   - `ingest_mbta_data` - Fetch MBTA predictions & schedules
   - `ingest_noaa_weather` - Fetch weather data
   - `ingest_google_maps_driving` - Fetch driving times

2. **run_data_quality_tests** - Validate data quality

3. **reliability_engine_cosmos_dag** - dbt transformations:
   - Silver layer (staging models)
   - Gold layer (analytics models)

## Step 5: Check Results in Snowflake

```sql
-- Check bronze data
SELECT COUNT(*) FROM lpham08.bronze_mbta_predictions;
SELECT COUNT(*) FROM lpham08.bronze_noaa_weather;

-- Check silver layer
SELECT COUNT(*) FROM lpham08.stg_mbta_performance;

-- Check gold layer - reliability scores
SELECT * FROM lpham08.fct_mbta_route_reliability
ORDER BY reliability_score DESC
LIMIT 10;

-- Compare modes
SELECT * FROM lpham08.fct_mode_comparison
WHERE weather_condition = 'clear'
  AND hour_bucket = 'AM_PEAK'
ORDER BY reliability_score DESC;
```

## Project Structure

```
├── dags/capstone/
│   └── boston_reliability_engine_dag.py    # Main DAG
├── include/
│   ├── capstone/
│   │   └── data_quality_tests.py           # Data validation
│   └── eczachly/scripts/snowpark/
│       └── load_boston_reliability_data.py # Ingestion logic
├── dbt_project/
│   └── models/capstone/
│       ├── staging/      # Silver layer (stg_*)
│       └── marts/        # Gold layer (fct_*)
└── docs/
    ├── ARCHITECTURE.md                      # System design
    ├── EXAMPLE_QUERIES.sql                  # Sample queries
    └── TESTING_GUIDE.md                     # Testing instructions
```

## Key Metrics

### Reliability Score (0-100)
```
Reliability Score = 0.6 × On-Time Rate + 0.4 × Stability Score

Where:
  On-Time Rate = % of trips within 5 minutes of schedule
  Stability Score = How consistent trip times are (low variance = high stability)
```

### Categories
- **90-100:** Highly reliable
- **75-90:** Moderately reliable
- **60-75:** Unreliable
- **0-60:** Very unreliable

## Example Use Cases

1. **"Should I take the T or drive to work?"**
   - Query `fct_mode_comparison` for your route and time
   - Compare reliability scores and travel times

2. **"How does rain affect my commute?"**
   - Query `fct_weather_impact` to see deltas from clear weather
   - Check weather resilience scores

3. **"Which MBTA line is most reliable?"**
   - Query `fct_mbta_route_reliability` grouped by route
   - Compare during peak vs off-peak hours

## Troubleshooting

### "Env var required but not provided: 'SNOWFLAKE_USER'"
**Fix:** Check that `.env` file exists and has all required variables

### "No module named 'include.eczachly.scripts.snowpark.load_boston_reliability_data'"
**Fix:** Ensure `include/eczachly/scripts/snowpark/__init__.py` exists

### "No rows in gold tables"
**Fix:** Let the DAG run for a few days to collect enough data (minimum 30 observations needed)

## Next Steps

1. ✅ Run the DAG daily to build historical data
2. 📊 Create visualizations using Snowflake or BI tools
3. 🔔 Set up alerts for data quality failures
4. 🚀 Deploy to Astronomer for production use

## Documentation

- **Architecture:** [ARCHITECTURE.md](ARCHITECTURE.md)
- **Testing:** [TESTING_GUIDE.md](TESTING_GUIDE.md)
- **Deployment:** [ASTRONOMER_DEPLOYMENT.md](ASTRONOMER_DEPLOYMENT.md)
- **Example Queries:** [EXAMPLE_QUERIES.sql](EXAMPLE_QUERIES.sql)
- **Implementation Summary:** [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)
