# Boston Reliability Engine - dbt Models

## Overview

This directory contains the dbt transformations for the Boston Reliability Engine capstone project. The models transform raw bronze layer data into analytics-ready silver and gold layers.

## Model Structure

```
capstone/
├── staging/           # Silver layer - cleaned and joined data
│   ├── stg_mbta_performance.sql
│   ├── stg_weather_daily.sql
│   ├── stg_driving_performance.sql
│   ├── stg_models.yml
│   └── _sources.yml
│
└── marts/             # Gold layer - analytics and metrics
    ├── fct_mbta_route_reliability.sql
    ├── fct_driving_route_reliability.sql
    ├── fct_mode_comparison.sql
    ├── fct_weather_impact.sql
    └── fct_models.yml
```

## Data Lineage

```
Bronze (Raw)               Silver (Staging)              Gold (Marts)
─────────────             ───────────────────           ──────────────

bronze_mbta_predictions ──┐
                          ├─> stg_mbta_performance ──┐
bronze_mbta_schedules ────┘                          ├─> fct_mbta_route_reliability ──┐
                                                      │                                 │
bronze_noaa_weather ──────> stg_weather_daily ───────┤                                 ├─> fct_mode_comparison
                                                      │                                 │
bronze_driving_routes ─────> stg_driving_performance─┴─> fct_driving_route_reliability┘
                                                                      │
                                                                      ├─> fct_weather_impact
                                                                      │
                                                                      └─> (other analyses)
```

## Running the Models

### Run All Capstone Models
```bash
cd dbt_project
dbt run --select tag:capstone
```

### Run by Layer
```bash
# Silver layer only
dbt run --select tag:silver

# Gold layer only
dbt run --select tag:gold
```

### Run Specific Models
```bash
# Just MBTA reliability
dbt run --select fct_mbta_route_reliability

# Mode comparison
dbt run --select fct_mode_comparison

# With dependencies
dbt run --select +fct_mode_comparison  # Includes upstream models
```

### Run Tests
```bash
# All capstone tests
dbt test --select tag:capstone

# Specific model tests
dbt test --select fct_mbta_route_reliability
```

## Key Models

### Silver Layer (Staging)

#### `stg_mbta_performance`
- **Purpose:** Join MBTA predictions with schedules to calculate delays
- **Materialization:** Incremental (efficient for growing data)
- **Key Columns:**
  - `delay_seconds`: Actual delay vs scheduled time
  - `is_on_time`: Boolean (delay ≤ 5 minutes)
  - `hour_bucket`: Time period (AM_PEAK, PM_PEAK, MIDDAY, OFF_PEAK)
  - `weather_condition`: Joined from weather data

#### `stg_weather_daily`
- **Purpose:** Enhanced weather data with severity scoring
- **Materialization:** Table
- **Key Columns:**
  - `weather_category_detailed`: More granular categories
  - `weather_severity_score`: 0-100 composite score

#### `stg_driving_performance`
- **Purpose:** Google Maps driving data with traffic analysis
- **Materialization:** Incremental
- **Key Columns:**
  - `traffic_delay_seconds`: Extra time due to traffic
  - `traffic_impact_pct`: Percentage impact of traffic

### Gold Layer (Marts)

#### `fct_mbta_route_reliability`
- **Purpose:** Calculate reliability scores for MBTA routes
- **Materialization:** Table
- **Key Metrics:**
  - `reliability_score`: 0-100 composite score (60% on-time + 40% stability)
  - `transit_volatility_cv`: Coefficient of variation
  - `p50/p90/p95_delay_sec`: Percentile travel times
  - `reliability_category`: Classification (highly_reliable, moderately_reliable, etc.)

#### `fct_driving_route_reliability`
- **Purpose:** Calculate reliability scores for driving routes
- **Materialization:** Table
- **Key Metrics:** Same as MBTA plus traffic impact metrics

#### `fct_mode_comparison`
- **Purpose:** Side-by-side comparison of transportation modes
- **Materialization:** Table
- **Key Columns:**
  - `most_reliable_mode`: Winner by reliability
  - `fastest_mode`: Winner by median time
  - `is_recommended`: Recommended mode for scenario
  - `time_delta_from_fastest_min`: Time difference vs fastest option

#### `fct_weather_impact`
- **Purpose:** Analyze how weather affects each mode
- **Materialization:** Table
- **Key Columns:**
  - `reliability_delta_from_clear`: Change vs clear weather
  - `time_delta_from_clear_min`: Additional time in bad weather
  - `weather_impact_category`: Impact severity
  - `weather_resilience_score`: How well mode handles weather

## Reliability Score Formula

```
Reliability Score = (0.6 × On-Time Score) + (0.4 × Stability Score)

Where:
  On-Time Score = % of trips within 5 minutes of target
  Stability Score = 100 × (1 - (P95 - P50) / P50)
```

**Interpretation:**
- **90-100:** Highly reliable - safe for tight schedules
- **75-90:** Moderately reliable - add 5-10 min buffer
- **60-75:** Unreliable - add 15+ min buffer
- **0-60:** Very unreliable - avoid if time-sensitive

## Testing

### Data Quality Tests

The models include comprehensive tests:

1. **Uniqueness tests** - Ensure no duplicate records
2. **Not null tests** - Critical fields must have values
3. **Range tests** - Scores must be 0-100
4. **Business logic tests** - P95 >= P90 >= P50
5. **Sample size tests** - Minimum 30 observations (warn if less)

### Running Tests

```bash
# Run all tests
dbt test --select tag:capstone

# View test results
dbt test --store-failures --select tag:capstone
```

## Debugging

### Check Model Compilation
```bash
dbt compile --select fct_mbta_route_reliability
cat target/compiled/reliability_engine/models/capstone/marts/fct_mbta_route_reliability.sql
```

### Run a Single Model with Logging
```bash
dbt run --select stg_mbta_performance --full-refresh --debug
```

### Check Model Results
```sql
-- In Snowflake
SELECT * FROM lpham08.fct_mbta_route_reliability LIMIT 10;
```

## Sample Queries

See `docs/EXAMPLE_QUERIES.sql` for common analysis patterns:
- Route recommendations
- Weather impact analysis
- Mode comparisons
- Peak vs off-peak analysis
- MBTA line rankings

## Configuration

Models are configured in `dbt_project.yml`:

```yaml
models:
  reliability_engine:
    capstone:
      +schema: capstone
      staging:
        +materialized: incremental
        +tags: ['capstone', 'silver']
      marts:
        +materialized: table
        +tags: ['capstone', 'gold']
```

## Dependencies

Required packages (in `packages.yml`):
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

Install with:
```bash
dbt deps
```

## Incremental Model Strategy

Silver layer models use incremental materialization:
- **First run:** Processes all historical data
- **Subsequent runs:** Only processes new data since last run
- **Full refresh:** `dbt run --select stg_mbta_performance --full-refresh`

## Performance Tips

1. **Incremental loads** - Silver models update incrementally
2. **Partitioning** - Consider partitioning by date for large datasets
3. **Indexing** - H3 indices enable fast spatial lookups
4. **Materialization** - Gold tables are pre-computed for fast queries

## Troubleshooting

### Issue: "Relation does not exist"
**Solution:** Run bronze layer ingestion first
```bash
# Trigger the Airflow DAG to populate bronze tables
airflow dags trigger boston_reliability_engine_ingestion
```

### Issue: "No rows in incremental model"
**Solution:** Check if bronze tables have recent data
```sql
SELECT MAX(fetched_at) FROM lpham08.bronze_mbta_predictions;
```

### Issue: "Tests failing"
**Solution:** Check test results and model logic
```bash
dbt test --select fct_mbta_route_reliability --store-failures
```

## Next Steps

1. ✅ Bronze layer ingestion (complete)
2. ✅ Silver layer models (complete)
3. ✅ Gold layer models (complete)
4. ⏭️ Run full pipeline end-to-end
5. ⏭️ Validate results with example queries
6. ⏭️ Set up monitoring and alerting
7. ⏭️ Create dashboard/visualization layer