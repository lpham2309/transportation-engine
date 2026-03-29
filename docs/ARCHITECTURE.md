# Boston Reliability Engine - Architecture & Diagrams

## 1. High-Level System Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        MBTA[MBTA V3 API<br/>Real-time Predictions<br/>& Schedules]
        NOAA[NOAA Weather API<br/>Daily Observations]
        GMAPS[Google Maps API<br/>Driving Times]
    end

    subgraph "Orchestration Layer"
        AIRFLOW[Apache Airflow<br/>Daily at 1 AM ET<br/>Astronomer Runtime]
    end

    subgraph "Ingestion & Processing"
        PYTHON[Python + Snowpark<br/>API Calls<br/>H3 Enrichment<br/>Data Validation]
    end

    subgraph "Data Warehouse - Snowflake Schema: lpham08"
        BRONZE[Bronze Layer<br/>bronze_mbta_predictions<br/>bronze_mbta_schedules<br/>bronze_noaa_weather<br/>bronze_driving_routes]
        SILVER[Silver Layer<br/>stg_mbta_performance<br/>stg_weather_daily<br/>stg_driving_performance]
        GOLD[Gold Layer<br/>fct_mbta_route_reliability<br/>fct_driving_route_reliability<br/>fct_mode_comparison<br/>fct_weather_impact]
    end

    subgraph "Transformation"
        DBT[dbt + Cosmos<br/>Incremental Silver Models<br/>Table Gold Models]
    end

    subgraph "Data Quality"
        DQ[Data Quality Tests<br/>- Not Null<br/>- Unique<br/>- H3 Validity<br/>- Coordinate Ranges<br/>- Freshness<br/>- Business Logic]
    end

    subgraph "Analytics Output"
        TABLES[Final Tables:<br/>- Route Reliability Scores<br/>- Weather Impact Analysis<br/>- MBTA vs Driving Comparison<br/>- Time-of-Day Patterns]
    end

    MBTA --> AIRFLOW
    NOAA --> AIRFLOW
    GMAPS --> AIRFLOW

    AIRFLOW --> PYTHON
    PYTHON --> BRONZE

    BRONZE --> DQ
    DQ --> DBT
    DBT --> SILVER
    SILVER --> DBT
    DBT --> GOLD

    GOLD --> TABLES

    style MBTA fill:#e1f5ff
    style NOAA fill:#e1f5ff
    style GMAPS fill:#e1f5ff
    style AIRFLOW fill:#fff4e6
    style BRONZE fill:#fce4ec
    style SILVER fill:#f3e5f5
    style GOLD fill:#fff9c4
    style TABLES fill:#c8e6c9
```

## 2. Data Pipeline Workflow (Actual Airflow DAG)

```mermaid
graph TB
    START([DAG Start<br/>boston_reliability_engine_ingestion<br/>Daily 1 AM ET])

    subgraph "Parallel Ingestion Tasks with H3 Enrichment"
        TASK1[ingest_mbta_data<br/>- Fetch 10 MBTA routes<br/>- Predictions & Schedules<br/>- Add H3 index<br/>- Append to bronze tables]
        TASK2[ingest_noaa_weather<br/>- Fetch Boston Logan weather<br/>- Daily observations<br/>- Upsert by date]
        TASK3[ingest_google_maps_driving<br/>- 4 sample routes<br/>- Traffic-aware duration<br/>- Add H3 for origin/dest<br/>- Append to bronze table]
    end

    subgraph "Data Quality Validation"
        DQ_TESTS[run_data_quality_tests<br/>- Not Null<br/>- Unique IDs<br/>- H3 Validity<br/>- Coordinate Ranges<br/>- Freshness Checks]
    end

    subgraph "dbt Transformations via Cosmos"
        DBT_GROUP[reliability_engine_cosmos_dag<br/>DbtTaskGroup]

        subgraph "Silver Layer Incremental"
            DBT_SILVER1[stg_mbta_performance<br/>Join predictions + schedules<br/>Calculate delays]
            DBT_SILVER2[stg_weather_daily<br/>Weather categories]
            DBT_SILVER3[stg_driving_performance<br/>Traffic analysis]
        end

        subgraph "Gold Layer Tables"
            DBT_GOLD1[fct_mbta_route_reliability<br/>Reliability scores 0-100]
            DBT_GOLD2[fct_driving_route_reliability<br/>Traffic impact metrics]
            DBT_GOLD3[fct_mode_comparison<br/>MBTA vs Driving rankings]
            DBT_GOLD4[fct_weather_impact<br/>Weather delta analysis]
        end
    end

    SUCCESS([DAG Success])

    START --> TASK1
    START --> TASK2
    START --> TASK3

    TASK1 --> DQ_TESTS
    TASK2 --> DQ_TESTS
    TASK3 --> DQ_TESTS

    DQ_TESTS --> DBT_GROUP
    DBT_GROUP --> DBT_SILVER1
    DBT_GROUP --> DBT_SILVER2
    DBT_GROUP --> DBT_SILVER3

    DBT_SILVER1 --> DBT_GOLD1
    DBT_SILVER2 --> DBT_GOLD1
    DBT_SILVER3 --> DBT_GOLD2
    DBT_SILVER1 --> DBT_GOLD3
    DBT_SILVER3 --> DBT_GOLD3
    DBT_SILVER1 --> DBT_GOLD4
    DBT_SILVER2 --> DBT_GOLD4

    DBT_GOLD1 --> SUCCESS
    DBT_GOLD2 --> SUCCESS
    DBT_GOLD3 --> SUCCESS
    DBT_GOLD4 --> SUCCESS

    style START fill:#90ee90
    style TASK1 fill:#87ceeb
    style TASK2 fill:#87ceeb
    style TASK3 fill:#87ceeb
    style DQ_TESTS fill:#dda0dd
    style DBT_GROUP fill:#fff4e6
    style DBT_SILVER1 fill:#f3e5f5
    style DBT_SILVER2 fill:#f3e5f5
    style DBT_SILVER3 fill:#f3e5f5
    style DBT_GOLD1 fill:#ffd700
    style DBT_GOLD2 fill:#ffd700
    style DBT_GOLD3 fill:#ffd700
    style DBT_GOLD4 fill:#ffd700
    style SUCCESS fill:#90ee90
```

## 3. Data Model - Bronze → Silver → Gold (Actual Implementation)

**Schema:** `lpham08` (all layers)

```mermaid
graph LR
    subgraph "Bronze Layer - Raw Data lpham08.bronze_*"
        B1[bronze_mbta_predictions<br/>- prediction_id<br/>- predicted_arrival<br/>- stop_h3_index<br/>- route_id<br/>- fetched_at]
        B2[bronze_mbta_schedules<br/>- schedule_id<br/>- scheduled_arrival<br/>- stop_h3_index<br/>- route_id<br/>- fetched_at]
        B3[bronze_noaa_weather<br/>- observation_date<br/>- precipitation_inches<br/>- temp_max_f/temp_min_f<br/>- weather_condition<br/>- wind_speed_mph]
        B4[bronze_driving_routes<br/>- route_name<br/>- duration_in_traffic_seconds<br/>- origin_h3_index<br/>- destination_h3_index<br/>- fetched_at]
    end

    subgraph "Silver Layer - Staging lpham08.stg_*"
        S1[stg_mbta_performance<br/>✅ Incremental<br/>- Join predictions + schedules<br/>- delay_seconds calculated<br/>- is_on_time boolean<br/>- hour_bucket categorization<br/>- weather_condition joined]
        S2[stg_weather_daily<br/>✅ Table<br/>- weather_category_detailed<br/>- weather_severity_score<br/>- Aggregated by date]
        S3[stg_driving_performance<br/>✅ Incremental<br/>- traffic_delay_seconds<br/>- traffic_impact_pct<br/>- hour_bucket<br/>- weather joined]
    end

    subgraph "Gold Layer - Facts lpham08.fct_*"
        G1[fct_mbta_route_reliability<br/>✅ Table<br/>- reliability_score 0-100<br/>- on_time_rate<br/>- transit_volatility_cv<br/>- p50/p90/p95_delay_sec<br/>- reliability_category<br/>- confidence_level]
        G2[fct_driving_route_reliability<br/>✅ Table<br/>- reliability_score<br/>- avg_traffic_impact_pct<br/>- median_duration_min<br/>- p90/p95 percentiles]
        G3[fct_mode_comparison<br/>✅ Table<br/>- MBTA vs Driving rankings<br/>- most_reliable_mode<br/>- fastest_mode<br/>- is_recommended<br/>- time_delta_from_fastest]
        G4[fct_weather_impact<br/>✅ Table<br/>- reliability_delta_from_clear<br/>- time_delta_from_clear_min<br/>- weather_impact_category<br/>- weather_resilience_score]
    end

    B1 --> S1
    B2 --> S1
    B3 --> S1
    B3 --> S2
    B3 --> S3
    B4 --> S3

    S1 --> G1
    S2 --> G1
    S3 --> G2
    S2 --> G2

    S1 --> G3
    S3 --> G3

    S1 --> G4
    S3 --> G4
    S2 --> G4

    style B1 fill:#ffebee
    style B2 fill:#ffebee
    style B3 fill:#ffebee
    style B4 fill:#ffebee
    style S1 fill:#f3e5f5
    style S2 fill:#f3e5f5
    style S3 fill:#f3e5f5
    style G1 fill:#fff9c4
    style G2 fill:#fff9c4
    style G3 fill:#fff9c4
    style G4 fill:#fff9c4
```

## 4. Reliability Score Calculation Flow

```mermaid
graph TB
    START([Route-Mode-Condition<br/>Stratification])

    subgraph "Input Data"
        TRIPS[Historical Trip Times<br/>T₁, T₂, ..., Tₙ]
        TARGET[Target Threshold<br/>MBTA: Schedule + 5min<br/>Bluebikes: P50 + 5min<br/>Driving: Prediction + 5min]
    end

    subgraph "Statistical Calculations"
        PERCENTILES[Calculate Percentiles<br/>P50 median<br/>P90 worst-case<br/>P95 extreme]
        STATS[Calculate Statistics<br/>Mean μ<br/>Std Dev σ]
    end

    subgraph "Component Scores"
        ONTIME[On-Time Score<br/>Count T ≤ Target / n × 100]
        STABILITY[Stability Score<br/>100 × max 0, 1 - P95-P50/P50]
    end

    subgraph "Final Calculation"
        RELIABILITY[Reliability Score<br/>0.6 × OnTimeScore<br/>+ 0.4 × StabilityScore<br/>Clipped to 0-100]
    end

    subgraph "Volatility Metric"
        VOLATILITY[Transit Volatility<br/>CV = σ / μ × 100]
    end

    subgraph "Interpretation"
        THRESHOLD{Score Range}
        HIGH[90-100: Highly Reliable<br/>Safe for tight schedules]
        MEDIUM[75-90: Moderately Reliable<br/>Add 5-10 min buffer]
        LOW[60-75: Unreliable<br/>Add 15+ min buffer]
        VERYLOW[0-60: Very Unreliable<br/>Avoid if time-sensitive]
    end

    START --> TRIPS
    START --> TARGET
    TRIPS --> PERCENTILES
    TRIPS --> STATS
    PERCENTILES --> ONTIME
    PERCENTILES --> STABILITY
    TARGET --> ONTIME
    STATS --> VOLATILITY

    ONTIME --> RELIABILITY
    STABILITY --> RELIABILITY

    RELIABILITY --> THRESHOLD
    THRESHOLD -->|90-100| HIGH
    THRESHOLD -->|75-90| MEDIUM
    THRESHOLD -->|60-75| LOW
    THRESHOLD -->|0-60| VERYLOW

    style START fill:#e1f5ff
    style RELIABILITY fill:#ffd700
    style VOLATILITY fill:#ffb6c1
    style HIGH fill:#90ee90
    style MEDIUM fill:#fff4a3
    style LOW fill:#ffb347
    style VERYLOW fill:#ff6347
```

## 5. H3 Spatial Indexing Workflow

```mermaid
graph TB
    subgraph "Input Sources"
        MBTA_COORDS[MBTA Stations<br/>Lat: 42.3601<br/>Lon: -71.0589]
        BLUEBIKES_COORDS[Bluebikes Docks<br/>Lat: 42.3554<br/>Lon: -71.0603]
        DRIVING_COORDS[Driving Routes<br/>Origin/Dest Coords]
    end

    subgraph "H3 Conversion"
        H3_CONV[H3 Library<br/>h3.latlng_to_cell lat, lon, 8]
    end

    subgraph "H3 Hexagons (Resolution 8)"
        HEX1[8844c2a9bffffff<br/>Kendall Square Area]
        HEX2[8844c2891ffffff<br/>Financial District]
        HEX3[8844c2b2fffffff<br/>Back Bay Area]
    end

    subgraph "Route Normalization"
        ROUTES[Route Pairs<br/>Origin H3 → Dest H3<br/>8844c2a9bffffff → 8844c2891ffffff]
    end

    subgraph "Benefits"
        BENEFIT1[Consistent Spatial Units<br/>Stations/Docks/Routes<br/>mapped to same grid]
        BENEFIT2[Aggregation<br/>Multiple stops in<br/>same hex = same zone]
        BENEFIT3[Comparison<br/>MBTA vs Bluebikes vs Driving<br/>on same route pairs]
    end

    MBTA_COORDS --> H3_CONV
    BLUEBIKES_COORDS --> H3_CONV
    DRIVING_COORDS --> H3_CONV

    H3_CONV --> HEX1
    H3_CONV --> HEX2
    H3_CONV --> HEX3

    HEX1 --> ROUTES
    HEX2 --> ROUTES
    HEX3 --> ROUTES

    ROUTES --> BENEFIT1
    ROUTES --> BENEFIT2
    ROUTES --> BENEFIT3

    style H3_CONV fill:#dda0dd
    style HEX1 fill:#87ceeb
    style HEX2 fill:#87ceeb
    style HEX3 fill:#87ceeb
    style ROUTES fill:#ffd700
    style BENEFIT1 fill:#90ee90
    style BENEFIT2 fill:#90ee90
    style BENEFIT3 fill:#90ee90
```

## 6. Weather Impact Analysis Flow

```mermaid
graph LR
    subgraph "Weather Stratification"
        W1[Clear Weather<br/>No precipitation<br/>Temp 20-95°F]
        W2[Rain<br/>Precip > 0 inches<br/>Temp > 32°F]
        W3[Snow<br/>Snow depth > 0<br/>Temp ≤ 32°F]
    end

    subgraph "Mode-Weather Combinations Calculated"
        MBTA_CLEAR[MBTA + Clear<br/>Query: AVG reliability_score<br/>WHERE weather='clear']
        MBTA_RAIN[MBTA + Rain<br/>Query: AVG reliability_score<br/>WHERE weather='rain']
        MBTA_SNOW[MBTA + Snow<br/>Query: AVG reliability_score<br/>WHERE weather='snow']

        DRIVE_CLEAR[Driving + Clear<br/>Query: AVG reliability_score<br/>WHERE weather='clear']
        DRIVE_RAIN[Driving + Rain<br/>Query: AVG reliability_score<br/>WHERE weather='rain']
        DRIVE_SNOW[Driving + Snow<br/>Query: AVG reliability_score<br/>WHERE weather='snow']
    end

    subgraph "Weather Impact Delta from fct_weather_impact"
        MBTA_DELTA[MBTA Impact<br/>reliability_delta_from_clear<br/>time_delta_from_clear_min]
        DRIVE_DELTA[Driving Impact<br/>reliability_delta_from_clear<br/>time_delta_from_clear_min]
    end

    subgraph "Weather Resilience Score"
        RESILIENCE[weather_resilience_score<br/>= 100 - ABS delta_from_clear<br/>Higher = more weather-resistant]
    end

    W1 --> MBTA_CLEAR
    W2 --> MBTA_RAIN
    W3 --> MBTA_SNOW
    W1 --> DRIVE_CLEAR
    W2 --> DRIVE_RAIN
    W3 --> DRIVE_SNOW

    MBTA_CLEAR --> MBTA_DELTA
    MBTA_RAIN --> MBTA_DELTA
    MBTA_SNOW --> MBTA_DELTA
    DRIVE_CLEAR --> DRIVE_DELTA
    DRIVE_RAIN --> DRIVE_DELTA
    DRIVE_SNOW --> DRIVE_DELTA

    MBTA_DELTA --> RESILIENCE
    DRIVE_DELTA --> RESILIENCE

    style W1 fill:#fff9c4
    style W2 fill:#b3e5fc
    style W3 fill:#e1f5fe
    style MBTA_DELTA fill:#f3e5f5
    style DRIVE_DELTA fill:#f3e5f5
    style RESILIENCE fill:#ffd700
```

## 7. Data Quality Testing Framework

```mermaid
graph TB
    subgraph "Source Validation (During Ingestion)"
        SV1[API Response Validation<br/>- Status codes<br/>- JSON structure<br/>- Required fields]
        SV2[Coordinate Validation<br/>- Valid lat/lon ranges<br/>- Boston area bounds<br/>42.2° to 42.5°N<br/>-71.2° to -70.9°W]
        SV3[H3 Validation<br/>- 15 character length<br/>- Hexadecimal format<br/>- Resolution 8]
    end

    subgraph "Bronze Layer Tests"
        BT1[Not Null Tests<br/>- stop_id<br/>- route_id<br/>- observation_date<br/>- weather_condition]
        BT2[Unique Tests<br/>- prediction_id<br/>- schedule_id<br/>- trip_id unique]
        BT3[H3 Index Validity<br/>- Format validation<br/>- Resolution check]
    end

    subgraph "Silver Layer Tests"
        ST1[Relationship Tests<br/>- All predictions<br/>have schedules<br/>- Weather data exists<br/>for trip dates]
        ST2[Range Tests<br/>- Travel times > 0<br/>- Delays reasonable<br/>< 2 hours]
        ST3[Join Completeness<br/>- Weather joined<br/>for all trips<br/>- H3 pairs valid]
    end

    subgraph "Gold Layer Tests"
        GT1[Business Logic<br/>- Reliability 0-100<br/>- Volatility ≥ 0<br/>- Sample size n ≥ 30]
        GT2[Consistency<br/>- P90 > P50<br/>- P95 > P90<br/>- Mean near median]
        GT3[Comparative<br/>- Weather impact<br/>makes sense<br/>- Mode differences<br/>expected]
    end

    subgraph "Actions"
        PASS[Tests Pass<br/>Continue Pipeline]
        FAIL[Tests Fail<br/>Alert & Stop]
    end

    SV1 --> BT1
    SV2 --> BT1
    SV3 --> BT1

    BT1 --> ST1
    BT2 --> ST1
    BT3 --> ST1

    ST1 --> GT1
    ST2 --> GT1
    ST3 --> GT1

    GT1 --> PASS
    GT2 --> PASS
    GT3 --> PASS

    BT1 -.->|Fail| FAIL
    ST1 -.->|Fail| FAIL
    GT1 -.->|Fail| FAIL

    style PASS fill:#90ee90
    style FAIL fill:#ff6347
    style SV1 fill:#fff9c4
    style BT1 fill:#ffebee
    style ST1 fill:#f3e5f5
    style GT1 fill:#ffd700
```

## 8. Implementation Status

### ✅ Phase 1: Bronze Layer - COMPLETE
- ✅ Astronomer Airflow environment set up
- ✅ Python + Snowpark ingestion scripts implemented
- ✅ Bronze tables created in Snowflake schema `lpham08`
  - `bronze_mbta_predictions`
  - `bronze_mbta_schedules`
  - `bronze_noaa_weather`
  - `bronze_driving_routes`
- ✅ H3 spatial indexing (resolution 8) implemented
- ✅ Historical data preservation (append mode)
- ✅ Data quality test framework implemented

### ✅ Phase 2: Silver Layer - COMPLETE
- ✅ dbt staging models created with Cosmos integration
- ✅ `stg_mbta_performance` - predictions + schedules joined, delays calculated
- ✅ `stg_weather_daily` - weather categories and severity scoring
- ✅ `stg_driving_performance` - traffic analysis and impact metrics
- ✅ Incremental materialization for efficiency
- ✅ Weather correlation logic
- ✅ Time bucketing (AM_PEAK, PM_PEAK, MIDDAY, OFF_PEAK)

### ✅ Phase 3: Gold Layer - COMPLETE
- ✅ Reliability score formula implemented (0.6 × on-time + 0.4 × stability)
- ✅ `fct_mbta_route_reliability` - reliability scores, volatility, percentiles
- ✅ `fct_driving_route_reliability` - traffic impact, reliability scores
- ✅ `fct_mode_comparison` - MBTA vs Driving with recommendations
- ✅ `fct_weather_impact` - weather delta analysis and resilience scores
- ✅ Transit volatility (coefficient of variation) metrics
- ✅ Confidence levels based on sample size

### ✅ Phase 4: Testing & Validation - IN PROGRESS
- ✅ Comprehensive dbt data quality tests in YAML
- ✅ Data quality tests in Python (not_null, unique, H3 validity, coordinate ranges, freshness)
- 🔄 Running daily to backfill historical data
- 🔄 Validating results with example queries
- ⏭️ Performance optimization (future)

### ✅ Phase 5: Documentation - COMPLETE
- ✅ Architecture diagrams (this file)
- ✅ Implementation summary (`IMPLEMENTATION_SUMMARY.md`)
- ✅ Testing guide (`TESTING_GUIDE.md`)
- ✅ Example queries (`EXAMPLE_QUERIES.sql`)
- ✅ dbt model README
- ⏭️ Production deployment (future)

## 9. Current Data Sources (Implemented)

| Data Source | API/Source | Update Frequency | Records per Day |
|-------------|-----------|------------------|-----------------|
| MBTA Predictions | MBTA V3 API | Daily | ~50K-100K |
| MBTA Schedules | MBTA V3 API | Daily | ~50K-100K |
| NOAA Weather | NOAA CDO API | Daily | 1 |
| Google Maps Driving | Google Maps API | Daily | ~100-200 |

**10 MBTA Routes:** Red, Orange, Blue, Green-B, Green-C, Green-D, Green-E, Mattapan, Silver Line SL1, Silver Line SL2

**4 Driving Routes:** Kendall→Financial District, Back Bay→Seaport, Harvard→Downtown, Allston→Kendall

## 10. Next Steps

1. **Production Deployment** - Deploy to Astronomer Cloud or production Airflow
2. **Increase Frequency** - Optionally change from daily to hourly/5-min ingestion
3. **Add More Routes** - Expand origin-destination pairs for driving analysis
4. **Dashboard Layer** - Connect Tableau/Looker to gold tables
5. **Alerting** - Set up alerts for data quality failures
6. **Bluebikes Integration** - Add bike-sharing data (future enhancement)