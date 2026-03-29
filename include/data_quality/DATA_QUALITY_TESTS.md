# Data Quality Tests - Boston Reliability Engine

## Overview

This document describes the comprehensive data quality test suite for the Boston Reliability Engine data pipeline. These tests validate the integrity, completeness, and correctness of data ingested from MBTA, NOAA, Bluebikes, and Google Maps APIs.

## Test Suite

### Test 1: Not Null Test - Critical Columns

**Purpose**: Ensure critical columns do not contain NULL values

**Tables Tested**:
- `bronze_mbta_predictions`: `stop_id`, `route_id`, `stop_h3_index`
- `bronze_mbta_schedules`: `stop_id`, `route_id`, `schedule_date`
- `bronze_noaa_weather`: `observation_date`, `weather_condition`
- `bronze_driving_routes`: `origin_h3_index`, `destination_h3_index`

**Why It Matters**: NULL values in these columns would break downstream joins, spatial indexing, and analytics queries.

**Example Failure Scenario**:
```
✗ MBTA Predictions: 150 null stop_id, 0 null route_id
```

**Resolution**: Check API response handling. Ensure we're properly extracting stop information from the MBTA API's included resources.

---

### Test 2: Unique Test - Primary Keys

**Purpose**: Validate that primary key columns contain unique values (no duplicates)

**Tables Tested**:
- `bronze_mbta_predictions`: `prediction_id` must be unique
- `bronze_mbta_schedules`: `schedule_id` must be unique
- `bronze_noaa_weather`: `observation_date` must be unique

**Why It Matters**: Duplicate primary keys indicate data quality issues and will cause MERGE operations to fail or produce incorrect results in downstream transformations.

**Example Failure Scenario**:
```
✗ MBTA Predictions: 45 duplicate prediction_ids found
```

**Resolution**: This likely means the DAG ran twice for the same execution date, or the API returned duplicate data. Check:
1. DAG execution history
2. MERGE logic in ingestion scripts (should use MERGE instead of INSERT)
3. API pagination handling

---

### Test 3: H3 Index Validity Test

**Purpose**: Validate that H3 spatial indices are in correct format

**H3 Format Requirements**:
- Exactly 15 characters long
- Hexadecimal characters (0-9, a-f)
- Starts with '8' (resolution 8)

**Tables Tested**:
- `bronze_mbta_predictions`: `stop_h3_index`
- `bronze_mbta_schedules`: `stop_h3_index`
- `bronze_bluebikes_trips`: `start_station_h3_index`, `end_station_h3_index`
- `bronze_driving_routes`: `origin_h3_index`, `destination_h3_index`

**Why It Matters**: Invalid H3 indices will cause spatial joins to fail and prevent geographic analysis.

**Example Failure Scenario**:
```
✗ MBTA Predictions H3: 0 wrong length, 12 wrong format, 5 wrong resolution
```

**Resolution**: Check H3 library usage:
```python
# Correct usage
h3_index = h3.geo_to_h3(latitude, longitude, resolution=8)

# Common mistakes
h3_index = h3.geo_to_h3(longitude, latitude, resolution=8)  # Wrong order!
h3_index = h3.geo_to_h3(lat, lon, resolution=9)  # Wrong resolution
```

---

### Test 4: Coordinate Range Test - Geographic Bounds

**Purpose**: Validate that latitude/longitude coordinates are within reasonable Boston area bounds

**Geographic Bounds**:
- Latitude: 42.2° to 42.5°N
- Longitude: -71.2° to -70.9°W

**Weather Data Ranges**:
- Temperature: -20°F to 120°F (extreme but possible)
- Precipitation: 0 to 20 inches per day (extreme but possible)

**Tables Tested**:
- `bronze_mbta_predictions`: `stop_latitude`, `stop_longitude`
- `bronze_mbta_schedules`: `stop_latitude`, `stop_longitude`
- `bronze_bluebikes_trips`: `start/end_station_latitude/longitude`
- `bronze_noaa_weather`: `temp_max_f`, `temp_min_f`, `precipitation_inches`

**Why It Matters**: Out-of-bounds coordinates indicate data corruption, API errors, or lat/lon swap bugs.

**Example Failure Scenario**:
```
✗ MBTA Coordinates: 8 invalid latitudes, 0 invalid longitudes
```

**Resolution**: Common issues:
1. **Lat/Lon swap**: Check if longitude is in latitude column
2. **Decimal place errors**: 42.36 vs 4236 (missing decimal point)
3. **Wrong city data**: API returned data for wrong region

---

### Test 5: Freshness Test - Data for Execution Date

**Purpose**: Validate that data was ingested for the correct execution date

**Checks**:
1. MBTA schedules exist for `execution_date`
2. NOAA weather observation exists for `execution_date`
3. Data was ingested within last 24 hours (check `_ingested_at` timestamp)

**Why It Matters**: Ensures the DAG is actually fetching data for the correct date and not failing silently.

**Example Failure Scenario**:
```
✗ MBTA Schedules: No data found for 2026-02-14
✗ NOAA Weather: No data found for 2026-02-14
```

**Resolution**: Common issues:
1. **API date parameter wrong**: Check date formatting in API calls
2. **API returned no data**: Check API response codes and error messages
3. **Timezone issues**: Ensure using correct timezone for Boston (ET)
4. **Future date**: Can't fetch schedules for dates in the future

---

## Running the Tests

### From Airflow DAG

Tests run automatically as part of the `validate_ingestion` task:

```
start >> [mbta, weather, bluebikes, driving] >> validation_task >> end
```

### Manually from Command Line

```bash
# Run all tests for today
python3 include/capstone/data_quality_tests.py

# Run tests for specific date
python3 include/capstone/data_quality_tests.py 2026-02-13

# Set schema via environment variable
export STUDENT_SCHEMA=lpham08
python3 include/capstone/data_quality_tests.py 2026-02-13
```

### From Python Script

```python
from include.capstone.data_quality_tests import run_all_dq_tests

results = run_all_dq_tests(
    schema='lpham08',
    execution_date='2026-02-13'
)

# Check if all tests passed
if all(results.values()):
    print("All tests passed!")
else:
    failed = [k for k, v in results.items() if not v]
    print(f"Failed tests: {failed}")
```

## Test Output

### Successful Run

```
======================================================================
RUNNING DATA QUALITY TESTS FOR 2026-02-13
Schema: lpham08
======================================================================

Test 1: Not Null Test - Critical columns must have values
  ✓ MBTA Predictions: All critical columns populated (tested 4523 rows)
  ✓ MBTA Schedules: All critical columns populated (tested 4891 rows)
  ✓ NOAA Weather: All critical columns populated (tested 1 rows)
  ✓ Driving Routes: All critical columns populated (tested 12 rows)

Test 2: Unique Test - No duplicate primary keys
  ✓ MBTA Predictions: All prediction_ids are unique
  ✓ MBTA Schedules: All schedule_ids are unique
  ✓ NOAA Weather: All observation_dates are unique

Test 3: H3 Index Validity Test - Valid H3 format and resolution
  ✓ MBTA Predictions: All 4523 H3 indices valid
  ✓ MBTA Schedules: All 4891 H3 indices valid
  ✓ Bluebikes: All H3 indices valid (tested 52340 rows)

Test 4: Coordinate Range Test - Geographic coordinates within bounds
  ✓ MBTA Predictions: All 4523 coordinates within Boston bounds
  ✓ NOAA Weather: All 1 weather values within reasonable ranges
  ✓ Bluebikes: All coordinates within Boston bounds (tested 52340 rows)

Test 5: Freshness Test - Data exists for execution date
  ✓ MBTA Schedules: 4891 schedules found for 2026-02-13
  ✓ NOAA Weather: 1 observation(s) found for 2026-02-13
  ✓ MBTA Predictions: 4523 rows ingested in last 24 hours

======================================================================
DATA QUALITY TEST SUMMARY
======================================================================
✓ not_null_test: PASSED
✓ unique_test: PASSED
✓ h3_validity_test: PASSED
✓ coordinate_range_test: PASSED
✓ freshness_test: PASSED

Overall: 5/5 tests passed (100.0%)
======================================================================
```

### Failed Run

```
======================================================================
RUNNING DATA QUALITY TESTS FOR 2026-02-13
Schema: lpham08
======================================================================

Test 1: Not Null Test - Critical columns must have values
  ✗ MBTA Predictions: 150 null stop_id, 0 null route_id
  ✓ MBTA Schedules: All critical columns populated (tested 4891 rows)
  ✓ NOAA Weather: All critical columns populated (tested 1 rows)
  ✓ Driving Routes: All critical columns populated (tested 12 rows)

Test 2: Unique Test - No duplicate primary keys
  ✗ MBTA Predictions: 23 duplicate prediction_ids found
  ✓ MBTA Schedules: All schedule_ids are unique
  ✓ NOAA Weather: All observation_dates are unique

...

======================================================================
DATA QUALITY TEST SUMMARY
======================================================================
✗ not_null_test: FAILED
✗ unique_test: FAILED
✓ h3_validity_test: PASSED
✓ coordinate_range_test: PASSED
✓ freshness_test: PASSED

Overall: 3/5 tests passed (60.0%)
======================================================================

⚠️  WARNING: 2 data quality test(s) failed:
   - not_null_test
   - unique_test
```

## Adding Custom Tests

To add a new data quality test:

1. **Create test function** in `data_quality_tests.py`:

```python
def test_custom_validation(schema: str) -> bool:
    """
    Your test description here.
    """
    tests_passed = True

    # Your test SQL
    test_sql = f"""
    SELECT COUNT(*) as violations
    FROM {schema}.your_table
    WHERE your_condition
    """

    result = execute_snowflake_query(test_sql)
    violations = result[0][0]

    if violations > 0:
        print(f"  ✗ Your Test: {violations} violations found")
        tests_passed = False
    else:
        print(f"  ✓ Your Test: No violations")

    return tests_passed
```

2. **Add to test suite** in `run_all_dq_tests()`:

```python
print("\nTest 6: Your Custom Test - Description")
results['custom_test'] = test_custom_validation(schema)
```

## Integration with Monitoring

### Send Alerts on Failure

Update the DAG validation task to send alerts:

```python
@task(task_id="validate_ingestion")
def validate(**context):
    from include.capstone.data_quality_tests import run_all_dq_tests

    dq_results = run_all_dq_tests(schema, execution_date)

    failed_tests = [k for k, v in dq_results.items() if not v]

    if failed_tests:
        # Send Slack/email alert
        send_alert(f"DQ Tests Failed: {failed_tests}")

        # Optionally raise exception to fail the DAG
        raise ValueError(f"Data quality tests failed: {failed_tests}")

    return dq_results
```

### Track Metrics Over Time

Store test results in a table for trend analysis:

```python
# After running tests
store_dq_results_sql = f"""
INSERT INTO {schema}.dq_test_history (
    execution_date, test_name, passed, tested_at
)
VALUES
{', '.join([f"('{execution_date}', '{k}', {v}, CURRENT_TIMESTAMP())"
            for k, v in dq_results.items()])}
"""
```

## Best Practices

1. **Run tests after every ingestion**: Catch data quality issues immediately
2. **Don't silently pass failures**: Log all violations clearly
3. **Set thresholds**: Some tests might allow small percentages of failures
4. **Monitor trends**: Track test pass rates over time
5. **Document expected ranges**: Update bounds as data patterns change
6. **Test in development**: Run tests on sample data before production

## Troubleshooting Common Issues

| Issue | Likely Cause | Solution |
|-------|-------------|----------|
| All tests fail | Schema not set correctly | Check `STUDENT_SCHEMA` env var |
| Freshness test fails | Wrong execution date | Verify date format (YYYY-MM-DD) |
| H3 test fails | Lat/lon swap | Check `h3.geo_to_h3(lat, lon)` order |
| Coordinate test fails | Wrong city data | Verify API location parameters |
| Null test fails | API response changed | Check API documentation for updates |
| Unique test fails | Duplicate ingestion | Use MERGE instead of INSERT |

## Contact

For questions about data quality tests, contact the Data Engineering team or refer to the [CAPSTONE_SETUP.md](CAPSTONE_SETUP.md) documentation.
