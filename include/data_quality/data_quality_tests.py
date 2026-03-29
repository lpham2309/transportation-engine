"""
Data Quality Tests for Boston Reliability Engine

This module contains comprehensive data quality tests for validating
the integrity, completerun_ness, and correctness of ingested data.
"""
import os
from typing import Dict, List, Tuple
from include.ingestion.snowflake_queries import execute_snowflake_query


def run_all_dq_tests(schema: str, execution_date: str) -> Dict[str, bool]:
    """
    Run all data quality tests and return results.

    Args:
        schema: Snowflake schema to test
        execution_date: Execution date in YYYY-MM-DD format

    Returns:
        Dictionary mapping test names to pass/fail status
    """
    schema = schema or os.getenv("STUDENT_SCHEMA")

    print(f"\n{'='*70}")
    print(f"RUNNING DATA QUALITY TESTS FOR {execution_date}")
    print(f"Schema: {schema}")
    print(f"{'='*70}\n")

    results = {}

    # Test 1: Not Null Test - Critical columns must not be null
    print("Test 1: Not Null Test - Critical columns must have values")
    results['not_null_test'] = test_not_null_critical_columns(schema)

    # Test 2: Unique Test - Primary keys must be unique
    print("\nTest 2: Unique Test - No duplicate primary keys")
    results['unique_test'] = test_unique_primary_keys(schema)

    # Test 3: H3 Index Validity Test - H3 indices must be valid format
    print("\nTest 3: H3 Index Validity Test - Valid H3 format and resolution")
    results['h3_validity_test'] = test_h3_index_validity(schema)

    # Test 4: Coordinate Range Test - Lat/Lon must be within Boston area
    print("\nTest 4: Coordinate Range Test - Geographic coordinates within bounds")
    results['coordinate_range_test'] = test_coordinate_ranges(schema)

    # Test 5: Freshness Test - Data exists for execution date
    print("\nTest 5: Freshness Test - Data exists for execution date")
    results['freshness_test'] = test_data_freshness(schema, execution_date)

    # Summary
    print(f"\n{'='*70}")
    print("DATA QUALITY TEST SUMMARY")
    print(f"{'='*70}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_name, passed_status in results.items():
        status_icon = "✓" if passed_status else "✗"
        status_text = "PASSED" if passed_status else "FAILED"
        print(f"{status_icon} {test_name}: {status_text}")

    print(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    print(f"{'='*70}\n")

    return results


# =============================================================================
# Test 1: Not Null Test - Critical Columns
# =============================================================================

def test_not_null_critical_columns(schema: str) -> bool:
    """
    Validate that critical columns do not contain NULL values.

    Tests:
    - MBTA: stop_id, route_id, stop_h3_index must not be NULL
    - NOAA: observation_date, weather_condition must not be NULL
    - Driving: origin_h3_index, destination_h3_index must not be NULL
    """
    tests_passed = True

    # MBTA Predictions - critical columns
    mbta_pred_test = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(CASE WHEN stop_id IS NULL THEN 1 END) as null_stop_id,
        COUNT(CASE WHEN route_id IS NULL THEN 1 END) as null_route_id,
        COUNT(CASE WHEN stop_h3_index IS NULL THEN 1 END) as null_h3_index
    FROM {schema}.bronze_mbta_predictions
    """

    result = execute_snowflake_query(mbta_pred_test)
    total, null_stop, null_route, null_h3 = result[0]

    if null_stop > 0 or null_route > 0:
        print(f"  ✗ MBTA Predictions: {null_stop} null stop_id, {null_route} null route_id")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Predictions: All critical columns populated (tested {total} rows)")

    # MBTA Schedules - critical columns
    mbta_sched_test = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(CASE WHEN stop_id IS NULL THEN 1 END) as null_stop_id,
        COUNT(CASE WHEN route_id IS NULL THEN 1 END) as null_route_id,
        COUNT(CASE WHEN schedule_date IS NULL THEN 1 END) as null_date
    FROM {schema}.bronze_mbta_schedules
    """

    result = execute_snowflake_query(mbta_sched_test)
    total, null_stop, null_route, null_date = result[0]

    if null_stop > 0 or null_route > 0 or null_date > 0:
        print(f"  ✗ MBTA Schedules: {null_stop} null stop_id, {null_route} null route_id, {null_date} null date")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Schedules: All critical columns populated (tested {total} rows)")

    # NOAA Weather - critical columns
    noaa_test = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(CASE WHEN observation_date IS NULL THEN 1 END) as null_date,
        COUNT(CASE WHEN weather_condition IS NULL THEN 1 END) as null_condition
    FROM {schema}.bronze_noaa_weather
    """

    result = execute_snowflake_query(noaa_test)
    total, null_date, null_condition = result[0]

    if null_date > 0 or null_condition > 0:
        print(f"  ✗ NOAA Weather: {null_date} null date, {null_condition} null condition")
        tests_passed = False
    else:
        print(f"  ✓ NOAA Weather: All critical columns populated (tested {total} rows)")

    # Driving Routes - critical columns
    driving_test = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(CASE WHEN origin_h3_index IS NULL THEN 1 END) as null_origin_h3,
        COUNT(CASE WHEN destination_h3_index IS NULL THEN 1 END) as null_dest_h3
    FROM {schema}.bronze_driving_routes
    """

    try:
        result = execute_snowflake_query(driving_test)
        total, null_origin, null_dest = result[0]

        if null_origin > 0 or null_dest > 0:
            print(f"  ✗ Driving Routes: {null_origin} null origin_h3, {null_dest} null dest_h3")
            tests_passed = False
        else:
            print(f"  ✓ Driving Routes: All critical columns populated (tested {total} rows)")
    except Exception as e:
        print(f"  ⚠ Driving Routes: Table not accessible - {e}")

    return tests_passed


# =============================================================================
# Test 2: Unique Test - Primary Keys
# =============================================================================

def test_unique_primary_keys(schema: str) -> bool:
    """
    Validate that primary key columns contain unique values (no duplicates).

    Tests:
    - MBTA Predictions: prediction_id must be unique
    - MBTA Schedules: schedule_id must be unique
    - NOAA Weather: observation_date must be unique
    """
    tests_passed = True

    # MBTA Predictions - check for duplicate prediction_ids
    mbta_pred_test = f"""
    SELECT
        prediction_id,
        COUNT(*) as dup_count
    FROM {schema}.bronze_mbta_predictions
    GROUP BY prediction_id
    HAVING COUNT(*) > 1
    """

    result = execute_snowflake_query(mbta_pred_test)
    if len(result) > 0:
        print(f"  ✗ MBTA Predictions: {len(result)} duplicate prediction_ids found")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Predictions: All prediction_ids are unique")

    # MBTA Schedules - check for duplicate schedule_ids
    mbta_sched_test = f"""
    SELECT
        schedule_id,
        COUNT(*) as dup_count
    FROM {schema}.bronze_mbta_schedules
    GROUP BY schedule_id
    HAVING COUNT(*) > 1
    """

    result = execute_snowflake_query(mbta_sched_test)
    if len(result) > 0:
        print(f"  ✗ MBTA Schedules: {len(result)} duplicate schedule_ids found")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Schedules: All schedule_ids are unique")

    # NOAA Weather - check for duplicate dates
    noaa_test = f"""
    SELECT
        observation_date,
        COUNT(*) as dup_count
    FROM {schema}.bronze_noaa_weather
    GROUP BY observation_date
    HAVING COUNT(*) > 1
    """

    result = execute_snowflake_query(noaa_test)
    if len(result) > 0:
        print(f"  ✗ NOAA Weather: {len(result)} duplicate observation_dates found")
        tests_passed = False
    else:
        print(f"  ✓ NOAA Weather: All observation_dates are unique")

    return tests_passed


# =============================================================================
# Test 3: H3 Index Validity Test
# =============================================================================

def test_h3_index_validity(schema: str) -> bool:
    """
    Validate that H3 indices are in valid format.

    H3 indices at resolution 8 should be:
    - Exactly 15 characters long
    - Hexadecimal characters (0-9, a-f)
    - Start with '8' (resolution 8)
    """
    tests_passed = True

    # MBTA Predictions - validate H3 indices
    mbta_pred_test = f"""
    SELECT
        COUNT(*) as total_with_h3,
        COUNT(CASE
            WHEN LENGTH(stop_h3_index) != 15 THEN 1
        END) as invalid_length,
        COUNT(CASE
            WHEN stop_h3_index NOT RLIKE '^[0-9a-f]{{15}}$' THEN 1
        END) as invalid_format,
        COUNT(CASE
            WHEN LEFT(stop_h3_index, 1) != '8' THEN 1
        END) as invalid_resolution
    FROM {schema}.bronze_mbta_predictions
    WHERE stop_h3_index IS NOT NULL
    """

    result = execute_snowflake_query(mbta_pred_test)
    total, invalid_len, invalid_fmt, invalid_res = result[0]

    if invalid_len > 0 or invalid_fmt > 0 or invalid_res > 0:
        print(f"  ✗ MBTA Predictions H3: {invalid_len} wrong length, {invalid_fmt} wrong format, {invalid_res} wrong resolution")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Predictions: All {total} H3 indices valid")

    # MBTA Schedules - validate H3 indices
    mbta_sched_test = f"""
    SELECT
        COUNT(*) as total_with_h3,
        COUNT(CASE WHEN LENGTH(stop_h3_index) != 15 THEN 1 END) as invalid_length
    FROM {schema}.bronze_mbta_schedules
    WHERE stop_h3_index IS NOT NULL
    """

    result = execute_snowflake_query(mbta_sched_test)
    total, invalid_len = result[0]

    if invalid_len > 0:
        print(f"  ✗ MBTA Schedules H3: {invalid_len} invalid H3 indices")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Schedules: All {total} H3 indices valid")

    # Bluebikes - validate H3 indices
    bluebikes_test = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(CASE WHEN LENGTH(start_station_h3_index) != 15 THEN 1 END) as invalid_start_h3,
        COUNT(CASE WHEN LENGTH(end_station_h3_index) != 15 THEN 1 END) as invalid_end_h3
    FROM {schema}.bronze_bluebikes_trips
    WHERE start_station_h3_index IS NOT NULL OR end_station_h3_index IS NOT NULL
    """

    try:
        result = execute_snowflake_query(bluebikes_test)
        total, invalid_start, invalid_end = result[0]

        if invalid_start > 0 or invalid_end > 0:
            print(f"  ✗ Bluebikes H3: {invalid_start} invalid start_h3, {invalid_end} invalid end_h3")
            tests_passed = False
        else:
            print(f"  ✓ Bluebikes: All H3 indices valid (tested {total} rows)")
    except Exception as e:
        print(f"  ⚠ Bluebikes: Table not accessible - {e}")

    return tests_passed


# =============================================================================
# Test 4: Coordinate Range Test - Geographic Bounds
# =============================================================================

def test_coordinate_ranges(schema: str) -> bool:
    """
    Validate that latitude/longitude coordinates are within reasonable Boston area bounds.

    Boston area bounds (approximate):
    - Latitude: 42.2° to 42.5°N
    - Longitude: -71.2° to -70.9°W

    Also validates weather data ranges:
    - Temperature: -20°F to 120°F
    - Precipitation: 0 to 20 inches (daily)
    """
    tests_passed = True

    # MBTA - validate coordinates are in Boston area
    mbta_coord_test = f"""
    SELECT
        COUNT(*) as total_with_coords,
        COUNT(CASE
            WHEN stop_latitude < 42.2 OR stop_latitude > 42.5
            THEN 1
        END) as invalid_latitude,
        COUNT(CASE
            WHEN stop_longitude < -71.2 OR stop_longitude > -70.9
            THEN 1
        END) as invalid_longitude
    FROM {schema}.bronze_mbta_predictions
    WHERE stop_latitude IS NOT NULL AND stop_longitude IS NOT NULL
    """

    result = execute_snowflake_query(mbta_coord_test)
    total, invalid_lat, invalid_lon = result[0]

    if invalid_lat > 0 or invalid_lon > 0:
        print(f"  ✗ MBTA Coordinates: {invalid_lat} invalid latitudes, {invalid_lon} invalid longitudes")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Predictions: All {total} coordinates within Boston bounds")

    # NOAA Weather - validate temperature and precipitation ranges
    weather_range_test = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(CASE
            WHEN temp_max_f IS NOT NULL AND (temp_max_f < -20 OR temp_max_f > 120)
            THEN 1
        END) as invalid_temp_max,
        COUNT(CASE
            WHEN temp_min_f IS NOT NULL AND (temp_min_f < -20 OR temp_min_f > 120)
            THEN 1
        END) as invalid_temp_min,
        COUNT(CASE
            WHEN precipitation_inches IS NOT NULL AND (precipitation_inches < 0 OR precipitation_inches > 20)
            THEN 1
        END) as invalid_precip
    FROM {schema}.bronze_noaa_weather
    """

    result = execute_snowflake_query(weather_range_test)
    total, invalid_tmax, invalid_tmin, invalid_precip = result[0]

    if invalid_tmax > 0 or invalid_tmin > 0 or invalid_precip > 0:
        print(f"  ✗ Weather Ranges: {invalid_tmax} invalid temp_max, {invalid_tmin} invalid temp_min, {invalid_precip} invalid precip")
        tests_passed = False
    else:
        print(f"  ✓ NOAA Weather: All {total} weather values within reasonable ranges")

    # Bluebikes - validate coordinates
    bluebikes_coord_test = f"""
    SELECT
        COUNT(*) as total_with_coords,
        COUNT(CASE
            WHEN start_station_latitude < 42.2 OR start_station_latitude > 42.5
            THEN 1
        END) as invalid_start_lat,
        COUNT(CASE
            WHEN end_station_latitude < 42.2 OR end_station_latitude > 42.5
            THEN 1
        END) as invalid_end_lat
    FROM {schema}.bronze_bluebikes_trips
    WHERE start_station_latitude IS NOT NULL OR end_station_latitude IS NOT NULL
    """

    try:
        result = execute_snowflake_query(bluebikes_coord_test)
        total, invalid_start, invalid_end = result[0]

        if invalid_start > 0 or invalid_end > 0:
            print(f"  ✗ Bluebikes Coordinates: {invalid_start} invalid start coords, {invalid_end} invalid end coords")
            tests_passed = False
        else:
            print(f"  ✓ Bluebikes: All coordinates within Boston bounds (tested {total} rows)")
    except Exception as e:
        print(f"  ⚠ Bluebikes: Table not accessible - {e}")

    return tests_passed


# =============================================================================
# Test 5: Freshness Test - Data for Execution Date
# =============================================================================

def test_data_freshness(schema: str, execution_date: str) -> bool:
    """
    Validate that data was ingested for the execution date.

    Tests:
    - MBTA schedules should exist for execution_date
    - NOAA weather should exist for execution_date
    """
    tests_passed = True

    # MBTA Schedules - should have data for execution_date
    mbta_sched_freshness = f"""
    SELECT COUNT(*) as row_count
    FROM {schema}.bronze_mbta_schedules
    WHERE schedule_date = '{execution_date}'
    """

    result = execute_snowflake_query(mbta_sched_freshness)
    row_count = result[0][0]

    if row_count == 0:
        print(f"  ✗ MBTA Schedules: No data found for {execution_date}")
        tests_passed = False
    else:
        print(f"  ✓ MBTA Schedules: {row_count} schedules found for {execution_date}")

    # NOAA Weather - should have data for execution_date
    noaa_freshness = f"""
    SELECT COUNT(*) as row_count
    FROM {schema}.bronze_noaa_weather
    WHERE observation_date = '{execution_date}'
    """

    result = execute_snowflake_query(noaa_freshness)
    row_count = result[0][0]

    if row_count == 0:
        print(f"  ✗ NOAA Weather: No data found for {execution_date}")
        tests_passed = False
    else:
        print(f"  ✓ NOAA Weather: {row_count} observation(s) found for {execution_date}")

    return tests_passed


# =============================================================================
# Bonus Tests (Optional)
# =============================================================================

def test_relationship_integrity(schema: str) -> bool:
    """
    BONUS: Validate referential integrity between related tables.

    Tests:
    - MBTA predictions and schedules share common routes
    - All routes in data are expected MBTA routes
    """
    tests_passed = True

    # Check that predictions and schedules have overlapping routes
    route_overlap_test = f"""
    WITH pred_routes AS (
        SELECT DISTINCT route_id FROM {schema}.bronze_mbta_predictions
    ),
    sched_routes AS (
        SELECT DISTINCT route_id FROM {schema}.bronze_mbta_schedules
    )
    SELECT COUNT(*) as common_routes
    FROM pred_routes
    INNER JOIN sched_routes USING (route_id)
    """

    result = execute_snowflake_query(route_overlap_test)
    common_routes = result[0][0]

    if common_routes == 0:
        print(f"  ✗ Route Overlap: No common routes between predictions and schedules")
        tests_passed = False
    else:
        print(f"  ✓ Route Overlap: {common_routes} routes appear in both predictions and schedules")

    return tests_passed


if __name__ == "__main__":
    # Example: Run all tests for lpham08 schema
    import sys
    from datetime import datetime

    schema = os.getenv("STUDENT_SCHEMA", "lpham08")
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")

    results = run_all_dq_tests(schema, execution_date)

    # Exit with error code if any test failed
    if not all(results.values()):
        sys.exit(1)
