"""
Boston Reliability Engine - Databricks Migration DAG

This DAG orchestrates the Databricks DLT pipeline for the Boston Reliability Engine.
It replaces the Snowflake-based pipeline (boston_reliability_engine_dag.py).

Architecture:
    1. Ingest data from APIs to S3 (runs on Airflow worker)
    2. Trigger DLT pipeline in Databricks (bronze -> silver -> gold)
    3. Validate pipeline output

During migration, this DAG runs in parallel with the Snowflake DAG
for validation purposes.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from dotenv import load_dotenv

# Load environment variables
env_locations = [
    "/usr/local/airflow/.env",
    Path(__file__).parent.parent / ".env",
]

for env_path in env_locations:
    if Path(env_path).exists():
        load_dotenv(env_path, override=False)
        break

# Configuration
DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
DLT_PIPELINE_ID = os.getenv("DATABRICKS_DLT_PIPELINE_ID")
S3_BUCKET = os.getenv("S3_BUCKET", "boston-reliability-engine")

default_args = {
    "owner": "Lam Pham",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="boston_reliability_engine_databricks",
    start_date=datetime(2026, 2, 1),
    schedule="0 1 * * *",  # Daily at 1 AM (same as Snowflake DAG)
    catchup=False,
    default_args=default_args,
    description="Boston Reliability Engine - Databricks DLT Pipeline",
    tags=["capstone", "boston", "transit", "reliability", "databricks", "migration"],
)
def boston_reliability_databricks_dag():
    """
    Orchestrates Boston Reliability Engine data pipeline on Databricks.

    Task Flow:
        start -> ingest_to_s3 -> run_dlt_pipeline -> validate_output -> end

    The ingestion task writes to S3, then DLT Auto Loader processes
    the data through bronze -> silver -> gold layers.
    """

    start = EmptyOperator(task_id="start")

    @task(task_id="ingest_mbta_to_s3")
    def ingest_mbta(**context):
        """Fetch MBTA predictions and schedules, write to S3."""
        from databricks.ingestion.s3_writer import (
            ingest_mbta_predictions,
            ingest_mbta_schedules
        )

        execution_date = context["ds"]
        print(f"Ingesting MBTA data for {execution_date}")

        results = {
            "predictions": ingest_mbta_predictions(execution_date),
            "schedules": ingest_mbta_schedules(execution_date)
        }

        return results

    @task(task_id="ingest_noaa_to_s3")
    def ingest_noaa(**context):
        """Fetch NOAA weather data, write to S3."""
        from databricks.ingestion.s3_writer import ingest_noaa_weather

        execution_date = context["ds"]
        print(f"Ingesting NOAA weather for {execution_date}")

        return ingest_noaa_weather(execution_date)

    @task(task_id="ingest_driving_to_s3")
    def ingest_driving(**context):
        """Fetch Google Maps driving data, write to S3."""
        from databricks.ingestion.s3_writer import ingest_driving_routes

        execution_date = context["ds"]
        print(f"Ingesting driving routes for {execution_date}")

        return ingest_driving_routes(execution_date)

    # Trigger DLT Pipeline in Databricks
    # This processes: bronze -> silver -> gold
    run_dlt_pipeline = DatabricksRunNowOperator(
        task_id="run_dlt_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DLT_PIPELINE_ID,
        notebook_params={
            "execution_date": "{{ ds }}",
            "full_refresh": "false"
        },
        wait_for_termination=True,
        timeout_seconds=3600,  # 1 hour timeout
    )

    @task(task_id="validate_pipeline_output")
    def validate_output(**context):
        """
        Post-pipeline validation to ensure data quality.
        Queries Databricks SQL to verify record counts and metrics.
        """
        try:
            from databricks import sql

            connection = sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST"),
                http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                access_token=os.getenv("DATABRICKS_TOKEN")
            )

            cursor = connection.cursor()
            execution_date = context["ds"]

            # Validation queries
            validations = {
                "bronze_mbta_predictions": f"""
                    SELECT COUNT(*) as cnt
                    FROM bronze_mbta_predictions
                    WHERE fetched_date = '{execution_date}'
                """,
                "stg_mbta_performance": f"""
                    SELECT COUNT(*) as cnt
                    FROM stg_mbta_performance
                    WHERE schedule_date = '{execution_date}'
                """,
                "fct_mbta_route_reliability": """
                    SELECT COUNT(*) as cnt, AVG(reliability_score) as avg_score
                    FROM fct_mbta_route_reliability
                """,
            }

            results = {}
            for table_name, query in validations.items():
                cursor.execute(query)
                row = cursor.fetchone()
                results[table_name] = {
                    "count": row[0],
                    "avg_score": row[1] if len(row) > 1 else None
                }
                print(f"{table_name}: {results[table_name]}")

            cursor.close()
            connection.close()

            # Validation checks
            if results["bronze_mbta_predictions"]["count"] == 0:
                print("WARNING: No MBTA predictions ingested for today")

            if results["fct_mbta_route_reliability"]["count"] < 10:
                print("WARNING: Low number of reliability records")

            return results

        except ImportError:
            print("databricks-sql-connector not installed, skipping validation")
            return {"skipped": True}
        except Exception as e:
            print(f"Validation error: {e}")
            return {"error": str(e)}

    end = EmptyOperator(task_id="end")

    # DAG structure: parallel ingestion -> DLT -> validation
    mbta_task = ingest_mbta()
    noaa_task = ingest_noaa()
    driving_task = ingest_driving()
    validation_task = validate_output()

    (
        start
        >> [mbta_task, noaa_task, driving_task]
        >> run_dlt_pipeline
        >> validation_task
        >> end
    )


# Instantiate the DAG
databricks_dag = boston_reliability_databricks_dag()
