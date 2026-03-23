import os
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from include.ingestion.scripts.snowpark.load_boston_reliability_data import load_boston_reliability_data
from include.data_quality.data_quality_tests import run_all_dq_tests
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env for local development
# In Astronomer deployment, environment variables must be set in deployment settings
env_locations = [
    "/usr/local/airflow/.env",  # Astronomer default
    Path(__file__).parent.parent.parent / ".env",  # Project root
]

for env_path in env_locations:
    if Path(env_path).exists():
        load_dotenv(env_path, override=False)
        break



# Determine Airflow home - use actual AIRFLOW_HOME or fallback for local testing
airflow_home = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')

PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_PROFILES = f'{airflow_home}/dbt_project/profiles.yml'

profile_config = ProfileConfig(
    profile_name="reliability_engine",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)


default_args = {
    "owner": "Lam Pham",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="boston_reliability_engine_ingestion",
    start_date=datetime(2026, 2, 1),
    schedule="0 1 * * *",  # Daily at 1 AM
    # schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    default_args=default_args,
    description="Ingest MBTA, NOAA, and Google Maps data into Snowflake Bronze layer",
    tags=["capstone", "boston", "transit", "reliability"],
)
def boston_reliability_engine_dag():
    """Main DAG for Boston Reliability Engine data ingestion"""

    start = EmptyOperator(task_id="start")

    @task(task_id="ingest_mbta_data")
    def ingest_mbta(**context):
        """Fetch MBTA predictions and schedules"""
        execution_date = context['ds']
        print(f"Ingesting MBTA data for {execution_date}")
        return load_boston_reliability_data(execution_date, data_source="mbta")

    @task(task_id="ingest_noaa_weather")
    def ingest_noaa(**context):
        """Fetch NOAA weather data"""
        execution_date = context['ds']
        print(f"Ingesting NOAA weather data for {execution_date}")
        return load_boston_reliability_data(execution_date, data_source="noaa")

    @task(task_id="ingest_google_maps_driving")
    def ingest_google_maps(**context):
        """Fetch Google Maps driving data for sample routes"""
        execution_date = context['ds']
        print(f"Ingesting Google Maps driving data for {execution_date}")
        return load_boston_reliability_data(execution_date, data_source="google_maps")

    @task(task_id="run_data_quality_tests")
    def run_dq_tests(**context):
        """Run comprehensive data quality tests"""
        execution_date = context['ds']
        schema = os.getenv("STUDENT_SCHEMA")

        print(f"Running data quality tests for {execution_date}")
        dq_results = run_all_dq_tests(schema, execution_date)

        # Check if any tests failed
        failed_tests = [test_name for test_name, passed in dq_results.items() if not passed]

        if failed_tests:
            print(f"\n⚠️  WARNING: {len(failed_tests)} data quality test(s) failed:")
            for test_name in failed_tests:
                print(f"   - {test_name}")
            # Note: Not raising error to allow DAG to complete
            # In production, you may want to raise an exception here
        else:
            print(f"\n✅ All {len(dq_results)} data quality tests passed!")

        return dq_results

    def execute_reliability_engine_dbt_workflow():
        # Determine dbt executable path - use venv if exists, otherwise system dbt
        dbt_venv_path = f"{os.getenv('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt"
        if os.path.exists(dbt_venv_path):
            dbt_executable = dbt_venv_path
        else:
            # Fallback to system dbt for local testing
            import shutil
            dbt_executable = shutil.which("dbt") or "dbt"

        dbt_run_staging = DbtTaskGroup(
            group_id="reliability_engine_cosmos_dag",
            project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
            profile_config=profile_config,
            execution_config=ExecutionConfig(
                dbt_executable_path=dbt_executable,
            ),
            render_config=RenderConfig(
                select=["tag:capstone"],
            ),
        )

        return dbt_run_staging

    end = EmptyOperator(task_id="end")

    # DAG structure - all ingestion tasks run in parallel
    mbta_task = ingest_mbta()
    noaa_task = ingest_noaa()
    google_maps_task = ingest_google_maps()
    dq_task = run_dq_tests()
    dbt_transformation_task = execute_reliability_engine_dbt_workflow()

    start >> [mbta_task, noaa_task, google_maps_task] >> dq_task >> dbt_transformation_task >> end


# Instantiate the DAG
boston_reliability_dag = boston_reliability_engine_dag()
