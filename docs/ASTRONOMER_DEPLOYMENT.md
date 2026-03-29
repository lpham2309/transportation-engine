# Astronomer Deployment Guide

## Prerequisites

1. Astro CLI installed
2. Astronomer account and deployment created
3. Snowflake credentials ready
4. **Access to set environment variables in Astronomer deployment** (required)

## ⚠️ IMPORTANT: Environment Variables Required

This DAG **requires** environment variables to be set in your Astronomer deployment. The DAG will fail if these are not configured.

## Step 1: Set Environment Variables in Astronomer (REQUIRED)

### Option A: Via Astronomer UI (Easiest)

1. Log in to Astronomer Cloud: https://cloud.astronomer.io
2. Select your deployment
3. Go to **Environment** tab
4. Click **+ Add Variable**
5. Add the following environment variables:

| Key | Value | Secret |
|-----|-----|--------|
| `SNOWFLAKE_USER` | `lpham08` | No |
| `SNOWFLAKE_PASSWORD` | `PN*7if2mkYx!%7P5` | **Yes** ✅ |
| `STUDENT_SCHEMA` | `lpham08` | No |
| `SNOWFLAKE_ACCOUNT` | `aab46027.us-west-2` | No |
| `SNOWFLAKE_DATABASE` | `DATAEXPERT_STUDENT` | No |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` | No |
| `SNOWFLAKE_ROLE` | `ALL_USERS_ROLE` | No |
| `MBTA_API_KEY` | `` | Yes |
| `NOAA_API_TOKEN` | `` | Yes |
| `GOOGLE_MAPS_API_KEY` | `` | Yes |

6. Click **Save Changes**
7. Restart the deployment

### Option B: Via Astro CLI

```bash
# Get your deployment ID
astro deployment list

# Set environment variables (replace <deployment-id> with your actual ID)
astro deployment variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_USER --value lpham08

astro deployment variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_PASSWORD --value "PN*7if2mkYx!%7P5" --secret

astro deployment variable create --deployment-id <deployment-id> \
  --key STUDENT_SCHEMA --value lpham08

astro deployment variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_ACCOUNT --value aab46027.us-west-2

astro deployment variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_DATABASE --value DATAEXPERT_STUDENT

astro deployment variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_WAREHOUSE --value COMPUTE_WH

astro deployment variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_ROLE --value ALL_USERS_ROLE

astro deployment variable create --deployment-id <deployment-id> \
  --key MBTA_API_KEY --value "927a1073e9924c2bbc650bb965a21420" --secret

astro deployment variable create --deployment-id <deployment-id> \
  --key NOAA_API_TOKEN --value "JbJJIOeoCtNgXMHjFAoXDJRFeonTBpQP" --secret

astro deployment variable create --deployment-id <deployment-id> \
  --key GOOGLE_MAPS_API_KEY --value "AIzaSyBUQwOYG1gp0i9Mt7r82f_8ZvM6TEM8F-Y" --secret
```

## Step 2: Deploy to Astronomer

```bash
# Make sure you're in the project directory
cd /Users/lampham/dataexpert/airflow-dbt-project

# Deploy to Astronomer
astro deploy

# Or deploy to a specific deployment
astro deploy --deployment-id <deployment-id>
```

## Step 3: Verify Deployment

1. Wait for the deployment to complete (usually 2-5 minutes)
2. Go to Astronomer UI → your deployment
3. Click **Open Airflow** button
4. Check that the `boston_reliability_engine_ingestion` DAG appears without errors
5. Go to Admin → Variables to verify environment variables are loaded

## Alternative: Using Airflow Variables

If you can't use environment variables, you can use Airflow Variables instead:

### Via Airflow UI in Astronomer:

1. Open Airflow in Astronomer deployment
2. Go to **Admin → Variables**
3. Add the same variables listed above
4. The DAG code has a fallback to check Airflow Variables if environment variables aren't set

### Via Astro CLI:

```bash
astro deployment airflow-variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_USER --value lpham08

astro deployment airflow-variable create --deployment-id <deployment-id> \
  --key SNOWFLAKE_PASSWORD --value "PN*7if2mkYx!%7P5"

# ... repeat for other variables
```

## Troubleshooting

### Error: "Env var required but not provided: 'SNOWFLAKE_USER'"

**Cause:** Environment variables not set in Astronomer deployment

**Solution:**
1. Follow Step 1 to set environment variables in Astronomer UI
2. Restart the deployment
3. Wait for scheduler to reload DAGs (1-2 minutes)

### Error: "No module named 'include.eczachly.scripts.snowpark.load_boston_reliability_data'"

**Cause:** Missing `__init__.py` file

**Solution:**
- Ensure `include/eczachly/scripts/snowpark/__init__.py` exists
- Run `astro deploy` again

### DAG not appearing in Airflow UI

**Cause:** DAG import error

**Solution:**
1. In Astronomer UI, go to deployment logs
2. Check scheduler logs for import errors
3. Fix any errors and redeploy

## Testing the Deployed DAG

Once deployed:

1. In Airflow UI, find `boston_reliability_engine_ingestion` DAG
2. Toggle it **ON**
3. Click **Trigger DAG** (play button)
4. Monitor the DAG run in Graph view
5. Check logs for each task

Expected flow:
```
start → [ingest_mbta, ingest_noaa, ingest_google_maps] (parallel)
  → run_data_quality_tests
    → reliability_engine_cosmos_dag (dbt transformations)
      → end
```

## Production Schedule

The DAG is configured to run:
- **Schedule:** Daily at 1 AM ET
- **Catchup:** Disabled

To change the schedule, edit `dags/capstone/boston_reliability_engine_dag.py`:
```python
schedule="0 1 * * *",  # Cron format
```

## Monitoring

- **Logs:** Astronomer UI → Deployment → Logs
- **Metrics:** Astronomer UI → Deployment → Metrics
- **Alerts:** Configure in Astronomer UI → Deployment → Alerts

## Next Steps

1. ✅ Deploy to Astronomer
2. ✅ Set environment variables
3. ✅ Verify DAG loads successfully
4. ✅ Run the DAG manually once to test
5. ✅ Check Snowflake to verify data populated
6. 🔄 Let it run daily to build historical data
7. 📊 Build dashboards on top of gold tables
