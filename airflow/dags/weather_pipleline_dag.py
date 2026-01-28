"""
Weather Forecasting Pipeline DAG
Runs daily at 2:00 AM to extract and load yesterday's weather data
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

# Add project paths
sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/scripts")


def extract_daily_weather():
    """Extract yesterday's weather data"""
    from scripts.extract_daily import main as extract_main
    
    print("=" * 80)
    print("TASK 1: Extracting yesterday's weather data")
    print("=" * 80)
    
    csv_path = extract_main()
    
    print(f"✅ Extraction completed: {csv_path}")
    return csv_path


def load_to_snowflake_incremental(**context):
    """Load extracted data to Snowflake (incremental mode)"""
    from scripts.utils import get_snowflake_connection, upload_dataframe
    
    print("=" * 80)
    print("TASK 2: Loading data to Snowflake (incremental)")
    print("=" * 80)
    
    # Get the CSV path from previous task
    ti = context['ti']
    csv_path = ti.xcom_pull(task_ids='extract_weather_data')
    
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Load CSV
    print(f"📂 Reading: {csv_path}")
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    print(f"✅ Loaded {len(df):,} records from CSV")
    
    # Get existing record_ids to prevent duplicates
    extract_date = df['datetime'].dt.date.iloc[0].strftime("%Y-%m-%d")
    print(f"📅 Extract date: {extract_date}")
    
    config_path = '/opt/airflow/config/snowflake_config.json'
    
    try:
        conn = get_snowflake_connection(config_path)
        cursor = conn.cursor()
        
        query = f"""
            SELECT record_id 
            FROM RAW.WEATHER_RAW 
            WHERE DATE(datetime) = '{extract_date}'
        """
        cursor.execute(query)
        existing_ids = {row[0] for row in cursor.fetchall()}
        
        cursor.close()
        conn.close()
        
        if existing_ids:
            print(f"   Found {len(existing_ids):,} existing records for {extract_date}")
            df = df[~df['record_id'].isin(existing_ids)].copy()
            print(f"   New records to insert: {len(df):,}")
        else:
            print(f"   No existing records for {extract_date}")
        
        if len(df) == 0:
            print("   ℹ️  All records already exist - nothing to load")
            return
        
    except Exception as e:
        print(f"   ⚠️  Could not check for duplicates: {str(e)}")
        print("   Proceeding with load...")
    
    # Upload to Snowflake
    upload_dataframe(
        df, 
        'WEATHER_RAW', 
        if_exists='append',
        config_path=config_path,
        verbose=True
    )
    
    print(f"✅ Successfully loaded {len(df):,} records to Snowflake")


# Default args for all tasks
default_args = {
    "owner": "pearson",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Create DAG
with DAG(
    dag_id="weather_forecast_pipeline",
    default_args=default_args,
    description="Daily weather data extraction and transformation pipeline",
    schedule_interval="0 2 * * *",  # Run at 2:00 AM daily
    start_date=datetime(2026, 1, 28),
    catchup=False,
    tags=["weather", "snowflake", "dbt", "daily"],
    max_active_runs=1,  # Only one run at a time
) as dag:

    # Task 1: Extract yesterday's weather data
    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_daily_weather,
        doc_md="""
        ### Extract Weather Data
        Extracts yesterday's weather data from Open-Meteo API for all Vietnam locations.
        - Locations: Hanoi, HCMC, Da Nang, Can Tho, Hai Phong
        - Data: 24 hours × 5 locations = 120 records
        - Output: CSV file in data/raw/
        """,
    )

    # Task 2: Load data to Snowflake (incremental)
    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake_incremental,
        provide_context=True,
        doc_md="""
        ### Load to Snowflake
        Loads extracted data to Snowflake RAW layer with duplicate prevention.
        - Checks for existing record_ids
        - Only inserts new records (incremental)
        - Target: RAW.WEATHER_RAW table
        """,
    )

    # Task 3: Run dbt seed (load location metadata)
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="""
        cd /opt/airflow/dbt_project &&
        dbt seed --profiles-dir /opt/airflow/dbt_project
        """,
        doc_md="""
        ### dbt Seed
        Loads seed data (location metadata, etc.) into Snowflake.
        """,
    )

    # Task 4: Run dbt models (transformations)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt_project &&
        dbt run --profiles-dir /opt/airflow/dbt_project
        """,
        doc_md="""
        ### dbt Run
        Executes all dbt transformations:
        - Staging: Data cleaning and validation
        - Intermediate: Feature engineering
        - Marts: Analytics-ready tables
        """,
    )

    # Task 5: Run dbt tests (data quality)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt_project &&
        dbt test --profiles-dir /opt/airflow/dbt_project
        """,
        doc_md="""
        ### dbt Test
        Runs data quality tests:
        - Not null checks
        - Unique key validation
        - Referential integrity
        - Custom business rules
        """,
    )

    # Task 6: Generate documentation (optional, runs weekly)
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
        cd /opt/airflow/dbt_project &&
        dbt docs generate --profiles-dir /opt/airflow/dbt_project
        """,
        doc_md="""
        ### dbt Docs
        Generates dbt documentation for the data pipeline.
        """,
        trigger_rule="all_done",  # Run even if tests fail
    )

    # Define task dependencies
    extract_task >> load_task >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs