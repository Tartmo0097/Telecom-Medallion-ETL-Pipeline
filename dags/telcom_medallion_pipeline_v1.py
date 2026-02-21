import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# --- CONFIGURATION ---
SUB_PATH = '/opt/airflow/data/subscribers/'
USAGE_PATH = '/opt/airflow/data/usage/'
CONN_ID = 'postgres_default'

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='telcom_medallion_taskflow',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'telecom']
)
def telcom_medallion_pipeline():

    # --- 1. SETUP TASK (Schema & Table Creation) ---
    init_db = PostgresOperator(
        task_id='initialize_db_structure',
        postgres_conn_id=CONN_ID,
        sql="""
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE SCHEMA IF NOT EXISTS silver;
            CREATE SCHEMA IF NOT EXISTS gold;

            CREATE TABLE IF NOT EXISTS silver.subscribers (
                subscriber_id VARCHAR,
                full_name VARCHAR,
                email VARCHAR,
                gender VARCHAR,
                region VARCHAR,
                state VARCHAR,
                msisdn VARCHAR,
                is_email_valid BOOLEAN,
                processed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS silver.usage (
                msisdn VARCHAR,
                voice_min NUMERIC,
                data_gb NUMERIC,
                processed_at TIMESTAMP
            );
        """
    )

    # --- 2. BRONZE TASKS  ---
    @task
    def load_parquet_to_bronze(folder_path: str, table_name: str, batch_size: int = 50000):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        
        files = sorted(Path(folder_path).glob('*.parquet'))
        if not files:
            raise FileNotFoundError(f"No files found in {folder_path}")

        first_chunk = True
        for file in files:
            pf = pq.ParquetFile(str(file))
            for record_batch in pf.iter_batches(batch_size=batch_size):
                df_chunk = pa.Table.from_batches([record_batch]).to_pandas()
                # 'replace' on first batch to wipe old bronze data, 'append' thereafter
                mode = 'replace' if first_chunk else 'append'
                df_chunk.to_sql(name=table_name, con=engine, schema='bronze', 
                                if_exists=mode, index=False, method='multi')
                first_chunk = False
        return f"Successfully loaded {table_name} to bronze"

    # --- 3. SILVER TASKS (Cleaning & Rounding) ---
    silver_subscribers = PostgresOperator(
        task_id='transform_subs_silver',
        postgres_conn_id=CONN_ID,
        sql="""
            TRUNCATE TABLE silver.subscribers;
            INSERT INTO silver.subscribers (
                subscriber_id, full_name, email, gender, 
                region, state, msisdn, is_email_valid, processed_at
            )
            SELECT DISTINCT ON (subscriber_id)
                subscriber_id, 
                TRIM(LOWER(full_name)), 
                TRIM(LOWER(email)),
                CASE WHEN LOWER(gender) IN ('male', 'm') THEN 'm' 
                     WHEN LOWER(gender) IN ('female', 'f') THEN 'f' 
                     ELSE 'unknown' END,
                region, 
                state, 
                TRIM(LOWER(msisdn)),
                (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
                NOW()
            FROM bronze.subscribers;
        """
    )

    silver_usage = PostgresOperator(
        task_id='transform_usage_silver',
        postgres_conn_id=CONN_ID,
        sql="""
            TRUNCATE TABLE silver.usage;
            INSERT INTO silver.usage (msisdn, voice_min, data_gb, processed_at)
            SELECT 
                TRIM(LOWER(msisdn)), 
                ROUND(CAST(COALESCE(voice_min, 0) AS NUMERIC), 2),
                ROUND(CAST(COALESCE(data_gb, 0) AS NUMERIC), 4),
                NOW()
            FROM bronze.usage;
        """
    )

    # --- 4. GOLD TASKS (Fact & Dimension) ---
    gold_dim_subs = PostgresOperator(
        task_id='update_dim_subscribers_scd2',
        postgres_conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS gold.dim_subscribers (
                subscriber_id VARCHAR, full_name VARCHAR, region VARCHAR,
                state VARCHAR, start_date TIMESTAMP, end_date TIMESTAMP, is_current BOOLEAN
            );

            -- Expire old records if location changed
            UPDATE gold.dim_subscribers SET end_date = NOW(), is_current = false
            FROM silver.subscribers s 
            WHERE gold.dim_subscribers.subscriber_id = s.subscriber_id
              AND gold.dim_subscribers.is_current = true
              AND (gold.dim_subscribers.region != s.region OR gold.dim_subscribers.state != s.state);

            -- Add new records
            INSERT INTO gold.dim_subscribers (subscriber_id, full_name, region, state, start_date, is_current)
            SELECT s.subscriber_id, s.full_name, s.region, s.state, NOW(), true
            FROM silver.subscribers s
            LEFT JOIN gold.dim_subscribers g ON s.subscriber_id = g.subscriber_id AND g.is_current = true
            WHERE g.subscriber_id IS NULL;
        """
    )

    gold_fact_activity = PostgresOperator(
        task_id='update_fact_activity',
        postgres_conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS gold.fact_subscriber (
                subscriber_id VARCHAR, msisdn VARCHAR, total_voice_min NUMERIC,
                total_data_gb NUMERIC, session_count INT, gold_updated_at TIMESTAMP
            );
            TRUNCATE TABLE gold.fact_subscriber;
            INSERT INTO gold.fact_subscriber
            SELECT 
                s.subscriber_id, 
                s.msisdn, 
                ROUND(SUM(u.voice_min), 2), 
                ROUND(SUM(u.data_gb), 4), 
                COUNT(u.*), 
                NOW()
            FROM silver.subscribers s
            INNER JOIN silver.usage u ON s.msisdn = u.msisdn 
            GROUP BY s.subscriber_id, s.msisdn;
        """
    )

    reporting_view = PostgresOperator(
        task_id='create_reporting_views',
        postgres_conn_id=CONN_ID,
        sql="""
            CREATE OR REPLACE VIEW gold.v_business_summary AS
            SELECT s.full_name, s.region, f.total_data_gb, f.session_count,
            CASE WHEN f.total_data_gb > 10 THEN 'High' ELSE 'Normal' END as tier
            FROM gold.dim_subscribers s 
            JOIN gold.fact_subscriber f ON s.subscriber_id = f.subscriber_id 
            WHERE s.is_current = true;
        """
    )

    # --- 5. EXECUTION FLOW ---
    subs_data = load_parquet_to_bronze(SUB_PATH, 'subscribers')
    usage_data = load_parquet_to_bronze(USAGE_PATH, 'usage')

    # Dependencies
    init_db >> [subs_data, usage_data]
    subs_data >> silver_subscribers
    usage_data >> silver_usage
    [silver_subscribers, silver_usage] >> gold_dim_subs >> gold_fact_activity >> reporting_view

telcom_medallion_pipeline()