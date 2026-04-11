from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': 300,
}


def fetch_options_chain_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    import os
    from pipeline.ingestion.fetch_options import fetch_options_chain
    
    tickers = os.getenv("TARGET_TICKERS", "SPY").split(",")
    
    all_data = []
    for ticker in tickers:
        ticker = ticker.strip()
        df = fetch_options_chain(ticker)
        all_data.append(df)
    
    combined = pd.concat(all_data, ignore_index=True)
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    combined.to_parquet(f'/tmp/options_raw_{execution_date}.parquet', index=False)
    
    return f'/tmp/options_raw_{execution_date}.parquet'


def fetch_macro_rates_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    from pipeline.ingestion.fetch_macro import fetch_risk_free_rate
    
    rate = fetch_risk_free_rate()
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    with open(f'/tmp/risk_free_rate_{execution_date}.txt', 'w') as f:
        f.write(str(rate))
    
    return str(rate)


def upload_raw_to_minio_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    import pandas as pd
    from pipeline.storage.minio_client import MinioClient
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    options_path = f'/tmp/options_raw_{execution_date}.parquet'
    df = pd.read_parquet(options_path)
    
    client = MinioClient()
    client.upload_parquet(df, 'options', f'options_{execution_date}.parquet')
    
    return "Uploaded to MinIO"


def transform_and_compute_iv_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    import pandas as pd
    import os
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    options_path = f'/tmp/options_raw_{execution_date}.parquet'
    df = pd.read_parquet(options_path)
    
    with open(f'/tmp/risk_free_rate_{execution_date}.txt', 'r') as f:
        risk_free_rate = float(f.read())
    
    from pipeline.transform.clean import clean_options_data, add_moneyness, add_expiry_buckets
    from pipeline.transform.iv_surface import compute_iv_surface
    from pipeline.ingestion.fetch_options import fetch_underlying_price
    
    tickers = os.getenv("TARGET_TICKERS", "SPY").split(",")
    ticker = tickers[0].strip()
    
    spot_price = fetch_underlying_price(ticker)
    
    df = clean_options_data(df)
    df = add_moneyness(df, spot_price)
    df = add_expiry_buckets(df)
    
    df = compute_iv_surface(df, spot_price, risk_free_rate)
    
    df.to_parquet(f'/tmp/options_transformed_{execution_date}.parquet', index=False)
    
    return f'Transformed {len(df)} records'


def load_duckdb_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    import pandas as pd
    import os
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    df = pd.read_parquet(f'/tmp/options_transformed_{execution_date}.parquet')
    
    with open(f'/tmp/risk_free_rate_{execution_date}.txt', 'r') as f:
        risk_free_rate = float(f.read())
    
    from pipeline.transform.load_duckdb import DuckDBLoader
    
    loader = DuckDBLoader()
    loader.load_options_data(df, snapshot_date=execution_date, risk_free_rate=risk_free_rate)
    loader.close()
    
    return 'Loaded to DuckDB'


def validate_data_quality_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    import pandas as pd
    import os
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    df = pd.read_parquet(f'/tmp/options_transformed_{execution_date}.parquet')
    
    total = len(df)
    with_iv = df['implied_vol'].notna().sum()
    iv_pct = with_iv / total * 100 if total > 0 else 0
    
    print(f"Validation: {total} records, {with_iv} with IV ({iv_pct:.1f}%)")
    
    if iv_pct < 50:
        raise ValueError(f"Low IV calculation rate: {iv_pct:.1f}%")
    
    return f'Validated: {iv_pct:.1f}% IV success rate'


with DAG(
    dag_id='options_vol_surface_daily',
    default_args=default_args,
    description='Daily options IV surface pipeline',
    schedule_interval='0 18 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['options', 'volatility'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    fetch_options = PythonOperator(
        task_id='fetch_options_chain',
        python_callable=fetch_options_chain_task,
    )
    
    fetch_macro = PythonOperator(
        task_id='fetch_macro_rates',
        python_callable=fetch_macro_rates_task,
    )
    
    upload_raw = PythonOperator(
        task_id='upload_raw_to_minio',
        python_callable=upload_raw_to_minio_task,
    )
    
    transform = PythonOperator(
        task_id='transform_and_compute_iv',
        python_callable=transform_and_compute_iv_task,
    )
    
    load = PythonOperator(
        task_id='load_duckdb',
        python_callable=load_duckdb_task,
    )
    
    validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality_task,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [fetch_options, fetch_macro] >> upload_raw >> transform >> load >> validate >> end