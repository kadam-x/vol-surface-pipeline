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
    import pandas as pd
    sys.path.insert(0, '/opt/airflow')

    import os
    from pipeline.ingestion.fetch_options import fetch_options_chain

    tickers = os.getenv("TARGET_TICKERS", "SPY").split(",")

    all_data = []
    for ticker in tickers:
        ticker = ticker.strip()
        print(f"Fetching options for {ticker}...")
        df = fetch_options_chain(ticker)
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)

    execution_date = context['ds']  # 'ds' is the standard Airflow date string YYYY-MM-DD
    path = f'/tmp/options_raw_{execution_date}.parquet'
    combined.to_parquet(path, index=False)

    print(f"Saved {len(combined)} records to {path}")
    return path


def fetch_macro_rates_task(**context):
    import sys
    sys.path.insert(0, '/opt/airflow')

    from pipeline.ingestion.fetch_macro import fetch_risk_free_rate

    rate = fetch_risk_free_rate()

    execution_date = context['ds']
    path = f'/tmp/risk_free_rate_{execution_date}.txt'
    with open(path, 'w') as f:
        f.write(str(rate))

    print(f"Risk-free rate: {rate}")
    return str(rate)


def upload_raw_to_minio_task(**context):
    import sys
    import pandas as pd
    sys.path.insert(0, '/opt/airflow')

    from pipeline.storage.minio_client import MinioClient

    execution_date = context['ds']
    path = f'/tmp/options_raw_{execution_date}.parquet'
    df = pd.read_parquet(path)

    client = MinioClient()
    client.upload_parquet(df, 'options', f'options_{execution_date}.parquet')

    print(f"Uploaded {len(df)} records to MinIO")
    return "Uploaded to MinIO"


def transform_and_compute_iv_task(**context):
    import sys
    import pandas as pd
    sys.path.insert(0, '/opt/airflow')

    execution_date = context['ds']

    df = pd.read_parquet(f'/tmp/options_raw_{execution_date}.parquet')

    with open(f'/tmp/risk_free_rate_{execution_date}.txt', 'r') as f:
        risk_free_rate = float(f.read())

    from pipeline.transform.clean import clean_options_data, add_moneyness, add_expiry_buckets
    from pipeline.transform.iv_surface import compute_iv_surface

    df = clean_options_data(df)
    df = add_moneyness(df)          # reads underlying_price from DataFrame
    df = add_expiry_buckets(df)
    df = compute_iv_surface(df, risk_free_rate)

    out_path = f'/tmp/options_transformed_{execution_date}.parquet'
    df.to_parquet(out_path, index=False)

    print(f"Transformed {len(df)} records with IV")
    return f'Transformed {len(df)} records'


def load_duckdb_task(**context):
    import sys
    import pandas as pd
    sys.path.insert(0, '/opt/airflow')

    from datetime import date
    from pipeline.transform.load_duckdb import load_to_duckdb

    execution_date = context['ds']

    df = pd.read_parquet(f'/tmp/options_transformed_{execution_date}.parquet')

    with open(f'/tmp/risk_free_rate_{execution_date}.txt', 'r') as f:
        risk_free_rate = float(f.read())

    snapshot_date = date.fromisoformat(execution_date)
    load_to_duckdb(df, risk_free_rate, snapshot_date=snapshot_date)

    print(f"Loaded {len(df)} rows for {execution_date}")
    return f'Loaded {len(df)} rows'


def validate_data_quality_task(**context):
    import sys
    import pandas as pd
    sys.path.insert(0, '/opt/airflow')

    execution_date = context['ds']
    df = pd.read_parquet(f'/tmp/options_transformed_{execution_date}.parquet')

    total = len(df)
    with_iv = df['implied_vol'].notna().sum()
    iv_pct = with_iv / total * 100 if total > 0 else 0

    print(f"Validation: {total} records, {with_iv} with IV ({iv_pct:.1f}%)")

    if total == 0:
        raise ValueError("No records in transformed output")
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

    start   = EmptyOperator(task_id='start')
    end     = EmptyOperator(task_id='end')

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

    start >> [fetch_options, fetch_macro]
    fetch_options >> upload_raw >> transform
    fetch_macro >> transform
    transform >> load >> validate >> end
