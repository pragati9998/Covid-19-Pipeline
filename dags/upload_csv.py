from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests

BUCKET_NAME = "covid19-raw-data-bucket"
COVID_TRACKING_URL = "https://covidtracking.com/data/download/all-states-history.csv"


def upload_covid_tracking_to_s3():
    s3 = S3Hook(aws_conn_id='aws_default')
    response = requests.get(COVID_TRACKING_URL)
    if response.status_code != 200:
        raise Exception(
            f"Failed to download COVID tracking data. Status: {response.status_code}")

    today_str = datetime.today().strftime('%Y-%m-%d')
    s3_key = f"covid/raw/all_data/all_states_history_{today_str}.csv"
    s3.load_string(response.text, key=s3_key,
                   bucket_name=BUCKET_NAME, replace=True)
    print(f"Uploaded COVID Tracking file to S3: {s3_key}")


with DAG(
    dag_id="covid_tracking_to_s3_dag",
    start_date=datetime(2025, 6, 7),
    schedule_interval="@daily",
    catchup=False,
    tags=["covid", "tracking", "s3", "aws"],
    description="Download COVID Tracking all-states-history CSV and upload to S3",
) as dag:

    upload_task = PythonOperator(
        task_id="upload_covid_tracking_csv",
        python_callable=upload_covid_tracking_to_s3
    )
