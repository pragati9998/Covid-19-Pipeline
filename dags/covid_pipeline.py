from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests

BUCKET_NAME = "covid19-raw-data-bucket"
WHO_DATA_URLS = {
    "global_daily": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-daily-data.csv",
    "global": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-data.csv",
    "table": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-table-data.csv",
    "monthly_death_by_age": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-monthly-death-by-age-data.csv",
    "hosp_icu": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-hosp-icu-data.csv",
    "vaccination": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/vaccination-data.csv",
}


def download_and_upload_who_data():
    s3 = S3Hook(aws_conn_id='aws_default')
    today_str = datetime.today().strftime('%Y-%m-%d')

    for name, url in WHO_DATA_URLS.items():
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f" Failed to download {name}. Status: {response.status_code}")

        s3_key = f"covid/raw/who/{name}_{today_str}.csv"
        s3.load_string(
            string_data=response.text,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f" Uploaded {name} to s3://{BUCKET_NAME}/{s3_key}")


with DAG(
    dag_id="who_covid_csv_to_s3_dag",
    start_date=datetime(2025, 5, 21),
    schedule_interval="@daily",
    catchup=False,
    tags=["covid", "who", "s3", "aws"],
    description="Download WHO COVID-19 CSV datasets and upload to S3",
) as dag:

    task_upload_who_data = PythonOperator(
        task_id="download_and_upload_who_data_to_s3",
        python_callable=download_and_upload_who_data
    )

    task_upload_who_data
