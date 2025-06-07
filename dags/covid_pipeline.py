from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import zipfile
import io

BUCKET_NAME = "covid19-raw-data-bucket"

WHO_DATA_URLS = {
    "global_daily": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-daily-data.csv",
    "global": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-data.csv",
    "table": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-table-data.csv",
    "monthly_death_by_age": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-monthly-death-by-age-data.csv",
    "hosp_icu": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-hosp-icu-data.csv",
    "vaccination": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/vaccination-data.csv",
}

MITRE_ZIP_URL = "https://mitre.box.com/shared/static/9iglv8kbs1pfi7z8phjl9sbpjk08spze.zip"


def upload_who_csvs_to_s3():
    s3 = S3Hook(aws_conn_id='aws_default')
    today_str = datetime.today().strftime('%Y-%m-%d')

    for name, url in WHO_DATA_URLS.items():
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f"Failed to download {name}. Status: {response.status_code}")

        s3_key = f"covid/raw/all_data/who_{name}_{today_str}.csv"
        s3.load_string(response.text, key=s3_key,
                       bucket_name=BUCKET_NAME, replace=True)
        print(f"Uploaded WHO file: {s3_key}")


def upload_mitre_zip_csvs_to_s3():
    s3 = S3Hook(aws_conn_id='aws_default')
    today_str = datetime.today().strftime('%Y-%m-%d')

    response = requests.get(MITRE_ZIP_URL)
    if response.status_code != 200:
        raise Exception(
            f"Failed to download MITRE zip. Status: {response.status_code}")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        for file_info in zip_file.infolist():
            if file_info.filename.endswith(".csv"):
                with zip_file.open(file_info) as f:
                    content = f.read().decode("utf-8")
                    base_name = file_info.filename.split(
                        '/')[-1].replace('.csv', '')
                    s3_key = f"covid/raw/all_data/mitre_{base_name}_{today_str}.csv"
                    s3.load_string(content, key=s3_key,
                                   bucket_name=BUCKET_NAME, replace=True)
                    print(f"Uploaded MITRE file: {s3_key}")


with DAG(
    dag_id="covid_all_data_to_s3_dag",
    start_date=datetime(2025, 6, 7),
    schedule_interval="@daily",
    catchup=False,
    tags=["covid", "who", "mitre", "s3", "aws"],
    description="Upload WHO and MITRE COVID-19 data to a single S3 folder",
) as dag:

    task_upload_who = PythonOperator(
        task_id="upload_who_csvs",
        python_callable=upload_who_csvs_to_s3
    )

    task_upload_mitre = PythonOperator(
        task_id="upload_mitre_zip_csvs",
        python_callable=upload_mitre_zip_csvs_to_s3
    )

    task_upload_who >> task_upload_mitre
