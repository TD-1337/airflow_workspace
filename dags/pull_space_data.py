from airflow.models import DAG
import json
import pandas as pd
from datetime import datetime, date
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.exceptions import AirflowSkipException
from datetime import timedelta

PROJ_ID = "aflow-training-rabo-2023-10-02"
DATASET_NAME = "td_dataset"
GCP_CONN_ID = "gcp_conn_id"
TABLE_NAME = "launches"

with DAG(
    dag_id="pull_space_flighs",
    start_date = datetime.now() - timedelta(days=10),
    description='Pulls all data of space flights in a day',
    schedule="@daily",
    ) as dag:

    sensor_api_live = HttpSensor(
        task_id="check_api_live",
        http_conn_id='thespacedevs_dev',
        endpoint="",
        dag=dag,
    )


    api_call_params = {
        "net__gte": "{{ ds }}T00:00:00Z", 
        "net__lt": "{{ next_ds }}T00:00:00Z",
    }

    http_get_operator = SimpleHttpOperator(
        task_id="pull_data_from_api",
        http_conn_id='thespacedevs_dev',
        method="GET",
        endpoint="",
        data=api_call_params,
        log_response=True,
        dag=dag
    )

    def _check_if_data_in_request(task_instance, **context):
        response = task_instance.xcom_pull(task_ids="pull_data_from_api", key="return_value")
        response_dict = json.loads(response)

        if response_dict["count"] == 0:
            raise AirflowSkipException(f"No data found on day {context['ds']}")

    python_check_data_in_day = PythonOperator(
        task_id="python_check_response_filled",
        python_callable=_check_if_data_in_request,
        provide_context=True,
        dag=dag,
    )

    def _extract_relevant_data(x: dict):
        return {
            "id": x.get("id"),
            "name": x.get("name"),
            "status": x.get("status").get("abbrev"),
            "country_code": x.get("pad").get("country_code"),
            "service_provider_name": x.get("launch_service_provider").get("name"),
            "service_provider_type": x.get("launch_service_provider").get("type")
        }

    def _preprocess_data(task_instance, **context):
        response = task_instance.xcom_pull(task_ids="pull_data_from_api")
        response_dict = json.loads(response)
        response_results = response_dict["results"]
        df_results = pd.DataFrame([_extract_relevant_data(i) for i in response_results])
        df_results.to_parquet(path=f"/tmp/{context['ds']}.parquet")

    preprocess_data = PythonOperator(
        task_id="preprocess_data",
        python_callable=_preprocess_data
    )

    create_empty_dataset_bq = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_bq",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJ_ID,
        dataset_id=DATASET_NAME,
        dag=dag,
    )

    upload_file_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src="/tmp/{{ ds }}.parquet",
        dst="td/launches/{{ ds }}.parquet",
        bucket=PROJ_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket="aflow-training-rabo-2023-10-02",
        source_objects="td/launches/{{ ds }}.parquet",
        destination_project_dataset_table=f'{PROJ_ID}.{DATASET_NAME}.{TABLE_NAME}',
        source_format='PARQUET',
        gcp_conn_id=GCP_CONN_ID,  # Airflow connection to BigQuery
        write_disposition='WRITE_TRUNCATE',  # Specify write disposition
        dag=dag,
    )

    # create_or_upsert_postgres_table = PostgresOperator(
    #     task_id="create_or_upsert_postgres_table",
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS pet (
    #         id SERIAL PRIMARY KEY,
    #         name VARCHAR NOT NULL,
    #         status VARCHAR NOT NULL,
    #         country_code VARCHAR NOT NULL,
    #         service_provider_name VARCHAR NOT NULL,
    #         service_provider_type VARCHAR NOT NULL)
            
    #         INSERT INTO 
    #         customers (name, email)
    #         VALUES 
    #         ('IBM', 'contact@ibm.com'),
    #         ('Microsoft', 'contact@microsoft.com'),
    #         ('Intel', 'contact@intel.com');
            
    #         ;
    #       """,
    # )

    # define dag order
    (
        sensor_api_live >>
        http_get_operator >>
        python_check_data_in_day >>
        preprocess_data >>
        create_empty_dataset_bq >>
        upload_file_gcs >>
        load_to_bigquery
    )