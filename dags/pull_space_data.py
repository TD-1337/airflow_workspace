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
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import timedelta

PROJ_ID = "aflow-training-rabo-2023-10-02"
DATASET_NAME = "td_dataset"
GCP_CONN_ID = "gcp_conn_id"
TABLE_NAME = "launches"
POSTGRES_CON_ID = "postgres"
POSTGRES_TABLE_NAME = "launches"

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
        write_disposition='WRITE_APPEND',  # Specify write disposition
        dag=dag,
    )

    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id=POSTGRES_CON_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
            id VARCHAR,
            name VARCHAR,
            status VARCHAR,
            country_code VARCHAR,
            service_provider_name VARCHAR,
            service_provider_type VARCHAR)
            ;
          """,
    )

    def _parquet_to_postgres(**context):

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CON_ID
        )
        pg_conn = pg_hook.get_sqlalchemy_engine()

        df = pd.read_parquet(f"/tmp/{context['ds']}.parquet")
        df.to_sql(POSTGRES_TABLE_NAME, pg_conn, if_exists="append", index=False)


    parquet_to_postgres = PythonOperator(
        task_id="parquet_to_postgres",
        python_callable=_parquet_to_postgres,
        provide_context=True,
        dag=dag,
    )



    # define dag base
    (
        sensor_api_live >>
        http_get_operator >>
        python_check_data_in_day >>
        preprocess_data
    )
    # dag part bq
    (
        preprocess_data >>
        create_empty_dataset_bq >>
        upload_file_gcs >>
        load_to_bigquery
    )
    # dag part postgreq
    (
        preprocess_data >>
        create_postgres_table >>
        parquet_to_postgres
    )