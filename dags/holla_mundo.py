from airflow.models import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta

with DAG(
    dag_id="holla_world",
    start_date = pendulum.today('UTC'),
    description='Prints hello world',
    schedule="@hourly",
    ) as dag:

    holla = BashOperator(
        task_id="task_id_bash",
        bash_command="echo {{ task_id }} is running in the {{ dag_id }} pipeline"
        dag=dag
    )

    def python_callable(**context):
        print(f"This script was executed at {context['templates_dict']['execution_date']}")
        print(f"Three days after execution is {context['start_date'] + timedelta(days=3)}")
    
    mundo = PythonOperator(
        task_id="task_id_py"
        dag=dag
        python_callable=python_callable
    )