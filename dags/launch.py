from airflow.models import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="goodbye_world",
    start_date = pendulum.today('UTC'),
    description='This DAG will launch a rocket and destroy earth',
    schedule="@hourly",
    ) as dag:

    procure_rocket_material = EmptyOperator(task_id="proc_mats")
    procure_fuel = EmptyOperator(task_id="proc_fuel")
    build_stage_1 = EmptyOperator(task_id="build_1")
    build_stage_2 = EmptyOperator(task_id="build_2")
    build_stage_3 = EmptyOperator(task_id="build_3")
    launch = EmptyOperator(task_id="launch")

    build_stages = [build_stage_1, build_stage_2, build_stage_3]
    (procure_rocket_material >> build_stages)
    ([procure_rocket_material, procure_fuel] >> build_stage_3)
    (build_stages >> launch)
