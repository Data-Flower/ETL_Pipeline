from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="hello",
    default_args={
        "depends_on_past":False,
        "retries":1,
        "retry_delay": timedelta(minutes=1),
    },

    description="test",
    start_date=days_ago(2),
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    def print_airflow() -> None:
        print('airflow')

    t1 = BashOperator(
        task_id = "print hello",
        bash_command="echo Hello",
    )

    t2 = PythonOperator(
        task_id="print airflow",
        python_callable=print_airflow,
    )

    t1 >> t2