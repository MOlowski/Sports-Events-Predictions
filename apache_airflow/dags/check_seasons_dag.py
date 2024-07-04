from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.check_seasons_functions import check


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_check_seasons = DAG(
    'check_seasons_dag',
    default_args = default_args,
    description = 'DAG finding new seasons',
    schedule_interval = '0 0 * * 0',
    start_date = days_ago(1),
    catchup = False,
)

def checking():
    check()



check_task = PythonOperator(
    task_id = 'check if there are new seasons',
    python_callable = checking,
    provide_context = True,
    dag = dag_check_seasons,
)


check_task