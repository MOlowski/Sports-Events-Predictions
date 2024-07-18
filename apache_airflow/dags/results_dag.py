from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.results_functions import get_results, check_bets

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'results_dag',
    default_args = default_args,
    description = 'DAG collecting recent results',
    schedule_interval = '0 0 * * 1,2,3,6',
    start_date = days_ago(1),
    catchup = False,
)

def get_recent_results():
    get_results()

def check_bets_result():
    check_bets()


get_recent_results_task = PythonOperator(
    task_id = 'get_recent_results',
    python_callable = get_recent_results,
    provide_context = True,
    dag = dag,
)

check_bets_result_task = PythonOperator(
    task_id = 'get_bets_results',
    python_callable = check_bets_result,
    provide_context = True,
    dag = dag,
)

get_recent_results_task >> check_bets_result_task