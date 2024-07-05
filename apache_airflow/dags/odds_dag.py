from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from scripts.odds_functions import get_predicted_matches, get_odds, send_to_sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_odds = DAG(
    'odds_dag',
    default_args = default_args,
    description = 'DAG collecting odds',
    schedule_interval = '0 0 * * 4,5',
    start_date = days_ago(1),
    catchup = False,
)



def get_matches(**kwargs):
    matches, start_date, end_date = get_predicted_matches()
    ti = kwargs['ti']
    ti.xcom_push(key='matches', value = matches)
    ti.xcom_push(key='start_date', value = start_date)
    ti.xcom_push(key='end_date', value = end_date)

def collect_odds(**kwargs):
    ti = kwargs['ti']
    matches = ti.xcom_pull(key='matches', task_ids = 'get_matches')
    start_date = ti.xcom_pull(key='start_date', task_ids = 'get_matches')
    end_date = ti.xcom_pull(key='end_date', task_ids = 'get_matches')
    odds_df = get_odds(matches, start_date, end_date)
    ti.xcom_push(key='odds_df', value = odds_df)

def send_to_postgresql(**kwargs):
    ti = kwargs['ti']
    odds_df = ti.xcom_pull(key='odds_df', task_ids = 'collect_odds')
    send_to_sql(odds_df)


get_matches_task = PythonOperator(
    task_id = 'get_matches',
    python_callable = get_matches,
    provide_context = True,
    dag = dag_odds,
)

collect_odds_task = PythonOperator(
    task_id = 'collect_odds',
    python_callable = collect_odds,
    provide_context = True,
    dag = dag_odds,
)

send_to_sql_task = PythonOperator(
    task_id = 'send_to_sql',
    python_callable = send_to_postgresql,
    provide_context = True,
    dag = dag_odds,
)

end = DummyOperator(
    task_id = 'end',
    dag = dag_odds
)

get_matches_task >> collect_odds_task >> send_to_sql_task >> end