from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.date_time import DateTimeSensor
from scripts.bets_functions import get_proper_odds, send_to_sql



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_bets = DAG(
    'bets_dag',
    default_args = default_args,
    description = 'DAG collecting bets',
    schedule_interval = '0 0 * * 5',
    start_date = days_ago(1),
    catchup = False,
)

def get_bets(**kwargs):
    bets_df = get_proper_odds()
    ti = kwargs['ti']
    ti.xcom_push(key='bets', value = bets_df)

def send_to_db(**kwargs):
    ti = kwargs['ti']
    bets_df = ti.xcom_pull(key='bets', task_ids = 'get_bets')
    send_to_sql(bets_df)

start = DummyOperator(
    task_id = 'start',
    dag = dag_bets
)

wait_for_odds = ExternalTaskSensor(
    task_id = 'wait_for_odds',
    external_dag_id = 'odds_dag',
    enternal_task_id = 'end',
    allowed_states = ['success'],
    execution_delta = timedelta(minutes=2),
    timeout = 120,
    dag = dag_bets
)

get_bets_task = PythonOperator(
    task_id = 'get_bets',
    python_callable = get_bets,
    provide_context = True,
    dag = dag_bets,
)

send_bets_task = PythonOperator(
    task_id = 'send_bets',
    python_callable = send_to_db,
    provide_context = True,
    dag = dag_bets,
)

end = DummyOperator(
    task_id = 'end',
    dag = dag_bets
)

delay = DateTimeSensor(
    task_id = 'delay',
    target_time = "{{ task_instance.start_date + macros.timedelta(minutes=10) }}",
    dag = dag_bets
)

start >> wait_for_odds >> delay >> get_bets_task >> send_bets_task >> end