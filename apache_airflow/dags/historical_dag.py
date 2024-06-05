from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.historical_functions import get_data, get_matches, encode_data, encode_fix_stats, data_to_sql, get_and_preproc_historical_data, send_to_postgresql_historical_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'historical_data_dag',
    default_args = default_args,
    description = 'DAG collecting past data',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2024, 6, 5),
    catchup = False,
)

def get_preprocess_past_data(**kwargs):
    teams_df, team_stats_df, fixtures_df, fixture_stats_df = get_and_preproc_historical_data()
    ti = kwargs['ti']
    ti.xcom_push(key='teams_df', value = teams_df)
    ti.xcom_push(key='team_stats_df', value = team_stats_df)
    ti.xcom_push(key='fixtures_df', value = fixtures_df)
    ti.xcom_push(key='fixture_stats_df', value = fixture_stats_df)

def send_to_db(**kwargs):
    ti = kwargs['ti']
    teams_df = ti.xcom_pull(key='teams_df', task_ids = 'get_preprocess_past_data')
    team_stats_df = ti.xcom_pull(key='team_stats_df', task_ids = 'get_preprocess_past_data')
    fixtures_df = ti.xcom_pull(key='fixtures_df', task_ids = 'get_preprocess_past_data')
    fixture_stats_df = ti.xcom_pull(key='fixture_stats_df', task_ids = 'get_preprocess_past_data')

    send_to_postgresql_historical_data(teams_df, team_stats_df, fixtures_df, fixture_stats_df)

get_preprocess_past_data_task = PythonOperator(
    task_id = 'get_preprocess_past_data',
    python_callable = get_preprocess_past_data,
    provide_context = True,
    dag = dag,
)

send_to_postgresql_historical_data_task = PythonOperator(
    task_id = 'send_to_postgresql_historical_data',
    python_callable = send_to_postgresql_historical_data,
    provide_context = True,
    dag = dag,
)

get_preprocess_past_data_task >> send_to_postgresql_historical_data_task