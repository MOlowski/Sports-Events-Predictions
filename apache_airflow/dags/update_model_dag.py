from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.update_model_functions import future_engineering, update_models



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_update_model = DAG(
    'model_update_dag',
    default_args = default_args,
    description = 'DAG updating dl models',
    schedule_interval = '@monthly', # or '0 0 1 * *'
    start_date = days_ago(1),
    catchup = False,
)

def perform_fe():
    future_engineering()

def upd_models():
    update_models()

preform_fe_task = PythonOperator(
    task_id = 'preform_future_engineering',
    python_callable = perform_fe,
    provide_context = True,
    dag = dag_update_model,
)

update_models_task = PythonOperator(
    task_id = 'update_models',
    python_callable = upd_models,
    provide_context = True,
    dag = dag_update_model,
)

preform_fe_task >> update_models_task