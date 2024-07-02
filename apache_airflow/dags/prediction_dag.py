from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.prediction_functions import get_preprocess_data, predict, send_to_sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'predicting_dag',
    default_args = default_args,
    description = 'DAG predicting results',
    schedule_interval = '0 0 * * 4',
    start_date = days_ago(1),
    catchup = False,
)

def preprocess_data(**kwargs):
    predict_df = get_preprocess_data()
    ti = kwargs['ti']
    ti.xcom_push(key='df_to_predict', value = predict_df)

def prediction(**kwargs):
    ti = kwargs['ti']
    predict_df = ti.xcom_pull(key='df_to_predict', task_ids = 'preprocess_data')
    predictions = predict(predict_df)
    ti.xcom_push(key='predictions_df', value = predictions)

def send_predictions_to_sql(**kwargs):
    ti = kwargs['ti']
    predictions = ti.xcom_pull(key='predictions_df', task_ids = 'prediction')
    send_to_sql(predictions)

preprocess_data_task = PythonOperator(
    task_id = 'preprocess_data',
    python_callable = preprocess_data,
    provide_context = True,
    dag = dag,
)

predict_task = PythonOperator(
    task_id = 'prediction',
    python_callable = prediction,
    provide_context = True,
    dag = dag,
)

send_to_sql_task = PythonOperator(
    task_id = 'send_to_sql',
    python_callable = send_predictions_to_sql,
    provide_context = True,
    dag = dag,
)

preprocess_data_task >> predict_task >> send_to_sql_task