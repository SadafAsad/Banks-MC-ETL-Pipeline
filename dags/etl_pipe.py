# imports block
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import etl_ops
import pandas as pd

# DAG arguments block
default_args = {
    'owner': 'Sadaf Asad',
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition block
dag = DAG(
    'banks_market_cap_etl',
    default_args=default_args,
    description='DAG for ETL operations on banks market cap data',
    schedule_interval=timedelta(days=1),
)

# Define the extract task
def extract_task(**kwargs):
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attribs = ['Name', 'MC_USD_Billion']
    extracted_data = etl_ops.extract(url, table_attribs)
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

# Define the transform task
def transform_task(**kwargs):
    rates_csv_path = "./exchange_rates.csv"
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_task', key='extracted_data')
    transformed_data = etl_ops.transform(extracted_data, rates_csv_path)
    ti.xcom_push(key='transformed_data', value=transformed_data)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

# Define the load tasks
def load_to_csv_task(**kwargs):
    output_csv_path = './Countries_by_GDP.csv'
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
    etl_ops.load_to_csv(transformed_data, output_csv_path)

load_to_csv_task = PythonOperator(
    task_id='load_to_csv_task',
    python_callable=load_to_csv_task,
    provide_context=True,
    dag=dag,
)

def load_to_db_task(**kwargs):
    db_name = 'Banks_MC.db'
    table_name = 'banks'
    sql_connection = sqlite3.connect(db_name)
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
    etl_ops.load_to_db(transformed_data, sql_connection, table_name)
    sql_connection.close()

load_to_db_task = PythonOperator(
    task_id='load_to_db_task',
    python_callable=load_to_db_task,
    provide_context=True,
    dag=dag,
)

# Define the query tasks
def run_query_task(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

run_query_task_1 = PythonOperator(
    task_id='run_query_task_1',
    python_callable=run_query_task,
    op_args=["SELECT * FROM banks", 'sqlite:///Banks_MC.db'],
    dag=dag,
)

run_query_task_2 = PythonOperator(
    task_id='run_query_task_2',
    python_callable=run_query_task,
    op_args=["SELECT AVG(MC_GBP_Billion) FROM banks", 'sqlite:///Banks_MC.db'],
    dag=dag,
)

run_query_task_3 = PythonOperator(
    task_id='run_query_task_3',
    python_callable=run_query_task,
    op_args=["SELECT Name FROM banks LIMIT 5", 'sqlite:///Banks_MC.db'],
    dag=dag,
)

# Define the log progress task
log_progress_task = DummyOperator(
    task_id='log_progress_task',
    dag=dag,
)

# Pipeline
extract_task >> transform_task >> [load_to_csv_task, load_to_db_task] >> log_progress_task
log_progress_task >> [run_query_task_1, run_query_task_2, run_query_task_3]
