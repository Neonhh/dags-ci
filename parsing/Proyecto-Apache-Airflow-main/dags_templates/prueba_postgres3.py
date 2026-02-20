from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def run_select_query(**kwargs):
    """Usa PostgresHook para obtener los registros y guardarlos en XCom."""
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    records = hook.get_records("SELECT * FROM example_table")
    # Envía los registros a XCom
    kwargs['ti'].xcom_push(key='my_records', value=records)

def print_query_results(**kwargs):
    """Recupera los registros desde XCom e imprime."""
    ti = kwargs['ti']
    query_results = ti.xcom_pull(key='my_records', task_ids='run_select_query')
    if query_results:
        for row in query_results:
            print(row)
    else:
        print("No rows returned.")

with DAG(
    'example_postgres_select_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    task_run_select = PythonOperator(
        task_id='run_select_query',
        python_callable=run_select_query,
    )

    task_print_results = PythonOperator(
        task_id='print_results',
        python_callable=print_query_results,
    )

    task_run_select >> task_print_results
