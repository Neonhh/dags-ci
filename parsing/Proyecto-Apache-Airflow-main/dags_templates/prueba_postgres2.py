from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def print_query_results(**kwargs):
    ti = kwargs['ti']
    query_results = ti.xcom_pull(task_ids='run_select_query')
    for row in query_results:
    	print(row)

with DAG(
    'example_postgres_select_pruebaotraforma',
    default_args=default_args,
    schedule_interval=None,
    catchup = False,
) as dag:
    
    run_select_query = PostgresOperator(
        task_id='run_select_query',
        postgres_conn_id='postgres_conn',
        sql="SELECT * FROM example_table",
        do_xcom_push=True,
    )

    print_results = PythonOperator(
        task_id='print_results',
        python_callable=print_query_results,
    )

    run_select_query >> print_results

   
