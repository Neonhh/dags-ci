from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'example_postgres_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag:
    
    create_table = PostgresOperator(

        task_id='create_table',
        postgres_conn_id='postgres_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS example_table (
            id SERIAL PRIMARY KEY,
            value TEXT NOT NULL
        );
        """,

    )

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_conn',
        sql="""
        INSERT INTO example_table (value) VALUES ('example_value');
        INSERT INTO example_table (value) VALUES ('example_value2');
        """,
    )

    create_table >> insert_data
