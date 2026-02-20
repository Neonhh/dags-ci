from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
}

def extract_and_stage(**kwargs):

    # Conexión a la BD de origen (source_db)
    source_hook = PostgresHook(postgres_conn_id='source_db')
    records = source_hook.get_records("SELECT * FROM source_table")
    print("Extract: Registros extraídos de source_db:", records)
    
    # Conexión a la BD de staging (staging_db)
    staging_hook = PostgresHook(postgres_conn_id='staging_db')
    
    # Elimina la tabla 'staging_table' si ya existe y la crea nuevamente.
    staging_hook.run("DROP TABLE IF EXISTS staging_table;")
    staging_hook.run("""
        CREATE TABLE staging_table (
            id INTEGER,
            name VARCHAR(255),
            age INTEGER,
            email VARCHAR(255)
        );
    """)
    print("Staging: Tabla 'staging_table' creada en staging_db.")
    
    # Inserta cada registro extraído de source_table en staging_table
    insert_sql = """
        INSERT INTO staging_table (id, name, age, email)
        VALUES (%s, %s, %s, %s);
    """
    for record in records:
        staging_hook.run(insert_sql, parameters=record)
    print("Load: Registros insertados en 'staging_table'.")


def transform_data(**kwargs):
   
    staging_hook = PostgresHook(postgres_conn_id='staging_db')

    # Se actualiza la columna 'name' convirtiéndola a mayúsculas
    update_sql = "UPDATE staging_table SET name = UPPER(name);"

    staging_hook.run(update_sql)
    print("Transform: staging_table actualizada con los datos transformados (name en mayúsculas).")


def load_data(**kwargs):
    
    # Conexión a staging_db para obtener los datos transformados
    staging_hook = PostgresHook(postgres_conn_id='staging_db')
    records = staging_hook.get_records("SELECT * FROM staging_table")
    print("Load: Datos transformados obtenidos desde staging_table:\n", records)
    
    # Conexión a la BD de destino (destination_db)
    dest_hook = PostgresHook(postgres_conn_id='destination_db')
    
    # Elimina y crea la tabla 'destination_table'
    dest_hook.run("DROP TABLE IF EXISTS destination_table;")
    dest_hook.run("""
        CREATE TABLE destination_table (
            id INTEGER,
            name VARCHAR(255),
            age INTEGER,
            email VARCHAR(255)
        );
    """)
    
    # Inserta los datos en destination_table
    insert_sql = """
        INSERT INTO destination_table (id, name, age, email)
        VALUES (%s, %s, %s, %s);
    """

    for record in records:
        dest_hook.run(insert_sql, parameters=record)
    print("Load: Datos cargados en 'destination_table'.")


# Definición del DAG
with DAG(
    dag_id='etl_simple_con_bd_intermedia',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    extract_and_stage_task = PythonOperator(
        task_id='extract_and_stage',
        python_callable=extract_and_stage,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True
    )

    # Secuencia de ejecución: extraer y cargar en staging -> transformar en staging -> cargar a destino.
    extract_and_stage_task >> transform_task >> load_task
