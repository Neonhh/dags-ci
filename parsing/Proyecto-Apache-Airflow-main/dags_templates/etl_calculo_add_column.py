from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'airflow',
}

def extract(**kwargs):

    ti = kwargs['ti']
    source_hook = PostgresHook(postgres_conn_id='source_db')
    
    records = source_hook.get_records("SELECT id, producto, cantidad, CAST(precio_unitario AS double precision) AS precio_unitario FROM ventas;")
    
    ti.xcom_push(key='ventas_records', value=records)
    print(f"Extract: Se extrajeron registros de 'ventas'.", records)

def transform(**kwargs):
    
    ti = kwargs['ti']
    records = ti.xcom_pull(key='ventas_records', task_ids='extract_task')
    
    # Armamos el DataFrame
    df = pd.DataFrame(records, columns=['id', 'producto', 'cantidad', 'precio_unitario'])
    
    # Agregamos la columna nueva
    df['precio_total'] = df['cantidad'] * df['precio_unitario']
    
    # Convertimos a lista de tuplas para pasar a la siguiente tarea
    transformed_records = list(df.itertuples(index=False, name=None))
    ti.xcom_push(key='transformed_records', value=transformed_records)
    
    print("Transform: DataFrame con columna 'precio_total' agregada.")
    print(df)

def load(**kwargs):
    """
    Cargar los registros transformados en la tabla 'ventas_calculada' de la BD destination_db.
    """
    ti = kwargs['ti']
    transformed_records = ti.xcom_pull(key='transformed_records', task_ids='transform_task')
    
    if not transformed_records:
        print("Load: No hay registros para cargar.")
        return
    
    dest_hook = PostgresHook(postgres_conn_id='destination_db')
    
    # Creamos la tabla destino (si no existe)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ventas_calculada (
        id INT,
        producto VARCHAR(100),
        cantidad INT,
        precio_unitario DECIMAL(10,2),
        precio_total DECIMAL(10,2)
    );
    """
    dest_hook.run(create_table_sql)
    
    # Insertamos
    insert_sql = """
    INSERT INTO ventas_calculada (id, producto, cantidad, precio_unitario, precio_total)
    VALUES (%s, %s, %s, %s, %s);
    """
    for row in transformed_records:
        dest_hook.run(insert_sql, parameters=row)
    
    print(f"Load: registros insertados en 'ventas_calculada'.", transformed_records)

# Definición del DAG
with DAG(
    dag_id='etl_calculo_add_column',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    # Orden de ejecución
    extract_task >> transform_task >> load_task
