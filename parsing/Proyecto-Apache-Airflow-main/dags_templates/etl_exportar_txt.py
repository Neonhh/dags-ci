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
    
    sql = """
    SELECT 
        id, 
        producto, 
        cantidad, 
        CAST(precio_unitario AS double precision) AS precio_unitario 
    FROM ventas;
    """
    records = source_hook.get_records(sql)
    ti.xcom_push(key='ventas_records', value=records)

def transform(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key='ventas_records', task_ids='extract_task')
    
    df = pd.DataFrame(records, columns=['id','producto','cantidad','precio_unitario'])
    # Agregar la columna calculada
    df['precio_total'] = df['cantidad'] * df['precio_unitario']
    
    # Convertir a lista de tuplas
    transformed_records = list(df.itertuples(index=False, name=None))
    ti.xcom_push(key='transformed_records', value=transformed_records)

def load(**kwargs):
    ti = kwargs['ti']
    transformed_records = ti.xcom_pull(key='transformed_records', task_ids='transform_task')
    if not transformed_records:
        print("No hay registros para cargar en 'ventas_calculada'.")
        return
    
    dest_hook = PostgresHook(postgres_conn_id='destination_db')
    
    dest_hook.run("DROP TABLE IF EXISTS ventas_calculada;")

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
    
    insert_sql = """
    INSERT INTO ventas_calculada (id, producto, cantidad, precio_unitario, precio_total)
    VALUES (%s, %s, %s, %s, %s);
    """
    for row in transformed_records:
        dest_hook.run(insert_sql, parameters=row)

def export_to_txt(**kwargs):
    
    # Conectarse a la BD destino
    dest_hook = PostgresHook(postgres_conn_id='destination_db')
    
    # Obtener registros
    records = dest_hook.get_records("SELECT * FROM ventas_calculada;")
    
    # Convertir a DataFrame para la exportacion
    df = pd.DataFrame(records, columns=['id','producto','cantidad','precio_unitario','precio_total'])
    

    # Guardar como CSV
    # Exportar a un archivo .txt en la carpeta indicada
    output_path = "/home/apacheuser/airflow/reports/ventas_calculada.txt"  
    df.to_csv(output_path, index=False, sep=';')
    
    print(f"Archivo TXT exportado en: {output_path}")

with DAG(
    dag_id='etl_exportar_txt',
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
    
    export_to_txt_task = PythonOperator(
        task_id='export_to_txt_task',
        python_callable=export_to_txt
    )

    # Orden de ejecución
    extract_task >> transform_task >> load_task >> export_to_txt_task
