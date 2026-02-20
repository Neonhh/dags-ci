from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'airflow',
}

def extract(**kwargs):
    """
    Usa PostgresHook para conectarse a la BD de origen y extraer los registros
    de 'source_table'. Los registros se envían a XCom con la clave 'my_records'.
    """
    # Conexión a la BD de origen definida en Airflow (source_db)
    hook = PostgresHook(postgres_conn_id='source_db')
    # Extrae los registros con la consulta SQL
    records = hook.get_records("SELECT * FROM source_table")
    # Envía los registros a XCom para la siguiente tarea
    kwargs['ti'].xcom_push(key='my_records', value=records)
    print("Extract: Registros extraídos:", records)

def transform(**kwargs):
    """
    Recupera los registros extraídos desde XCom, los carga en un DataFrame de pandas,
    transforma la columna 'name' a mayúsculas y envía los registros transformados a XCom.
    """
    ti = kwargs['ti']
    # Recupera los registros extraídos por la tarea extract
    records = ti.xcom_pull(key='my_records', task_ids='extract_task')
    
    # Define los nombres de columnas. Ajusta según la estructura de source_table.
    columns = ['id', 'name', 'age', 'email']
    
    # Crea un DataFrame a partir de los registros
    df = pd.DataFrame(records, columns=columns)
    
    # Aplica la transformación: convierte la columna 'name' a mayúsculas
    df['name'] = df['name'].str.upper()
    
    # Convierte el DataFrame a una lista de tuplas para pasarla mediante XCom
    transformed_records = list(df.itertuples(index=False, name=None))
    ti.xcom_push(key='transformed_records', value=transformed_records)
    print("Transform: Registros transformados:", transformed_records)

def load(**kwargs):
    """
    Recupera los registros transformados desde XCom, crea la tabla 'destination_table'
    si no existe, y carga los registros en la BD destino.
    """
    ti = kwargs['ti']
    # Recupera los registros transformados desde la tarea transform
    transformed_records = ti.xcom_pull(key='transformed_records', task_ids='transform_task')
    
    # Conexión a la BD destino definida en Airflow (destination_db)
    dest_hook = PostgresHook(postgres_conn_id='destination_db')
    
    # Crea la tabla destination_table si no existe.
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS destination_table (
            id INT,
            name VARCHAR(100) NOT NULL,
            age INT,
            email VARCHAR(100)
        );
    """
    dest_hook.run(create_table_sql)
    print("Load: Tabla 'destination_table' verificada/creada.")
    
    # Prepara la sentencia INSERT. Asegúrate de que la cantidad de columnas coincida.
    insert_sql = """
        INSERT INTO destination_table (id, name, age, email)
        VALUES (%s, %s, %s, %s);
    """
    
    # Inserta cada registro transformado en la tabla destino
    for record in transformed_records:
        dest_hook.run(insert_sql, parameters=record)
    
    print("Load: Registros cargados en destination_table.")

# Definición del DAG
with DAG(
    dag_id='etl_simple',
    default_args=default_args,
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True
    )

    # Definir el orden de ejecución
    extract_task >> transform_task >> load_task

