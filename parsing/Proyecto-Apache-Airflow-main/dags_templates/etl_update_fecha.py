from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import timedelta

default_args = {
    'owner': 'airflow',
}

def extract(**kwargs):
    """
    Extraer registros de las tablas 'personas' y 'catastros' desde la BD source_db.
    Guardar ambos resultados en XCom para la tarea transform.
    """
    ti = kwargs['ti']
    source_hook = PostgresHook(postgres_conn_id='source_db')
    
    # Lee la tabla 'personas' con columnas (id, name, catastro)
    personas_records = source_hook.get_records("SELECT * FROM personas;")
    ti.xcom_push(key='personas_records', value=personas_records)
    
    # Lee la tabla 'catastros' con columnas (catastro, fecha)
    inmueble_records = source_hook.get_records(
        "SELECT catastro, to_char(fecha, 'YYYY-MM-DD') as fecha FROM inmueble;"
    )
    ti.xcom_push(key='inmueble_records', value=inmueble_records)
    
    print(f"Extract: registros en 'personas'.", personas_records)
    print(f"Extract: registros en 'inmueble'.", inmueble_records)


def transform(**kwargs):
    
    ti = kwargs['ti']
    
    # Recuperar los registros de XCom
    personas_records = ti.xcom_pull(key='personas_records', task_ids='extract_task')
    inmueble_records = ti.xcom_pull(key='inmueble_records', task_ids='extract_task')
    
    # DataFrames
    df_personas = pd.DataFrame(personas_records, columns=['id','name','catastro'])
    df_inmueble = pd.DataFrame(inmueble_records, columns=['catastro','fecha'])
    
    # (Opcional) Realizar join
    df_joined = pd.merge(df_personas, df_inmueble, on='catastro', how='inner')
    
    # Convertimos 'fecha' a tipo datetime
    df_joined['fecha'] = pd.to_datetime(df_joined['fecha'])
    # Aumentamos 1 día
    df_joined['fecha'] = df_joined['fecha'] + timedelta(days=1)
    # Volvemos a convertirlo a string para XCom
    df_joined['fecha'] = df_joined['fecha'].dt.strftime('%Y-%m-%d')
    
    # Convertimos a lista de tuplas
    joined_records = list(df_joined.itertuples(index=False, name=None))
    ti.xcom_push(key='joined_records', value=joined_records)
    print("Transform: DataFrame joined\n", df_joined)


def load(**kwargs):
    
    ti = kwargs['ti']
    joined_records = ti.xcom_pull(key='joined_records', task_ids='transform_task')
    
    if not joined_records:
        print("Load: No hay registros para cargar.")
        return
    
    # Conexión BD destino
    dest_hook = PostgresHook(postgres_conn_id='destination_db')
    
    # Tabla destino
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS destino_update_fechas (
        id INT,
        name VARCHAR(100),
        catastro VARCHAR(50),
        fecha DATE
    );
    """

    dest_hook.run(create_table_sql)
    print("Load: Tabla verificada/creada.")
    
    # INSERT
    insert_sql = """
    INSERT INTO destino_update_fechas (id, name, catastro, fecha)
    VALUES (%s, %s, %s, %s);
    """
    
    # Inserto cada registro
    for record in joined_records:
        dest_hook.run(insert_sql, parameters=record)
    
    print(f"Load: registros cargados.", joined_records)


# Definición del DAG
with DAG(
    dag_id='etl_update_fecha',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
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

    extract_task >> transform_task >> load_task
