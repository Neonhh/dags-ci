from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import time
import tempfile
import os
import logging
from decimal import Decimal
import json

logger = logging.getLogger(__name__)

def serialize_value(value):
    """
    Helper para serializar valores de PostgreSQL a JSON.
    Convierte Decimal, date, datetime a string para preservar precision.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value  # Ya es string
    if isinstance(value, datetime):
        return value.isoformat()  # Formato ISO 8601
    if isinstance(value, (int, float, Decimal)):
        return str(value)  # Tipos numericos
    return json.dumps(value)  # Otros tipos

def get_variable(key, default_var=""):
    """
    Helper para obtener variables de Airflow y deserializarlas.
    Retorna el valor como string (compatible con SQL queries).
    
    Para conversiones especificas:
    - int: int(get_variable('key'))
    - Decimal: Decimal(get_variable('key'))
    - datetime: datetime.fromisoformat(get_variable('key'))
    """
    raw = Variable.get(key, default_var=default_var)
    try:
        return json.loads(raw)
    except Exception:
        return raw

### FUNCIONES DE CADA TAREA ###

def FileName_Error(**kwargs):
    value = 'ErrorValid'
    Variable.set('FileName_Error', serialize_value(value))

def AT03_ATSUDEBAN_ERROR(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Truncate
    sql_query_deftxt = f'''TRUNCATE TABLE "DEV/MENSUALES/{FileName_Error}"'''
    logger.info("Accion a ejecutarse: Truncate") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate, ejecutada exitosamente") 

    # Generate header
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS header (
    ODI_ORIGIN VARCHAR(100),
    ODI_ERR_MESS VARCHAR(255),
    ODI_CHECK_DATE VARCHAR(15),
    ODI_CONS_NAME VARCHAR(130),
    OFICINA VARCHAR(5),
    CODIGO_CONTABLE VARCHAR(10),
    SALDO VARCHAR(20),
    CONSECUTIVO VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Generate header") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Generate header, ejecutada exitosamente") 

    # Insert new rows
    sql_query_coltxt = f'''SELECT
    E__ATS_TH_AT03.ODI_ORIGIN AS ODI_ORIGIN,
    E__ATS_TH_AT03.ODI_ERR_MESS AS ODI_ERR_MESS,
    E__ATS_TH_AT03.ODI_CHECK_DATE AS ODI_CHECK_DATE,
    E__ATS_TH_AT03.ODI_CONS_NAME AS ODI_CONS_NAME,
    E__ATS_TH_AT03.OFICINA AS OFICINA,
    E__ATS_TH_AT03.CODIGO_CONTABLE AS CODIGO_CONTABLE,
    E__ATS_TH_AT03.SALDO AS SALDO,
    E__ATS_TH_AT03.CONSECUTIVO AS CONSECUTIVO
FROM
    E$_ATS_TH_AT03 AS E__ATS_TH_AT03
WHERE
    1 = 1;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "DEV/MENSUALES/{FileName_Error}" (ODI_ORIGIN, ODI_ERR_MESS, ODI_CHECK_DATE, ODI_CONS_NAME, OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO) VALUES ((char) 58 + 'ODI_ORIGIN', (char) 58 + 'ODI_ERR_MESS', (char) 58 + 'ODI_CHECK_DATE', (char) 58 + 'ODI_CONS_NAME', (char) 58 + 'OFICINA', (char) 58 + 'CODIGO_CONTABLE', (char) 58 + 'SALDO', (char) 58 + 'CONSECUTIVO');'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def GENERAR_REPORTE_TXT_ERROR(**kwargs):
    # Conexion a la bd 
    hook = PostgresHook(postgres_conn_id='nombre_conexion')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

    # Recuperar variables
    FileAT = get_variable('FileAT_at#')
    FechaFile = get_variable('FechaFile')
    FileName_Error = get_variable('FileName_Error')

    # Obtener registros
    logger.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records("SELECT * FROM tabla_final;")
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Rutas del archivo de salida en GCS y local
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT#/SALIDAS/{FileName_Error}{FileAT}{FechaFile}.txt"
    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
    local_file_path = os.path.join(temp_dir, f"{FileName_Error}{FileAT}{FechaFile}.txt") # Ruta del archivo temporal local

    try:
        logger.info(f"Escribiendo datos en archivo temporal local: {local_file_path}")
        # Escribir los registros en el archivo de texto temporal local
        with open(local_file_path, 'w', encoding='utf-8') as f:
            for row in registros:
                linea = "~".join(str(v) if v is not None else "" for v in row)
                f.write(linea + "\n")

        logger.info(f"Archivo temporal local generado correctamente. Subiendo a GCS: gs://{gcs_bucket}/{gcs_object_path}")
        # Subir el archivo temporal local a GCS
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=gcs_object_path,
            filename=local_file_path,
        )
        logger.info("Archivo subido correctamente.")

    except Exception as e:
        logger.error(f"Error durante la generacion o subida del archivo: {str(e)}")
        import traceback
        logger.error("Traceback completo:\n" + traceback.format_exc())
        raise

    finally:
        # Limpieza: Asegurarse de eliminar el archivo temporal y el directorio
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            logger.info(f"Archivo temporal eliminado: {local_file_path}")
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
            logger.info(f"Directorio temporal eliminado: {temp_dir}")

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT03_TO_FILE_ERROR',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

FileName_Error_task = PythonOperator(
    task_id='FileName_Error_task',
    python_callable=FileName_Error,
    dag=dag
)

AT03_ATSUDEBAN_ERROR_task = PythonOperator(
    task_id='AT03_ATSUDEBAN_ERROR_task',
    python_callable=AT03_ATSUDEBAN_ERROR,
    dag=dag
)

GENERAR_REPORTE_TXT_ERROR_task = PythonOperator(
    task_id='GENERAR_REPORTE_TXT_ERROR_task',
    python_callable=GENERAR_REPORTE_TXT_ERROR,
    dag=dag
)
###### SECUENCIA DE EJECUCION ######
FileName_Error_task >> AT03_ATSUDEBAN_ERROR_task >> GENERAR_REPORTE_TXT_ERROR_task
