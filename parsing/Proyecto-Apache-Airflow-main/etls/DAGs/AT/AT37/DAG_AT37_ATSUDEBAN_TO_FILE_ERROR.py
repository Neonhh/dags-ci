from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
import time
import os
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)

def serialize_value(value):
    """
    Helper para serializar valores de PostgreSQL a JSON.
    Convierte Decimal, date, datetime a string para preservar precisión.
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
    
    Para conversiones específicas:
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

def AT37_ATSUDEBAN_TOFILE_ERROR_(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

    # creamos tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.AT37_FILE_ERROR_ (
	TIPOTRANSFERENCIA VARCHAR(1),
	CLASIFICACION VARCHAR(3),
	TIPOCLIENTE VARCHAR(1),
	IDENTIFICACIONCLIENTE VARCHAR(15),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO VARCHAR(5),
	CUENTACLIENTE VARCHAR(20),
	MONEDA VARCHAR(3),
	MONTO VARCHAR(15),
	FECHA VARCHAR(8),
	REFERENCIA VARCHAR(30),
	EMPRESAFORMACION VARCHAR(3),
	MOTIVO VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA VARCHAR(5)
    );'''
    hook.run(sql_query_deftxt)

    # TRUNCAR
    sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.AT37_FILE_ERROR_;'''
    hook.run(sql_query_deftxt)

    # insertar en destino
    sql_query_deftxt = '''INSERT INTO FILE_AT.AT37_FILE_ERROR_ (
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA
    ) 
    SELECT
        E__AT37_ATSS.TIPOTRANSFERENCIA AS TIPOTRANSFERENCIA,
        E__AT37_ATSS.CLASIFICACION AS CLASIFICACION,
        E__AT37_ATSS.TIPOCLIENTE AS TIPOCLIENTE,
        E__AT37_ATSS.IDENTIFICACIONCLIENTE AS IDENTIFICACIONCLIENTE,
        E__AT37_ATSS.NOMBRECLIENTE AS NOMBRECLIENTE,
        E__AT37_ATSS.TIPOINSTRUMENTO AS TIPOINSTRUMENTO,
        E__AT37_ATSS.CUENTACLIENTE AS CUENTACLIENTE,
        E__AT37_ATSS.MONEDA AS MONEDA,
        E__AT37_ATSS.MONTO AS MONTO,
        E__AT37_ATSS.FECHA AS FECHA,
        E__AT37_ATSS.REFERENCIA AS REFERENCIA,
        E__AT37_ATSS.EMPRESAFORMACION AS EMPRESAFORMACION,
        E__AT37_ATSS.MOTIVO AS MOTIVO,
        E__AT37_ATSS.DIRECCIONIP AS DIRECCIONIP,
        E__AT37_ATSS.TIPOCONTRAPARTE AS TIPOCONTRAPARTE,
        E__AT37_ATSS.IDENTIFICACIONCONTRAPARTE AS IDENTIFICACIONCONTRAPARTE,
        E__AT37_ATSS.NOMBRECONTRAPARTE AS NOMBRECONTRAPARTE,
        E__AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE AS TIPOINSTRUMENTOCONTRAPARTE,
        E__AT37_ATSS.CUENTACONTRAPARTE AS CUENTACONTRAPARTE,
        E__AT37_ATSS.OFICINA AS OFICINA
    FROM ATSUDEBAN.E$_AT37_ATSS AS E__AT37_ATSS;'''
    hook.run(sql_query_deftxt)

def AT37_FILE_ERROR_TOTXT(**kwargs):
    # Conexion a la bd
    hook = PostgresHook(postgres_conn_id='at37')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

    # Recuperar las variables definidas en las tareas previas
    FileAT = get_variable('FileAT_at37')
    FechaFileS = get_variable('FechaFileS')
    FileName_Error = get_variable('FileName_Error')

    # Generar txt
    logger.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records('SELECT * FROM FILE_AT.AT37_FILE_ERROR_;')
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Definir la ruta del archivo de salida en GCS
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT37/SALIDAS/{FileName_Error}{FileAT}{FechaFileS}.txt"
    
    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
    local_file_path = os.path.join(temp_dir, f"{FileName_Error}{FileAT}{FechaFileS}.txt") # Ruta del archivo temporal local


    try:
        logger.info(f"Escribiendo datos a archivo temporal local: {local_file_path}")
        # Escribir los registros en el archivo de texto temporal local
        with open(local_file_path, 'w', encoding='utf-8') as f:
            for row in registros:
                # Convertimos cada fila (tupla) a una cadena separada por tildes y aseguramos que los valores None se traten como cadenas vaci­as
                linea = "~".join(str(valor) if valor is not None else "" for valor in row)
                f.write(linea + "\n")
        
        logger.info(f"Archivo temporal local generado correctamente. Subiendo a GCS: gs://{gcs_bucket}/{gcs_object_path}")
        
        # Subir el archivo temporal local a GCS
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=gcs_object_path,
            filename=local_file_path,
        )
        logger.info(f"Archivo generado y subido a GCS: gs://{gcs_bucket}/{gcs_object_path}")

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

dag = DAG(dag_id='AT37_ATSUDEBAN_TOFILERROR',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

FileName_Error_task = PythonOperator(
    task_id='FileName_Error_task',
    python_callable=FileName_Error,
    dag=dag
)

AT37_ATSUDEBAN_TOFILE_ERROR__task = PythonOperator(
    task_id='AT37_ATSUDEBAN_TOFILE_ERROR__task',
    python_callable=AT37_ATSUDEBAN_TOFILE_ERROR_,
    dag=dag
)

AT37_FILE_ERROR_TOTXT_task = PythonOperator(
    task_id='AT37_FILE_ERROR_TOTXT_task',
    python_callable=AT37_FILE_ERROR_TOTXT,
    dag=dag
)
###### SECUENCIA DE EJECUCION ######
FileName_Error_task >> AT37_ATSUDEBAN_TOFILE_ERROR__task >> AT37_FILE_ERROR_TOTXT_task
