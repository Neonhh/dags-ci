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

def AT12_ATSUDEBAN_ERROR(**kwargs):
    hook = PostgresHook(postgres_conn_id='at12')

    # CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.ATS_TH_AT12_ERROR (
    PAISCONSUMO VARCHAR(2),
    MUNICIPIO VARCHAR(6),
    EXTRANJERA VARCHAR(2),
    CLASETARJETA VARCHAR(2),
    TIPOTARJETA VARCHAR(2),
    CONCEPTO VARCHAR(2),
    FRANQUICIA VARCHAR(2),
    CANTIDADCONSUMOS VARCHAR(10),
    MONTOCONSUMO VARCHAR(20),
    COMISIONESPROPIAS VARCHAR(20),
    COMISIONESTERCEROS VARCHAR(20),
    RED VARCHAR(2)
    );'''
    hook.run(sql_query_deftxt)

    # TRUNCAR
    sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.ATS_TH_AT12_ERROR;'''
    hook.run(sql_query_deftxt)

    # INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO FILE_AT.ATS_TH_AT12_ERROR (
	PAISCONSUMO,
	MUNICIPIO,
	EXTRANJERA,
	CLASETARJETA,
	TIPOTARJETA,
	CONCEPTO,
	FRANQUICIA,
	CANTIDADCONSUMOS,
	MONTOCONSUMO,
	COMISIONESPROPIAS,
	COMISIONESTERCEROS,
	RED
    ) 
    SELECT
        E__ATS_TH_AT12.PAISCONSUMO AS PAISCONSUMO,
        E__ATS_TH_AT12.MUNICIPIO AS MUNICIPIO,
        E__ATS_TH_AT12.EXTRANJERA AS EXTRANJERA,
        E__ATS_TH_AT12.CLASETARJETA AS CLASETARJETA,
        E__ATS_TH_AT12.TIPOTARJETA AS TIPOTARJETA,
        E__ATS_TH_AT12.CONCEPTO AS CONCEPTO,
        E__ATS_TH_AT12.FRANQUICIA AS FRANQUICIA,
        E__ATS_TH_AT12.CANTIDADCONSUMOS AS CANTIDADCONSUMOS,
        E__ATS_TH_AT12.MONTOCONSUMO AS MONTOCONSUMO,
        E__ATS_TH_AT12.COMISIONESPROPIAS AS COMISIONESPROPIAS,
        E__ATS_TH_AT12.COMISIONESTERCEROS AS COMISIONESTERCEROS,
        E__ATS_TH_AT12.RED AS RED
    FROM ATSUDEBAN.E$_ATS_TH_AT12 AS E__ATS_TH_AT12;'''
    hook.run(sql_query_deftxt)

def ATS_TH_AT12_ERROR_TOTXT(**kwargs):
    # Conexion a la bd
    hook = PostgresHook(postgres_conn_id='at12')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

    # Recuperar las variables definidas en las tareas previas
    FileAT = get_variable('FileAT_at12')
    FechaFile = get_variable('FechaFile')
    FileName_Error = get_variable('FileName_Error')

    # Generar txt
    logger.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records('SELECT * FROM FILE_AT.ATS_TH_AT12_ERROR;')
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Definir la ruta del archivo de salida en GCS
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT12/SALIDAS/{FileName_Error}{FileAT}{FechaFile}.txt"
    
    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
    local_file_path = os.path.join(temp_dir, f"{FileName_Error}{FileAT}{FechaFile}.txt") # Ruta del archivo temporal local


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
            #mime_type='text/plain' # Opcional: especificar el tipo MIME
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

dag = DAG(dag_id='AT12_TO_FILE_ERROR',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

FileName_Error_task = PythonOperator(
    task_id='FileName_Error_task',
    python_callable=FileName_Error,
    dag=dag
)

AT12_ATSUDEBAN_ERROR_task = PythonOperator(
    task_id='AT12_ATSUDEBAN_ERROR_task',
    python_callable=AT12_ATSUDEBAN_ERROR,
    dag=dag
)

ATS_TH_AT12_ERROR_TOTXT_task = PythonOperator(
    task_id='ATS_TH_AT12_ERROR_TOTXT_task',
    python_callable=ATS_TH_AT12_ERROR_TOTXT,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
FileName_Error_task >> AT12_ATSUDEBAN_ERROR_task >> ATS_TH_AT12_ERROR_TOTXT_task
