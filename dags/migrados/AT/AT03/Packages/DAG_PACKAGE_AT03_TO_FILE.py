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

def FileName(**kwargs):
    value = '#ODI_AT_SUDEBAN_DEV.FileName'
    Variable.set('FileName', serialize_value(value))

def AT03_ATSUDEBAN_TOFILE(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Create target
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS "DEV/MENSUALES/{FileName}" (OFICINA VARCHAR(5), CODIGO_CONTABLE VARCHAR(10), SALDO VARCHAR(20), CONSECUTIVO VARCHAR(1));'''
    logger.info("Accion a ejecutarse: Create target")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target, ejecutada exitosamente")

    # Truncate
    sql_query_deftxt = f'''TRUNCATE TABLE "DEV/MENSUALES/{FileName}"'''
    logger.info("Accion a ejecutarse: Truncate")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate, ejecutada exitosamente")

    # Insert new rows
    sql_query_coltxt = '''SELECT
    ATS_TH_AT03.OFICINA AS OFICINA,
    ATS_TH_AT03.CODIGO_CONTABLE AS CODIGO_CONTABLE,
    TO_CHAR(ATS_TH_AT03.SALDO, 'FM999999999999999990D00') AS SALDO,
    ATS_TH_AT03.CONSECUTIVO AS CONSECUTIVO
FROM ATS_TH_AT03
WHERE 1 = 1
ORDER BY OFICINA, CODIGO_CONTABLE;'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_coltxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "<?=snpRef.getObjectName("L", "DEV/MENSUALES/{FileName}", "ARCHIVOSUDEBAN", "", "D")?>" (OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO) VALUES (<?= (char) 58 + "OFICINA" ?>, <?= (char) 58 + "CODIGO_CONTABLE" ?>, <?= (char) 58 + "SALDO" ?>, <?= (char) 58 + "CONSECUTIVO" ?>);'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")


def GENERAR_REPORTE_TXT(**kwargs):
    # Conexion a la bd
    hook = PostgresHook(postgres_conn_id='nombre_conexion')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

    # Recuperar variables
    FileAT = get_variable('FileAT_at#')
    FileCodSupervisado = get_variable('FileCodSupervisado')
    FechaFile = get_variable('FechaFile')

    # Obtener registros
    logger.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records("SELECT * FROM tabla_final;")
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Rutas del archivo de salida en GCS y local
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT#/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFile}.txt"
    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
    local_file_path = os.path.join(temp_dir, f"{FileAT}{FileCodSupervisado}{FechaFile}.txt") # Ruta del archivo temporal local

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

dag = DAG(dag_id='AT03_TO_FILE',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

FileName_task = PythonOperator(
    task_id='FileName_task',
    python_callable=FileName,
    dag=dag
)

AT03_ATSUDEBAN_TOFILE_task = PythonOperator(
    task_id='AT03_ATSUDEBAN_TOFILE_task',
    python_callable=AT03_ATSUDEBAN_TOFILE,
    dag=dag
)

GENERAR_REPORTE_TXT_task = PythonOperator(
    task_id='GENERAR_REPORTE_TXT_task',
    python_callable=GENERAR_REPORTE_TXT,
    dag=dag
)

Enviar_Email_task = EmailOperator(
    task_id='Enviar_Email_task',
    to='colocar_correo_aqui@gmail.com',          # correo destino
    subject='DAG {{ dag.dag_id }} completado',   # asunto del correo
    html_content="""                             
        <h3>¡Hola!</h3>
        <p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at# }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>
    """,
    conn_id='email_conn',
    dag=dag
)
###### SECUENCIA DE EJECUCION ######
FileName_task >> AT03_ATSUDEBAN_TOFILE_task >> GENERAR_REPORTE_TXT_task >> Enviar_Email_task
