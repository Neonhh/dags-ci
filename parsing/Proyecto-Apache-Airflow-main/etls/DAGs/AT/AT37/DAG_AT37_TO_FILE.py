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
from airflow.operators.email import EmailOperator
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

def AT37_ATSUDEBAN_TOFILE(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

    # creamos tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.AT37_FILE_ (
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
    OFICINA VARCHAR(5),
    UBICACIONIP VARCHAR(1)
    );'''
    hook.run(sql_query_deftxt)

    # truncate
    sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.AT37_FILE_;'''
    hook.run(sql_query_deftxt)

    # insertar en destino
    sql_query_deftxt = '''INSERT INTO FILE_AT.AT37_FILE_ (
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
	OFICINA,
	UBICACIONIP
    ) 
    SELECT
        CASE 
            WHEN AT37_BC_TOTAL_FIN.TIPOTRANSFERENCIA = 1 THEN 6 
            WHEN AT37_BC_TOTAL_FIN.TIPOTRANSFERENCIA = 2 THEN 7 
            ELSE AT37_BC_TOTAL_FIN.TIPOTRANSFERENCIA 
        END AS TIPOTRANSFERENCIA,
        AT37_BC_TOTAL_FIN.CLASIFICACION AS CLASIFICACION,
        AT37_BC_TOTAL_FIN.TIPOCLIENTE AS TIPOCLIENTE,
        AT37_BC_TOTAL_FIN.IDENTIFICACIONCLIENTE AS IDENTIFICACIONCLIENTE,
        REPLACE(AT37_BC_TOTAL_FIN.NOMBRECLIENTE, '~', '') AS NOMBRECLIENTE,
        AT37_BC_TOTAL_FIN.TIPOINSTRUMENTO AS TIPOINSTRUMENTO,
        AT37_BC_TOTAL_FIN.CUENTACLIENTE AS CUENTACLIENTE,
        AT37_BC_TOTAL_FIN.MONEDA AS MONEDA,
        REPLACE(TO_CHAR(AT37_BC_TOTAL_FIN.MONTO, 'FM9999999999999.00'), '.', ',') AS MONTO,
        AT37_BC_TOTAL_FIN.FECHA AS FECHA,
        AT37_BC_TOTAL_FIN.REFERENCIA AS REFERENCIA,
        AT37_BC_TOTAL_FIN.EMPRESAFORMACION AS EMPRESAFORMACION,
        AT33_BC_TOTAL_FIN.MOTIVO AS MOTIVO,
        AT37_BC_TOTAL_FIN.DIRECCIONIP AS DIRECCIONIP,
        AT37_BC_TOTAL_FIN.TIPOCONTRAPARTE AS TIPOCONTRAPARTE,
        AT37_BC_TOTAL_FIN.IDENTIFICACIONCONTRAPARTE AS IDENTIFICACIONCONTRAPARTE,
        AT37_BC_TOTAL_FIN.NOMBRECONTRAPARTE AS NOMBRECONTRAPARTE,
        AT37_BC_TOTAL_FIN.TIPOINSTRUMENTOCONTRAPARTE AS TIPOINSTRUMENTOCONTRAPARTE,
        AT37_BC_TOTAL_FIN.CUENTACONTRAPARTE AS CUENTACONTRAPARTE,
        CONCAT('0', AT37_BC_TOTAL_FIN.OFICINA) AS OFICINA,
        AT37_BC_TOTAL_FIN.UBICACION_IP AS UBICACIONIP
    FROM atsudeban.at37_bc_total_fin AS AT37_BC_TOTAL_FIN;'''
    hook.run(sql_query_deftxt)

def AT37_FILE_TOTXT(**kwargs):
    # Conexion a la bd
    hook = PostgresHook(postgres_conn_id='at37')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

    # Recuperar las variables definidas en las tareas previas
    FileAT = get_variable('FileAT_at37')
    FileCodSupervisado = get_variable('FileCodSupervisado')
    FechaFileS = get_variable('FechaFileS')

    # Generar txt
    logger.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records('SELECT * FROM FILE_AT.AT37_FILE_;')
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Definir la ruta del archivo de salida en GCS
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT37/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFileS}.txt"
    
    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
    local_file_path = os.path.join(temp_dir, f"{FileAT}{FileCodSupervisado}{FechaFileS}.txt") # Ruta del archivo temporal local


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

dag = DAG(dag_id='AT37_TO_FILE',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

AT37_ATSUDEBAN_TOFILE_task = PythonOperator(
    task_id='AT37_ATSUDEBAN_TOFILE_task',
    python_callable=AT37_ATSUDEBAN_TOFILE,
    dag=dag
)

AT37_FILE_TOTXT_task = PythonOperator(
    task_id='AT37_FILE_TOTXT_task',
    python_callable=AT37_FILE_TOTXT,
    dag=dag
)

Enviar_Email_task = EmailOperator(
    task_id='Enviar_Email_task',
    to='airflowprueba2025@gmail.com',            # correo destino
    subject='DAG {{ dag.dag_id }} completado',   # asunto del correo
    html_content="""                             
        <h3>¡Hola!</h3>
        <p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at37 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFileS }}.txt</p>
    """,
    conn_id="email_conn",
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_ATSUDEBAN_TOFILE_task >> AT37_FILE_TOTXT_task >> Enviar_Email_task
