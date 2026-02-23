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

def AT03_ATSUDEBAN_TOFILE(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT03 (
	OFICINA VARCHAR(5),
	CODIGO_CONTABLE VARCHAR(10),
	SALDO VARCHAR(30),
	CONSECUTIVO VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT03;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT03 (
	OFICINA, 
	CODIGO_CONTABLE, 
	SALDO, 
	CONSECUTIVO
	) 
	SELECT
		OFICINA,
		CODIGO_CONTABLE,
		REPLACE(TO_CHAR(SALDO, 'FM999999999999990.00'), '.', ',') AS SALDO,
		CONSECUTIVO
	FROM ATSUDEBAN.ATS_TH_AT03_
	ORDER BY OFICINA, CODIGO_CONTABLE
	;'''
	hook.run(sql_query_deftxt)

	# Borramos tablas TEMPORALES
	#sql_query = '''DROP TABLE IF EXISTS ATSUDEBAN.ATS_TH_AT03_;'''
	#hook.run(sql_query)

def ATS_TH_AT03_TOTXT(**kwargs):
	# Conexion a la bd
	hook = PostgresHook(postgres_conn_id='repodataprd')
	gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

	# Recuperar las variables definidas en las tareas previas
	FileAT = get_variable('FileAT_at03')
	FileCodSupervisado = get_variable('FileCodSupervisado')
	FechaFile = get_variable('FechaFile')

	# Generar txt
	logger.info("Obteniendo registros de la base de datos...")
	registros = hook.get_records('SELECT * FROM ATSUDEBAN.AT03;')
	logger.info(f"Se obtuvieron {len(registros)} registros.")

	# Definir la ruta del archivo de salida en GCS
	gcs_bucket = 'airflow-dags-data'
	gcs_object_path = f"data/AT03/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFile}.txt"

	temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
	local_file_path = os.path.join(temp_dir, f"{FileAT}{FileCodSupervisado}{FechaFile}.txt") # Ruta del archivo temporal local


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

dag = DAG(dag_id='AT03_TO_FILE',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

AT03_ATSUDEBAN_TOFILE_task = PythonOperator(
	task_id='AT03_ATSUDEBAN_TOFILE_task',
	python_callable=AT03_ATSUDEBAN_TOFILE,
	dag=dag
)

ATS_TH_AT03_TOTXT_task = PythonOperator(
	task_id='ATS_TH_AT03_TOTXT_task',
	python_callable=ATS_TH_AT03_TOTXT,
	dag=dag
)

def Enviar_Email_task_callable(**kwargs):
    """Envía email usando send_email() - Convertido desde EmailOperator"""
    from airflow.utils.email import send_email
    send_email(
        to="daniel.figueroa@kreadata.com",
        subject="DAG {{ dag.dag_id }} completado",
        html_content="""
                             
		<h3>¡Hola!</h3>
		<p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at03 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>
	
        """
    )

Enviar_Email_task = PythonOperator(
    task_id="Enviar_Email_task",
    python_callable=Enviar_Email_task_callable,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT03_ATSUDEBAN_TOFILE_task >> ATS_TH_AT03_TOTXT_task >> Enviar_Email_task
