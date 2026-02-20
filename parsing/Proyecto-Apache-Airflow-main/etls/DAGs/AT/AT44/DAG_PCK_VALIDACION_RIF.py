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
import time
from airflow.operators.email import EmailOperator
import os
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)

### FUNCIONES DE CADA TAREA ###

def IT_CLIENTES_SIN_RIF2(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.tmp_clientes_sinrif (
	en_ente NUMERIC(10),
	en_oficina VARCHAR(100),
	en_ced_ruc VARCHAR(40),
	en_fecha_crea DATE,
	en_fecha_mod DATE,
	edad VARCHAR(50)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.TMP_CLIENTES_SINRIF;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.tmp_clientes_sinrif (
	en_ente,
	en_oficina,
	en_ced_ruc,
	en_fecha_crea,
	en_fecha_mod,
	edad
	)
	SELECT DISTINCT
		CL_ENTE.EN_ENTE,
		CL_ENTE.EN_OFICINA,
		CL_ENTE.EN_CED_RUC,
		CL_ENTE.EN_FECHA_CREA,
		CL_ENTE.EN_FECHA_MOD,
		TRIM(CAST(EXTRACT(YEAR FROM AGE(current_date, cl_ente.p_fecha_nac)) AS TEXT)) AS EDAD
	FROM ODS.CL_ENTE AS CL_ENTE INNER JOIN ODS.CC_CTACTE AS CC_CTACTE ON TRIM(CL_ENTE.EN_CED_RUC) = TRIM(CC_CTACTE.CC_CED_RUC)
	WHERE (cl_ente.en_nit IS NULL OR cl_ente.en_nit = ' ') AND CL_ENTE.EN_SUBTIPO = 'P'
		AND CL_ENTE.EN_OFICINA NOT IN ('169', '179')
		AND CL_ENTE.EN_OFICINA::INTEGER < 700
		AND CC_CTACTE.CC_ESTADO = 'A'
	ORDER BY en_oficina;'''
	hook.run(sql_query_deftxt)

def IT_CLIENTES_TO_FILE_4(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.CLIENTESME_SIN_RIF (
	CLIENTE VARCHAR(50), 
	OFICINA VARCHAR(50), 
	CEDULA VARCHAR(50), 
	CUENTA VARCHAR(50), 
	OBSERVACION VARCHAR(100)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.CLIENTESME_SIN_RIF;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO FILE_AT.CLIENTESME_SIN_RIF (
	CLIENTE, 
	OFICINA, 
	CEDULA, 
	CUENTA, 
	OBSERVACION
	) 
	SELECT
		AT44_CC_CTACTE_TMP.CLIENTE AS CLIENTE,
		SUBSTRING(TRIM(AT44_CC_CTACTE_TMP.CC_CTA_BANCO) FROM 6 FOR 3) AS OFICINA,
		CONCAT(AT44_CC_CTACTE_TMP.TIPOCLIENTE, AT44_CC_CTACTE_TMP.IDENTIFICACION) AS CEDULA,
		TRIM(AT44_CC_CTACTE_TMP.CC_CTA_BANCO) AS CUENTA,
		'REGISTRO AT44 SIN RIF' AS OBSERVACION
	FROM AT_STG.AT44_CC_CTACTE_TMP
	WHERE
		(AT44_CC_CTACTE_TMP.IDENTIFACIONRIF IS NULL OR AT44_CC_CTACTE_TMP.IDENTIFACIONRIF = ' ' OR AT44_CC_CTACTE_TMP.IDENTIFACIONRIF = '')
	ORDER BY OFICINA;'''
	hook.run(sql_query_deftxt)

def IT_CLIENTES_TO_FILE_4_TXT(**kwargs):
	# Conexion a la bd
	hook = PostgresHook(postgres_conn_id='at44')
	gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

	# Generar txt
	logger.info("Obteniendo registros de la base de datos...")
	registros = hook.get_records('SELECT * FROM FILE_AT.CLIENTESME_SIN_RIF;')
	logger.info(f"Se obtuvieron {len(registros)} registros.")
	
	# Definir la ruta del archivo de salida en GCS
	gcs_bucket = 'airflow-dags-data'
	gcs_object_path = f"data/AT44/SALIDAS/CLIENTESME_SIN_RIF.txt"
	
	temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
	local_file_path = os.path.join(temp_dir, f"CLIENTESME_SIN_RIF.txt") # Ruta del archivo temporal local


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

def IT_CLIENTES_TO_FILE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.CLIENTES_SIN_RIF (
	CLIENTE VARCHAR(50),
	OFICINA VARCHAR(50),
	CEDULA VARCHAR(50),
	FECHACREACION VARCHAR(50),
	FECHAMODIF VARCHAR(50),
	EDAD VARCHAR(10),
	OBSERVACION VARCHAR(100)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.CLIENTES_SIN_RIF;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO   
	sql_query_deftxt = '''INSERT INTO FILE_AT.CLIENTES_SIN_RIF (
	CLIENTE, 
	OFICINA, 
	CEDULA, 
	FECHACREACION, 
	FECHAMODIF, 
	EDAD, 
	OBSERVACION
	) 
	SELECT
		TMP_CLIENTES_SINRIF.EN_ENTE::VARCHAR AS CLIENTE,
		TMP_CLIENTES_SINRIF.EN_OFICINA AS OFICINA,
		TMP_CLIENTES_SINRIF.EN_CED_RUC AS CEDULA,
		TMP_CLIENTES_SINRIF.EN_FECHA_CREA AS FECHACREACION,
		TMP_CLIENTES_SINRIF.EN_FECHA_MOD AS FECHAMODIF,
		TMP_CLIENTES_SINRIF.EDAD AS EDAD,
		'MAYOR DE EDAD SIN RIF - CON CTACTE ACTIVA' AS OBSERVACION
	FROM AT_STG.TMP_CLIENTES_SINRIF AS TMP_CLIENTES_SINRIF
	WHERE CAST(TMP_CLIENTES_SINRIF.EDAD AS INTEGER) >= 18
	ORDER BY OFICINA;
	'''
	hook.run(sql_query_deftxt)

def IT_CLIENTES_TO_FILE_2(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# insertar en destino
	sql_query_deftxt = '''INSERT INTO FILE_AT.CLIENTES_SIN_RIF (
	CLIENTE, 
	OFICINA, 
	CEDULA, 
	FECHACREACION, 
	FECHAMODIF, 
	EDAD, 
	OBSERVACION
	) 
	SELECT
		TMP_CLIENTES_SINRIF.EN_ENTE AS CLIENTE,
		TMP_CLIENTES_SINRIF.EN_OFICINA AS OFICINA,
		TMP_CLIENTES_SINRIF.EN_CED_RUC AS CEDULA,
		TMP_CLIENTES_SINRIF.EN_FECHA_CREA AS FECHACREACION,
		TMP_CLIENTES_SINRIF.EN_FECHA_MOD AS FECHAMODIF,
		TMP_CLIENTES_SINRIF.EDAD AS EDAD,
		'MAYOR DE EDAD NO INICIA CON V O E' AS OBSERVACION
	FROM AT_STG.TMP_CLIENTES_SINRIF AS TMP_CLIENTES_SINRIF
	WHERE
		CAST(TMP_CLIENTES_SINRIF.EDAD AS INTEGER) >= 18
		AND (TMP_CLIENTES_SINRIF.EN_CED_RUC NOT LIKE 'V%')
		AND (TMP_CLIENTES_SINRIF.EN_CED_RUC NOT LIKE 'E%');'''
	hook.run(sql_query_deftxt)

def IT_CLIENTES_TO_FILE_3(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO FILE_AT.CLIENTES_SIN_RIF (
	CLIENTE, 
	OFICINA, 
	CEDULA, 
	FECHACREACION, 
	FECHAMODIF, 
	EDAD, 
	OBSERVACION
	) 
	SELECT
		TMP_CLIENTES_SINRIF.EN_ENTE AS CLIENTE,
		TMP_CLIENTES_SINRIF.EN_OFICINA AS OFICINA,
		TMP_CLIENTES_SINRIF.EN_CED_RUC AS CEDULA,
		TMP_CLIENTES_SINRIF.EN_FECHA_CREA AS FECHACREACION,
		TMP_CLIENTES_SINRIF.EN_FECHA_MOD AS FECHAMODIF,
		TMP_CLIENTES_SINRIF.EDAD AS EDAD,
		'MENOR DE EDAD NO INICIA CON M - CON CTACTE ACTIVA' AS OBSERVACION
	FROM AT_STG.TMP_CLIENTES_SINRIF AS TMP_CLIENTES_SINRIF
	WHERE
		CAST(TMP_CLIENTES_SINRIF.EDAD AS INTEGER) < 18
		AND TMP_CLIENTES_SINRIF.EN_CED_RUC NOT LIKE 'M%'
	ORDER BY OFICINA;'''
	hook.run(sql_query_deftxt)

def IT_CLIENTES_TO_FILE_3_TXT(**kwargs):
	# Conexion a la bd
	hook = PostgresHook(postgres_conn_id='at44')
	gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

	# Generar txt
	logger.info("Obteniendo registros de la base de datos...")
	registros = hook.get_records('SELECT * FROM FILE_AT.CLIENTES_SIN_RIF;')
	logger.info(f"Se obtuvieron {len(registros)} registros.")

	# Definir la ruta del archivo de salida en GCS
	gcs_bucket = 'airflow-dags-data'
	gcs_object_path = f"data/AT44/SALIDAS/CLIENTES_SIN_RIF.txt"
	
	temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
	local_file_path = os.path.join(temp_dir, f"CLIENTES_SIN_RIF.txt") # Ruta del archivo temporal local


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

dag = DAG(dag_id='PCK_VALIDACION_RIF',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

IT_CLIENTES_SIN_RIF2_task = PythonOperator(
	task_id='IT_CLIENTES_SIN_RIF2_task',
	python_callable=IT_CLIENTES_SIN_RIF2,
	dag=dag
)

IT_CLIENTES_TO_FILE_4_task = PythonOperator(
	task_id='IT_CLIENTES_TO_FILE_4_task',
	python_callable=IT_CLIENTES_TO_FILE_4,
	dag=dag
)

IT_CLIENTES_TO_FILE_4_TXT_task = PythonOperator(
	task_id='IT_CLIENTES_TO_FILE_4_TXT_task',
	python_callable=IT_CLIENTES_TO_FILE_4_TXT,
	dag=dag
)

IT_CLIENTES_TO_FILE_task = PythonOperator(
	task_id='IT_CLIENTES_TO_FILE_task',
	python_callable=IT_CLIENTES_TO_FILE,
	dag=dag
)

IT_CLIENTES_TO_FILE_2_task = PythonOperator(
	task_id='IT_CLIENTES_TO_FILE_2_task',
	python_callable=IT_CLIENTES_TO_FILE_2,
	dag=dag
)

IT_CLIENTES_TO_FILE_3_task = PythonOperator(
	task_id='IT_CLIENTES_TO_FILE_3_task',
	python_callable=IT_CLIENTES_TO_FILE_3,
	dag=dag
)

IT_CLIENTES_TO_FILE_3_TXT_task = PythonOperator(
	task_id='IT_CLIENTES_TO_FILE_3_TXT_task',
	python_callable=IT_CLIENTES_TO_FILE_3_TXT,
	dag=dag
)

Enviar_Email_task = EmailOperator(
	task_id='Enviar_Email_task',
	to='colocar_correo_aqui@gmail.com',            # correo destino
	subject='DAG {{ dag.dag_id }} completado',   # asunto del correo
	html_content="""                             
		<h3>¡Hola!</h3>
		<p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: CLIENTES_SIN_RIF.txt</p>
	""",
	conn_id="email_conn",
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_CLIENTES_SIN_RIF2_task >> IT_CLIENTES_TO_FILE_4_task >> IT_CLIENTES_TO_FILE_4_TXT_task >> IT_CLIENTES_TO_FILE_task >> IT_CLIENTES_TO_FILE_2_task >> IT_CLIENTES_TO_FILE_3_task >> IT_CLIENTES_TO_FILE_3_TXT_task >> Enviar_Email_task
