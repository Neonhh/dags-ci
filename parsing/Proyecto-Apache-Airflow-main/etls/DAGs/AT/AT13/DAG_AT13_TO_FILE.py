from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
import time
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
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

def AT13_ATSUDEBAN_TOFILE(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT13 (
	RECLAMO VARCHAR(20),
	OFICINA VARCHAR(10),
	TIPOCLIENTE VARCHAR(50),
	IDCLIENTE VARCHAR(50),
	NOMBRECLIENTE VARCHAR(255),
	FECHARECLAMO VARCHAR(8),
	TIPOINSTRUMENTO VARCHAR(8),
	TIPORECLAMO VARCHAR(150),
	MONTORECLAMO NUMERIC(15,2),
	CODIGOCUENTA VARCHAR(24),
	CREDITOAFECTADO VARCHAR(20),
	TARJETAAFECTADA VARCHAR(24),
	ENTESUPERVISADO VARCHAR(10),
	ESTADORECLAMO VARCHAR(50),
	FECHASOLUCION VARCHAR(8),
	MONTOREINTEGRO NUMERIC(15,2),
	FECHAREINTEGRO VARCHAR(8),
	FECHANOTIFICACION VARCHAR(8),
	CODIGOENTE VARCHAR(20),
	IDPOS VARCHAR(30),
	SISTEMA VARCHAR(50),
	OFICINARECLAMO VARCHAR(50),
	TIPOOPERACION VARCHAR(255),
	CANAL VARCHAR(255),
	FRANQUICIA VARCHAR(255),
	RED VARCHAR(255),
	TIPOCLIENTEDESTINO VARCHAR(50),
	IDCLIENTEDESTINO VARCHAR(50),
	NOMBRECLIENTEDESTINO VARCHAR(50),
	NROCUENTADESTINO NUMERIC(50),
	CODIGOCUENTADESTINO VARCHAR(20),
	MONTODESTINO NUMERIC(15,2)
	);'''
	hook.run(sql_query_deftxt)
	
	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT13;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT13 (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO
	)
	SELECT DISTINCT
		RECLAMO AS RECLAMO,
		CONCAT('0', OFICINA) AS OFICINA,
		TIPOCLIENTE AS TIPOCLIENTE,
		IDCLIENTE AS IDCLIENTE,
		NOMBRECLIENTE AS NOMBRECLIENTE,
		FECHARECLAMO AS FECHARECLAMO,
		TIPOINSTRUMENTO AS TIPOINSTRUMENTO,
		TIPORECLAMO AS TIPORECLAMO,
		MONTORECLAMO AS MONTORECLAMO,
		CODIGOCUENTA AS CODIGOCUENTA,
		CREDITOAFECTADO AS CREDITOAFECTADO,
		TARJETAAFECTADA AS TARJETAAFECTADA,
		ENTESUPERVISADO AS ENTESUPERVISADO,
		ESTADORECLAMO AS ESTADORECLAMO,
		FECHASOLUCION AS FECHASOLUCION,
		MONTOREINTEGRO AS MONTOREINTEGRO,
		FECHAREINTEGRO AS FECHAREINTEGRO,
		FECHANOTIFICACION AS FECHANOTIFICACION,
		CODIGOENTE AS CODIGOENTE,
		IDPOS AS IDPOS,
		SISTEMA AS SISTEMA,
		CASE 
			WHEN OFICINARECLAMO = '0' THEN OFICINARECLAMO 
			ELSE CONCAT('0', OFICINARECLAMO) 
		END AS OFICINARECLAMO,
		TIPOOPERACION AS TIPOOPERACION,
		CANAL AS CANAL,
		FRANQUICIA AS FRANQUICIA,
		RED AS RED,
		TIPOCLIENTEDESTINO AS TIPOCLIENTEDESTINO,
		IDCLIENTEDESTINO AS IDCLIENTEDESTINO,
		NOMBRECLIENTEDESTINO AS NOMBRECLIENTEDESTINO,
		NROCUENTADESTINO AS NROCUENTADESTINO,
		CODIGOCUENTADESTINO AS CODIGOCUENTADESTINO,
		MONTODESTINO AS MONTODESTINO
	FROM ATSUDEBAN.AT13_TH_BC
	ORDER BY RECLAMO ASC;
	'''
	hook.run(sql_query_deftxt)

def ATS_TH_AT13_TOTXT(**kwargs): 
	# Conexion a la bd at13
	hook = PostgresHook(postgres_conn_id='repodataprd')
	gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

	# Recuperar las variables definidas en las tareas previas
	FileAT = get_variable('FileAT_at13')
	FileCodSupervisado  = get_variable('FileCodSupervisado')
	FechaFile = get_variable('FechaFile')

	# Generar txt
	logger.info("Obteniendo registros de la base de datos...")
	registros = hook.get_records(''' SELECT DISTINCT
		RECLAMO AS RECLAMO,
		CONCAT('0', OFICINA) AS OFICINA,
		TIPOCLIENTE AS TIPOCLIENTE,
		IDCLIENTE AS IDCLIENTE,
		NOMBRECLIENTE AS NOMBRECLIENTE,
		FECHARECLAMO AS FECHARECLAMO,
		TIPOINSTRUMENTO AS TIPOINSTRUMENTO,
		TIPORECLAMO AS TIPORECLAMO,
		REPLACE(TO_CHAR(MONTORECLAMO, 'FM99999999999990D00'), '.', ',') AS MONTORECLAMO,
		CODIGOCUENTA AS CODIGOCUENTA,
		CREDITOAFECTADO AS CREDITOAFECTADO,
		TARJETAAFECTADA AS TARJETAAFECTADA,
		ENTESUPERVISADO AS ENTESUPERVISADO,
		ESTADORECLAMO AS ESTADORECLAMO,
		FECHASOLUCION AS FECHASOLUCION,
		REPLACE(TO_CHAR(MONTOREINTEGRO, 'FM99999999999990D00'), '.', ',') AS MONTOREINTEGRO,
		FECHAREINTEGRO AS FECHAREINTEGRO,
		FECHANOTIFICACION AS FECHANOTIFICACION,
		CODIGOENTE AS CODIGOENTE,
		IDPOS AS IDPOS,
		SISTEMA AS SISTEMA,
		CASE 
			WHEN OFICINARECLAMO = '0' THEN OFICINARECLAMO 
			ELSE CONCAT('0', OFICINARECLAMO) 
		END AS OFICINARECLAMO,
		TIPOOPERACION AS TIPOOPERACION,
		CANAL AS CANAL,
		FRANQUICIA AS FRANQUICIA,
		RED AS RED,
		TIPOCLIENTEDESTINO AS TIPOCLIENTEDESTINO,
		IDCLIENTEDESTINO AS IDCLIENTEDESTINO,
		NOMBRECLIENTEDESTINO AS NOMBRECLIENTEDESTINO,
		NROCUENTADESTINO AS NROCUENTADESTINO,
		CODIGOCUENTADESTINO AS CODIGOCUENTADESTINO,
		REPLACE(TO_CHAR(MONTODESTINO, 'FM99999999999990D00'), '.', ',') AS MONTODESTINO
	FROM ATSUDEBAN.ATS_TH_AT13;''')
	logger.info(f"Se obtuvieron {len(registros)} registros.")

	# Definir la ruta del archivo de salida en GCS
	gcs_bucket = 'airflow-dags-data'
	gcs_object_path = f"data/AT13/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFile}.txt"
	
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

dag = DAG(dag_id='AT13_TO_FILE', default_args=default_args, schedule=None, catchup=False)

AT13_ATSUDEBAN_TOFILE_task = PythonOperator(
	task_id='AT13_ATSUDEBAN_TOFILE_task',
	python_callable=AT13_ATSUDEBAN_TOFILE,
	dag=dag
)

ATS_TH_AT13_TOTXT_task = PythonOperator(
	task_id='ATS_TH_AT13_TOTXT_task',
	python_callable=ATS_TH_AT13_TOTXT,
	dag=dag
)

Enviar_Email_task = EmailOperator(
	task_id='Enviar_Email_task',
	to='soporte-da@sannet.com.ve',            # correo destino
	subject='DAG {{ dag.dag_id }} completado',   # asunto del correo
	html_content="""                             
		<h3>¡Hola!</h3>
		<p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at13 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>
	""",
	conn_id="email_conn",
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT13_ATSUDEBAN_TOFILE_task >> ATS_TH_AT13_TOTXT_task >> Enviar_Email_task
