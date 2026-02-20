import logging
from decimal import Decimal
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import time
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
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

def AT12_FileName_TDD(**kwargs):
	FileAT = get_variable('FileAT_at12')
	FileCodSupervisado = get_variable('FileCodSupervisado')
	FechaFile = get_variable('FechaFile')

	value = f'{FileAT}{FileCodSupervisado}{FechaFile}_TDD.txt'
	Variable.set('AT12_FileName_TDD', serialize_value(value))

def AT12_FileName_BIP(**kwargs):
	FileAT = get_variable('FileAT_at12')
	FileCodSupervisado = get_variable('FileCodSupervisado')
	FechaFile = get_variable('FechaFile')

	value = f'{FileAT}{FileCodSupervisado}{FechaFile}_BIP.txt'
	Variable.set('AT12_FileName_BIP', serialize_value(value))

def IMPORTAR_INSUMOS_TDD_BIP(**kwargs):

	FechaFile = get_variable('FechaFile')
	AT12_FileName_TDD = get_variable('AT12_FileName_TDD')
	AT12_FileName_BIP = get_variable('AT12_FileName_BIP')

	# Comprobamos que la FechaFile coincida en ambos txt: tdd y bip
	# Si la fecha no aparece en ambos nombres, hace return
	if not (FechaFile in AT12_FileName_TDD and FechaFile in AT12_FileName_BIP):
		logging.info(f"Fechas del txt de TDD y txt de BIP no coinciden ({FechaFile}).")
		return  # la tarea acaba aqui, sin error

	# IMPORTAR TDD
	try:
		# Define la informacion del bucket y el objeto en GCS
		gcs_bucket = 'airflow-dags-data'
		gcs_object_tdd = f"data/AT12/SALIDAS/{AT12_FileName_TDD}"
		
		# Inicializa los hooks
		postgres_hook = PostgresHook(postgres_conn_id='at12')
		gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

		# 1. Validar conexion a PostgreSQL
		try:
			conn = postgres_hook.get_conn()
			cursor = conn.cursor()
			cursor.execute("SELECT 1")
			logger.info("Conexion a PostgreSQL validada exitosamente")
		except Exception as e:
			logger.error(f"Error al conectar a PostgreSQL: {str(e)}")
			raise

		# 2. Validar acceso a GCS
		try:
			if not gcs_hook.exists(bucket_name=gcs_bucket, object_name=gcs_object_tdd):
				raise Exception(f"El archivo {gcs_object_tdd} no existe en el bucket {gcs_bucket}")
			logger.info("Validacion de archivo GCS exitosa")
		except Exception as e:
			logger.error(f"Error al validar archivo en GCS: {str(e)}")
			raise

		# 3. Truncar tabla
		logger.info("Truncando la tabla FILE_AT12_UNIFICADO.AT12_TDD...")
		postgres_hook.run("TRUNCATE TABLE FILE_AT12_UNIFICADO.AT12_TDD;")
		logger.info("Tabla FILE_AT12_UNIFICADO.AT12_TDD truncada exitosamente.")

		# 4. Preparar consulta COPY
		sql_tdd = """
		COPY FILE_AT12_UNIFICADO.AT12_TDD (
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
		FROM STDIN
		WITH (FORMAT txt, DELIMITER '~', ENCODING 'UTF8');
		"""
		
		# 5. Descargar archivo temporal y crea directorio temporal
		temp_dir_tdd = tempfile.mkdtemp()
		local_file_path_tdd = os.path.join(temp_dir_tdd, f"{AT12_FileName_TDD}")
		
		try:
			logger.info(f"Descargando archivo '{gcs_object_tdd}' desde GCS...")
			gcs_hook.download(
				bucket_name=gcs_bucket,
				object_name=gcs_object_tdd,
				filename=local_file_path_tdd
			)
			
			# 6. Validar archivo descargado
			file_size = os.path.getsize(local_file_path_tdd)
			if file_size == 0:
				raise Exception("El archivo descargado esta vacio")
				
			logger.info(f"Archivo descargado correctamente. Tamaño: {file_size} bytes")
			
			# 7. Mostrar primeras li­neas para debug (ya usa 'windows-1252' segun tu codigo)
			with open(local_file_path_tdd, 'r', encoding='utf8') as f:
				lines = [next(f) for _ in range(5)]
				logger.info("Primeras lineas del archivo:\n" + "".join(lines))

			# 8. Ejecutar COPY
			logger.info("Iniciando carga de datos a PostgreSQL...")
			start_time = datetime.now()
			
			postgres_hook.copy_expert(sql=sql_tdd, filename=local_file_path_tdd)
			
			duration = (datetime.now() - start_time).total_seconds()
			logger.info(f"Carga completada en {duration:.2f} segundos")
			
			# 9. Verificar conteo de registros
			count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_AT12_UNIFICADO.AT12_TDD;")[0]
			logger.info(f"Total de registros cargados: {count}")
			
			if count == 0:
				raise Exception("No se cargaron registros - verificar formato del archivo")
				
		except Exception as e:
			logger.error(f"Error durante la carga de datos: {str(e)}")
			
			# Intentar leer el archivo para debug (ya usa 'windows-1252' segun tu codigo)
			try:
				with open(local_file_path_tdd, 'r', encoding='utf8') as f:
					sample = f.read(1000)
					logger.info(f"Contenido parcial del archivo:\n{sample}")
			except Exception as read_error:
				logger.error(f"No se pudo leer el archivo para debug: {str(read_error)}")
			
			raise
			
		finally:
			# Limpieza siempre se ejecuta
			if os.path.exists(local_file_path_tdd):
				os.remove(local_file_path_tdd)
				logger.info(f"Archivo temporal eliminado: {local_file_path_tdd}")
			if os.path.exists(temp_dir_tdd):
				os.rmdir(temp_dir_tdd)
				logger.info(f"Directorio temporal eliminado: {temp_dir_tdd}")
				
	except Exception as e:
		logger.error(f"Error general en IMPORTAR_INSUMO: {str(e)}")
		# Registrar el error completo para diagnostico
		import traceback
		logger.error("Traceback completo:\n" + traceback.format_exc())
		raise


	# IMPORTAR BIP
	try:
		# Define la informacion del bucket y el objeto en GCS
		gcs_object_bip = f"data/AT12/SALIDAS/{AT12_FileName_BIP}"

		# 2. Validar acceso a GCS
		try:
			if not gcs_hook.exists(bucket_name=gcs_bucket, object_name=gcs_object_bip):
				raise Exception(f"El archivo {gcs_object_bip} no existe en el bucket {gcs_bucket}")
			logger.info("Validacion de archivo GCS exitosa")
		except Exception as e:
			logger.error(f"Error al validar archivo en GCS: {str(e)}")
			raise

		# 3. Truncar tabla
		logger.info("Truncando la tabla FILE_AT12_UNIFICADO.AT12_BIP...")
		postgres_hook.run("TRUNCATE TABLE FILE_AT12_UNIFICADO.AT12_BIP;")
		logger.info("Tabla FILE_AT12_UNIFICADO.AT12_BIP truncada exitosamente.")

		# 4. Preparar consulta COPY
		sql_bip = """
		COPY FILE_AT12_UNIFICADO.AT12_BIP (
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
		FROM STDIN
		WITH (FORMAT txt, DELIMITER '~', ENCODING 'UTF8');
		"""
		
		# 5. Descargar archivo temporal y crea directorio temporal
		temp_dir_bip = tempfile.mkdtemp()
		local_file_path_bip = os.path.join(temp_dir_bip, f"{AT12_FileName_BIP}")
		
		try:
			logger.info(f"Descargando archivo '{gcs_object_bip}' desde GCS...")
			gcs_hook.download(
				bucket_name=gcs_bucket,
				object_name=gcs_object_bip,
				filename=local_file_path_bip
			)
			
			# 6. Validar archivo descargado
			file_size = os.path.getsize(local_file_path_bip)
			if file_size == 0:
				raise Exception("El archivo descargado esta vacio")
				
			logger.info(f"Archivo descargado correctamente. Tamaño: {file_size} bytes")
			
			# 7. Mostrar primeras li­neas para debug (ya usa 'windows-1252' segun tu codigo)
			with open(local_file_path_bip, 'r', encoding='utf8') as f:
				lines = [next(f) for _ in range(5)]
				logger.info("Primeras lineas del archivo:\n" + "".join(lines))

			# 8. Ejecutar COPY
			logger.info("Iniciando carga de datos a PostgreSQL...")
			start_time = datetime.now()
			
			postgres_hook.copy_expert(sql=sql_bip, filename=local_file_path_bip)
			
			duration = (datetime.now() - start_time).total_seconds()
			logger.info(f"Carga completada en {duration:.2f} segundos")
			
			# 9. Verificar conteo de registros
			count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_AT12_UNIFICADO.AT12_BIP;")[0]
			logger.info(f"Total de registros cargados: {count}")
			
			if count == 0:
				raise Exception("No se cargaron registros - verificar formato del archivo")
				
		except Exception as e:
			logger.error(f"Error durante la carga de datos: {str(e)}")
			
			# Intentar leer el archivo para debug (ya usa 'windows-1252' segun tu codigo)
			try:
				with open(local_file_path_bip, 'r', encoding='utf8') as f:
					sample = f.read(1000)
					logger.info(f"Contenido parcial del archivo:\n{sample}")
			except Exception as read_error:
				logger.error(f"No se pudo leer el archivo para debug: {str(read_error)}")
			
			raise
			
		finally:
			# Limpieza siempre se ejecuta
			if os.path.exists(local_file_path_bip):
				os.remove(local_file_path_bip)
				logger.info(f"Archivo temporal eliminado: {local_file_path_bip}")
			if os.path.exists(temp_dir_bip):
				os.rmdir(temp_dir_bip)
				logger.info(f"Directorio temporal eliminado: {temp_dir_bip}")
				
	except Exception as e:
		logger.error(f"Error general en IMPORTAR_INSUMO: {str(e)}")
		# Registrar el error completo para diagnostico
		import traceback
		logger.error("Traceback completo:\n" + traceback.format_exc())
		raise

def ATS_TH_AT12_BANCARIBE_UNIFICADO(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT12 (
	PAISCONSUMO VARCHAR(2),
	MUNICIPIO VARCHAR(6),
	EXTRANJERA NUMERIC(2),
	CLASETARJETA NUMERIC(2),
	TIPOTARJETA NUMERIC(2),
	CONCEPTO NUMERIC(5),
	FRANQUICIA NUMERIC(2),
	CANTIDADCONSUMOS NUMERIC(10),
	MONTOCONSUMO NUMERIC(32,2),
	COMISIONESPROPIAS NUMERIC(32,2),
	COMISIONESTERCEROS NUMERIC(32,2),
	RED NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT12;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT12 (
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
		PAISCONSUMO, 
		MUNICIPIO, 
		CAST(REPLACE(EXTRANJERA, ',', '.') AS DECIMAL) AS EXTRANJERA, 
		CAST(REPLACE(CLASETARJETA, ',', '.') AS DECIMAL) AS CLASETARJETA, 
		CAST(REPLACE(TIPOTARJETA, ',', '.') AS DECIMAL) AS TIPOTARJETA, 
		CAST(REPLACE(CONCEPTO, ',', '.') AS DECIMAL) AS CONCEPTO, 
		CAST(REPLACE(FRANQUICIA, ',', '.') AS DECIMAL) AS FRANQUICIA, 
		CAST(REPLACE(CANTIDADCONSUMOS, ',', '.') AS DECIMAL) AS CANTIDADCONSUMOS, 
		CAST(REPLACE(MONTOCONSUMO, ',', '.') AS DECIMAL) AS MONTOCONSUMO, 
		CAST(REPLACE(COMISIONESPROPIAS, ',', '.') AS DECIMAL) AS COMISIONESPROPIAS, 
		CAST(REPLACE(COMISIONESTERCEROS, ',', '.') AS DECIMAL) AS COMISIONESTERCEROS, 
		CAST(REPLACE(RED, ',', '.') AS DECIMAL) AS RED
	FROM FILE_AT12_UNIFICADO.AT12_TDD

	UNION

	SELECT 
		PAISCONSUMO, 
		MUNICIPIO, 
		CAST(REPLACE(EXTRANJERA, ',', '.') AS DECIMAL) AS EXTRANJERA, 
		CAST(REPLACE(CLASETARJETA, ',', '.') AS DECIMAL) AS CLASETARJETA, 
		CAST(REPLACE(TIPOTARJETA, ',', '.') AS DECIMAL) AS TIPOTARJETA, 
		CAST(REPLACE(CONCEPTO, ',', '.') AS DECIMAL) AS CONCEPTO, 
		CAST(REPLACE(FRANQUICIA, ',', '.') AS DECIMAL) AS FRANQUICIA, 
		CAST(REPLACE(CANTIDADCONSUMOS, ',', '.') AS DECIMAL) AS CANTIDADCONSUMOS, 
		CAST(REPLACE(MONTOCONSUMO, ',', '.') AS DECIMAL) AS MONTOCONSUMO, 
		CAST(REPLACE(COMISIONESPROPIAS, ',', '.') AS DECIMAL) AS COMISIONESPROPIAS, 
		CAST(REPLACE(COMISIONESTERCEROS, ',', '.') AS DECIMAL) AS COMISIONESTERCEROS, 
		CAST(REPLACE(RED, ',', '.') AS DECIMAL) AS RED
	FROM FILE_AT12_UNIFICADO.AT12_BIP;'''
	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT12_UNIFICADO',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

AT12_FileName_TDD_task = PythonOperator(
	task_id='AT12_FileName_TDD_task',
	python_callable=AT12_FileName_TDD,
	dag=dag
)

AT12_FileName_BIP_task = PythonOperator(
	task_id='AT12_FileName_BIP_task',
	python_callable=AT12_FileName_BIP,
	dag=dag
)

wait_for_file_TDD = GCSObjectExistenceSensor(
	task_id='wait_for_file_TDD',
	bucket='airflow-dags-data',
	object='data/AT12/SALIDAS/{{ var.value.AT12_FileName_TDD }}',                      
	poke_interval=10,
	timeout=60 * 10,
	dag=dag
)

wait_for_file_BIP = GCSObjectExistenceSensor(
	task_id='wait_for_file_BIP',
	bucket='airflow-dags-data',
	object='data/AT12/SALIDAS/{{ var.value.AT12_FileName_BIP }}',                      
	poke_interval=10,
	timeout=60 * 10,
	dag=dag
)

IMPORTAR_INSUMOS_TDD_BIP_task = PythonOperator(
	task_id='IMPORTAR_INSUMOS_TDD_BIP_task',
	python_callable=IMPORTAR_INSUMOS_TDD_BIP,
	dag=dag
)

ATS_TH_AT12_BANCARIBE_UNIFICADO_task = PythonOperator(
	task_id='ATS_TH_AT12_BANCARIBE_UNIFICADO_task',
	python_callable=ATS_TH_AT12_BANCARIBE_UNIFICADO,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT12_FileName_TDD_task >> AT12_FileName_BIP_task >> wait_for_file_TDD >> wait_for_file_BIP >> IMPORTAR_INSUMOS_TDD_BIP_task >> ATS_TH_AT12_BANCARIBE_UNIFICADO_task
