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
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import tempfile 

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

def FechaFin_Sem2(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)) + 6, 'DD/MM/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_Sem2', serialize_value(result[0][0]))

def FechaFin_Sem(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)) + 6, 'MM/DD/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_Sem', serialize_value(result[0][0]))

def FechafinS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	FechaFin_Sem = get_variable('FechaFin_Sem')

	sql_query = f'''SELECT '{FechaFin_Sem}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechafinS', serialize_value(result[0][0]))

def FechaFileS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	FechafinS = get_variable('FechafinS')

	sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechafinS}', 'MM/DD/YY'), 'YYMMDD');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFileS', serialize_value(result[0][0]))

def FileCodSupervisado(**kwargs):
	value = '01410'
	Variable.set('FileCodSupervisado', serialize_value(value))

def FechaInicio_Sem(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)), 'MM/DD/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio_Sem', serialize_value(result[0][0]))

def FechainicioS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	FechaInicio_Sem = get_variable('FechaInicio_Sem')

	sql_query = f'''SELECT '{FechaInicio_Sem}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechainicioS', serialize_value(result[0][0]))


def AT44_CB_COTIZACION(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	FechaFin_Sem2 = get_variable('FechaFin_Sem2')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT44_CB_COTIZACION (
	CT_EMPRESA NUMERIC(3), 
	CT_MONEDA NUMERIC(3), 
	CT_FECHA DATE, 
	CT_VALOR NUMERIC(32,7), 
	CT_COMPRA NUMERIC(32,10), 
	CT_VENTA NUMERIC(32,7));'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT44_CB_COTIZACION;'''
	hook.run(sql_query_deftxt)

	# INSERTAMOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at44_cb_cotizacion (
	CT_EMPRESA, 
	CT_MONEDA, 
	CT_FECHA, 
	CT_VALOR, 
	CT_COMPRA, 
	CT_VENTA
	) 
	SELECT
		CB_COTIZACION.CT_EMPRESA,
		CB_COTIZACION.CT_MONEDA,
		CB_COTIZACION.CT_FECHA,
		CB_COTIZACION.CT_VALOR,
		CB_COTIZACION.CT_COMPRA,
		CB_COTIZACION.CT_VENTA
	FROM ODS.CB_COTIZACION AS CB_COTIZACION
	WHERE CB_COTIZACION.CT_FECHA = TO_DATE('{FechaFin_Sem2}', 'DD/MM/YYYY') - INTERVAL '1 day'
		AND CB_COTIZACION.CT_EMPRESA = 1
		AND CB_COTIZACION.CT_MONEDA IN (1, 42, 31);'''
	hook.run(sql_query_deftxt)

def AT44_CC_HIS_MOVIMIENTO(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	FechaInicio_Sem = get_variable('FechaInicio_Sem')
	FechaFin_Sem = get_variable('FechaFin_Sem')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT44_CC_HIS_MOVIMIENTO (
	HM_CTA_BANCO VARCHAR(50), 
	HM_VALOR NUMERIC(32,7)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT44_CC_HIS_MOVIMIENTO;'''
	hook.run(sql_query_deftxt)

	# INSERTAMOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.AT44_CC_HIS_MOVIMIENTO (
	HM_CTA_BANCO, 
	HM_VALOR
	)
	SELECT
	TRIM(CC_HIS_MOVIMIENTO.HM_CTA_BANCO) AS HM_CTA_BANCO,
	(SUM(CC_HIS_MOVIMIENTO.HM_VALOR) + SUM(CC_HIS_MOVIMIENTO.HM_MONTO_IMP)) AS HM_VALOR
	FROM ODS.CC_HIS_MOVIMIENTO
	WHERE (CC_HIS_MOVIMIENTO.HM_FECHA BETWEEN TO_DATE('{FechaInicio_Sem}', 'MM/DD/YY') AND TO_DATE('{FechaFin_Sem}', 'MM/DD/YY') 
		AND CC_HIS_MOVIMIENTO.HM_SIGNO = 'D' 
		AND CC_HIS_MOVIMIENTO.HM_ESTADO <> 'R' 
		AND CC_HIS_MOVIMIENTO.HM_MONEDA <> 0)
	GROUP BY TRIM(CC_HIS_MOVIMIENTO.HM_CTA_BANCO);'''
	hook.run(sql_query_deftxt)

def AT44_CC_CTACTE_TMP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at44_cc_ctacte_tmp (
	tipocliente VARCHAR(1),
	identificacion VARCHAR(40),
	cliente VARCHAR(64),
	tipopersonarif VARCHAR(1),
	identifacionrif VARCHAR(40),
	tipoinstrumento VARCHAR(1),
	cc_cta_banco VARCHAR(24),
	instrumentocaptacion VARCHAR(70),
	fechainicio VARCHAR(8),
	producto VARCHAR(1),
	moneda NUMERIC(3),
	saldodivisas NUMERIC(32,7),
	retirodebitos NUMERIC(32,7),
	codcuentacontable VARCHAR(50),
	saldobs NUMERIC(32,7),
	tipocambio NUMERIC(32,7),
	control VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT44_CC_CTACTE_TMP;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT44_CC_CTACTE_TMP (
	TIPOCLIENTE,
	IDENTIFICACION,
	CLIENTE,
	TIPOPERSONARIF,
	IDENTIFACIONRIF,
	CC_CTA_BANCO,
	FECHAINICIO,
	MONEDA,
	SALDODIVISAS,
	CODCUENTACONTABLE,
	TIPOINSTRUMENTO,
	INSTRUMENTOCAPTACION,
	PRODUCTO,
	RETIRODEBITOS,
	SALDOBS,
	TIPOCAMBIO,
	CONTROL
	) 
	SELECT
		CASE
			WHEN CL_ENTE.EN_CED_RUC IS NOT NULL THEN substring(cl_ente.en_ced_ruc FROM 1 FOR 1)
			WHEN CL_ENTE.EN_CED_RUC IS NULL THEN 'P'
			ELSE ' '
		END AS TIPOCLIENTE,
		CASE
			WHEN CL_ENTE.EN_CED_RUC IS NOT NULL THEN SUBSTRING(CL_ENTE.EN_CED_RUC FROM 2 FOR 19)
			WHEN CL_ENTE.EN_CED_RUC IS NULL THEN CL_ENTE.P_PASAPORTE
			ELSE ' '
		END AS IDENTIFICACION,
		TRIM(CC_CTACTE_TMP.CC_NOMBRE) AS CLIENTE,
		CASE
			WHEN substr(cl_ente.en_ced_ruc, 1, 1) IN ('V','E')
				THEN substr(cl_ente.en_nit, 1, 1)
			WHEN substr(cl_ente.en_ced_ruc, 1, 1) IN ('J','G','R','C')
				THEN substr(cl_ente.en_ced_ruc, 1, 1)
			WHEN cl_ente.en_ced_ruc IS NULL
				OR cl_ente.en_ced_ruc = ' '
				THEN 'P'
			ELSE ' '
		END AS TIPOPERSONARIF,
		CASE
			WHEN substr(cl_ente.en_ced_ruc, 1, 1) IN ('V','E')
				THEN substr(cl_ente.en_nit, 2, 19)
			WHEN substr(cl_ente.en_ced_ruc, 1, 1) IN ('J','G','R','C')
				THEN substr(cl_ente.en_ced_ruc, 2, 19)
			WHEN cl_ente.en_ced_ruc IS NULL OR cl_ente.en_ced_ruc = ' '
				THEN cl_ente.p_pasaporte
			ELSE ' '
		END AS IDENTIFACIONRIF,
		TRIM(CC_CTACTE_TMP.CC_CTA_BANCO) AS CC_CTA_BANCO,
		TO_CHAR(CC_CTACTE_TMP.CC_FECHA_APER, 'YYYYMMDD') AS FECHAINICIO,
		CC_CTACTE_TMP.CC_MONEDA AS MONEDA,
		CC_CTACTE_TMP.CC_DISPONIBLE AS SALDODIVISAS,
		CASE
			WHEN CC_CTACTE_TMP.CC_MONEDA IN (1, 42) AND CC_CTACTE_TMP.CC_TIPOCTA = 'P' THEN '2110520100'
			WHEN CC_CTACTE_TMP.CC_MONEDA IN (1, 42) AND CC_CTACTE_TMP.CC_TIPOCTA = 'C' THEN '2110520200'
			WHEN CC_CTACTE_TMP.CC_MONEDA = 31 AND CC_CTACTE_TMP.CC_TIPOCTA = 'P' THEN '2110520100'
			WHEN CC_CTACTE_TMP.CC_MONEDA = 31 AND CC_CTACTE_TMP.CC_TIPOCTA = 'C' THEN '2110520200'
		END AS CODCUENTACONTABLE,
		'1' AS TIPOINSTRUMENTO,
		'Captacion en Moneda Extranjera' AS INSTRUMENTOCAPTACION,
		'1' AS PRODUCTO,
		0 AS RETIRODEBITOS,
		0 AS SALDOBS,
		0 AS TIPOCAMBIO,
		'1' AS CONTROL
	FROM ODS.CC_CTACTE_TMP AS CC_CTACTE_TMP INNER JOIN ODS.CL_ENTE AS CL_ENTE ON (CC_CTACTE_TMP.CC_CLIENTE = CL_ENTE.EN_ENTE)
	WHERE CC_CTACTE_TMP.CC_MONEDA <> 0 AND CC_CTACTE_TMP.CC_ESTADO = 'A' AND CC_CTACTE_TMP.CC_CATEGORIA IN ('U', 'M');'''
	hook.run(sql_query_deftxt)

def AT44_ALL(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT44_ALL (
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACION	VARCHAR(40),
	CLIENTE	VARCHAR(64),
	TIPOPERSONARIF	VARCHAR(1),
	IDENTIFACIONRIF	VARCHAR(40),
	TIPOINSTRUMENTO	VARCHAR(1),
	CC_CTA_BANCO	VARCHAR(24),
	INSTRUMENTOCAPTACION	VARCHAR(70),
	FECHAINICIO	VARCHAR(8),
	PRODUCTO	VARCHAR(1),
	MONEDA	VARCHAR(3),
	SALDODIVISAS	NUMERIC(32,4),
	HM_VALOR	NUMERIC(32,4),
	CODCUENTACONTABLE	VARCHAR(50),
	SALDOBS	NUMERIC(32,4),
	CT_VALOR	NUMERIC(32,4),
	CONTROL	VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT44_ALL;'''
	hook.run(sql_query_deftxt)
	
	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT44_ALL (
	TIPOCLIENTE,
	IDENTIFICACION,
	CLIENTE,
	TIPOPERSONARIF,
	IDENTIFACIONRIF,
	TIPOINSTRUMENTO,
	CC_CTA_BANCO,
	INSTRUMENTOCAPTACION,
	FECHAINICIO,
	PRODUCTO,
	MONEDA,
	SALDODIVISAS,
	HM_VALOR,
	CODCUENTACONTABLE,
	SALDOBS,
	CT_VALOR,
	CONTROL
	)
	SELECT 
		AT44_CC_CTACTE_TMP.TIPOCLIENTE,
		AT44_CC_CTACTE_TMP.IDENTIFICACION,
		AT44_CC_CTACTE_TMP.CLIENTE,
		AT44_CC_CTACTE_TMP.TIPOPERSONARIF,
		CASE
			WHEN substr(at44_cc_ctacte_tmp.identificacion, 1, 1) IN ('P','M','R')
				THEN substr(at44_cc_ctacte_tmp.identifacionrif, 2, 19)
			ELSE at44_cc_ctacte_tmp.identifacionrif
		END,
		AT44_CC_CTACTE_TMP.TIPOINSTRUMENTO,
		AT44_CC_CTACTE_TMP.CC_CTA_BANCO,
		AT44_CC_CTACTE_TMP.INSTRUMENTOCAPTACION,
		AT44_CC_CTACTE_TMP.FECHAINICIO,
		AT44_CC_CTACTE_TMP.PRODUCTO,
		CASE
			WHEN AT44_CC_CTACTE_TMP.MONEDA = 1 THEN 'USD'
			WHEN AT44_CC_CTACTE_TMP.MONEDA = 42 THEN 'EUR'
			WHEN AT44_CC_CTACTE_TMP.MONEDA = 31 THEN 'COP'
		END,
		ROUND(AT44_CC_CTACTE_TMP.SALDODIVISAS, 4),
		ROUND(AT44_CC_HIS_MOVIMIENTO.HM_VALOR, 4),
		AT44_CC_CTACTE_TMP.CODCUENTACONTABLE,
		ROUND(AT44_CC_CTACTE_TMP.SALDOBS, 4),
		ROUND(AT44_CB_COTIZACION.CT_VALOR, 4),
		AT44_CC_CTACTE_TMP.CONTROL
	FROM (at_stg.at44_cc_ctacte_tmp AS at44_cc_ctacte_tmp LEFT JOIN at_stg.at44_cc_his_movimiento AS at44_cc_his_movimiento
  		ON at44_cc_ctacte_tmp.cc_cta_banco = at44_cc_his_movimiento.hm_cta_banco) LEFT JOIN at_stg.at44_cb_cotizacion AS at44_cb_cotizacion
  		ON at44_cc_ctacte_tmp.moneda = at44_cb_cotizacion.ct_moneda;'''
	hook.run(sql_query_deftxt)

def AT44_ACTUALIZAR_SALDOS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	sql_query_deftxt = '''UPDATE AT_STG.AT44_ALL SET HM_VALOR = 0 WHERE HM_VALOR IS NULL;'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE AT_STG.AT44_ALL SET SALDOBS = (SALDODIVISAS * CT_VALOR);'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE AT_STG.AT44_ALL
	SET
		TIPOCLIENTE='J',
		IDENTIFICACION='000029490',
		TIPOPERSONARIF='J',
		IDENTIFACIONRIF='000029490'
	WHERE
		IDENTIFACIONRIF IS NULL;'''
	hook.run(sql_query_deftxt)

def AT44_FINAL(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT44_FINAL (
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACION	VARCHAR(40),
	CLIENTE	VARCHAR(64),
	TIPOPERSONARIF	VARCHAR(1),
	IDENTIFACIONRIF	VARCHAR(40),
	TIPOINSTRUMENTO	VARCHAR(1),
	CC_CTA_BANCO	VARCHAR(24),
	INSTRUMENTOCAPTACION	VARCHAR(70),
	FECHAINICIO	VARCHAR(8),
	PRODUCTO	VARCHAR(1),
	MONEDA	VARCHAR(3),
	SALDODIVISAS	VARCHAR(36),
	HM_VALOR	VARCHAR(36),
	CODCUENTACONTABLE	VARCHAR(50),
	SALDOBS	VARCHAR(36),
	CT_VALOR	VARCHAR(36),
	CONTROL	VARCHAR(1),
	FECHACARGA	DATE
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT44_FINAL;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO atsudeban.at44_final (
	TIPOCLIENTE,
	IDENTIFICACION,
	CLIENTE,
	TIPOPERSONARIF,
	IDENTIFACIONRIF,
	TIPOINSTRUMENTO,
	CC_CTA_BANCO,
	INSTRUMENTOCAPTACION,
	FECHAINICIO,
	PRODUCTO,
	MONEDA,
	SALDODIVISAS,
	HM_VALOR,
	CODCUENTACONTABLE,
	SALDOBS,
	CT_VALOR,
	CONTROL,
	FECHACARGA
	) 
	SELECT
		AT44_ALL.TIPOCLIENTE,
		AT44_ALL.IDENTIFICACION,
		AT44_ALL.CLIENTE,
		AT44_ALL.TIPOPERSONARIF,
		AT44_ALL.IDENTIFACIONRIF,
		AT44_ALL.TIPOINSTRUMENTO,
		AT44_ALL.CC_CTA_BANCO,
		AT44_ALL.INSTRUMENTOCAPTACION,
		AT44_ALL.FECHAINICIO,
		AT44_ALL.PRODUCTO,
		AT44_ALL.MONEDA,
		REPLACE(TRIM(TO_CHAR(AT44_ALL.saldodivisas, 'FM99999999999990D0000')),'.', ','),
		REPLACE(TRIM(TO_CHAR(AT44_ALL.HM_VALOR, 'FM99999999999990D0000')),'.', ','),
		AT44_ALL.CODCUENTACONTABLE,
		REPLACE(TRIM(TO_CHAR(AT44_ALL.SALDOBS, 'FM99999999999990D0000')),'.', ','),
		REPLACE(TRIM(TO_CHAR(AT44_ALL.CT_VALOR, 'FM99999999999990D0000')),'.', ','),
		AT44_ALL.CONTROL,
		CURRENT_DATE AS FECHACARGA
	FROM AT_STG.AT44_ALL;'''
	hook.run(sql_query_deftxt)

def IMPORTAR_INSUMO(**kwargs):
	try:
		# Define la informacion del bucket y el objeto en GCS
		gcs_bucket = 'airflow-dags-data'
		gcs_object = 'data/AT44/INSUMOS/Plantilla_Custodia_AT44.csv'
		
		# Inicializa los hooks
		postgres_hook = PostgresHook(postgres_conn_id='at37')
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
			if not gcs_hook.exists(bucket_name=gcs_bucket, object_name=gcs_object):
				raise Exception(f"El archivo {gcs_object} no existe en el bucket {gcs_bucket}")
			logger.info("Validacion de archivo GCS exitosa")
		except Exception as e:
			logger.error(f"Error al validar archivo en GCS: {str(e)}")
			raise

		# 3. Truncar tabla
		logger.info("Truncando la tabla FILE_AT.AT44...")
		postgres_hook.run("TRUNCATE TABLE FILE_AT.AT44;")
		logger.info("Tabla FILE_AT.AT44 truncada exitosamente.")

		# 4. Preparar consulta COPY
		sql_copy = """
		COPY FILE_AT.AT44 (
			TIPOCLIENTE,
			IDENTIFICACION,
			CLIENTE,
			TIPOPERSONARIF,
			IDENTIFACIONRIF,
			TIPOINSTRUMENTO,
			CC_CTA_BANCO,
			INSTRUMENTOCAPTACION,
			FECHAINICIO,
			PRODUCTO,
			MONEDA,
			SALDODIVISAS,
			HM_VALOR,
			CODCUENTACONTABLE,
			SALDOBS,
			CT_VALOR,
			CONTROL
		)
		FROM STDIN
		WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');
		"""
		
		# 5. Descargar archivo temporal y crea directorio temporal
		temp_dir = tempfile.mkdtemp()
		local_file_path = os.path.join(temp_dir, 'Plantilla_Custodia_AT44.csv')
		
		try:
			logger.info(f"Descargando archivo '{gcs_object}' desde GCS...")
			gcs_hook.download(
				bucket_name=gcs_bucket,
				object_name=gcs_object,
				filename=local_file_path
			)
			
			# 6. Validar archivo descargado
			file_size = os.path.getsize(local_file_path)
			if file_size == 0:
				raise Exception("El archivo descargado esta vacio")
				
			logger.info(f"Archivo descargado correctamente. Tamaño: {file_size} bytes")
			
			# 7. Mostrar primeras li­neas para debug (ya usa 'windows-1252' segun tu codigo)
			with open(local_file_path, 'r', encoding='utf8') as f:
				lines = [next(f) for _ in range(5)]
				logger.info("Primeras lineas del CSV:\n" + "".join(lines))

			# 8. Ejecutar COPY
			logger.info("Iniciando carga de datos a PostgreSQL...")
			start_time = datetime.now()
			
			postgres_hook.copy_expert(sql=sql_copy, filename=local_file_path)
			
			duration = (datetime.now() - start_time).total_seconds()
			logger.info(f"Carga completada en {duration:.2f} segundos")
			
			# 9. Verificar conteo de registros
			count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_AT.AT44;")[0]
			logger.info(f"Total de registros cargados: {count}")
			
			if count == 0:
				raise Exception("No se cargaron registros - verificar formato del archivo CSV")
				
		except Exception as e:
			logger.error(f"Error durante la carga de datos: {str(e)}")
			
			# Intentar leer el archivo para debug (ya usa 'windows-1252' segun tu codigo)
			try:
				with open(local_file_path, 'r', encoding='utf8') as f:
					sample = f.read(1000)
					logger.info(f"Contenido parcial del archivo:\n{sample}")
			except Exception as read_error:
				logger.error(f"No se pudo leer el archivo para debug: {str(read_error)}")
			
			raise
			
		finally:
			# Limpieza siempre se ejecuta
			if os.path.exists(local_file_path):
				os.remove(local_file_path)
				logger.info(f"Archivo temporal eliminado: {local_file_path}")
			if os.path.exists(temp_dir):
				os.rmdir(temp_dir)
				logger.info(f"Directorio temporal eliminado: {temp_dir}")
				
	except Exception as e:
		logger.error(f"Error general en IMPORTAR_INSUMO: {str(e)}")
		# Registrar el error completo para diagnostico
		import traceback
		logger.error("Traceback completo:\n" + traceback.format_exc())
		raise

def AT44_IMPORTAR(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# INSERTAMOS EN DESTINO (UNION CON TAREA ANTERIOR)
	sql_query_deftxt = '''INSERT INTO atsudeban.at44_final (
	TIPOCLIENTE,
	IDENTIFICACION,
	CLIENTE,
	TIPOPERSONARIF,
	IDENTIFACIONRIF,
	TIPOINSTRUMENTO,
	CC_CTA_BANCO,
	INSTRUMENTOCAPTACION,
	FECHAINICIO,
	PRODUCTO,
	MONEDA,
	SALDODIVISAS,
	HM_VALOR,
	CODCUENTACONTABLE,
	SALDOBS,
	CT_VALOR,
	CONTROL,
	FECHACARGA
	)
	SELECT
		TIPOCLIENTE,
		IDENTIFICACION,
		CLIENTE,
		TIPOPERSONARIF,
		IDENTIFACIONRIF,
		TIPOINSTRUMENTO,
		CC_CTA_BANCO,
		INSTRUMENTOCAPTACION,
		FECHAINICIO,
		PRODUCTO,
		MONEDA,
		CAST(SALDODIVISAS AS TEXT) AS SALDODIVISAS,
		HM_VALOR,
		CODCUENTACONTABLE,
		SALDOBS,
		CT_VALOR,
		CONTROL,
		CURRENT_DATE AS FECHACARGA
	FROM FILE_AT.AT44;'''
	hook.run(sql_query_deftxt)

def FileAT_at44(**kwargs):
	value = 'AT44'
	Variable.set('FileAT_at44', serialize_value(value))

def ELIMINAR_REGISTROS_DIARIOS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	sql_query_deftxt = '''DELETE FROM ATSUDEBAN.AT44_FINAL WHERE FECHACARGA >= CURRENT_DATE;'''
	hook.run(sql_query_deftxt)

def AT44_TO_FILE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at44')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.AT44_SALIDA (
	TIPOCLIENTE VARCHAR(1),
	IDENTIFICACION VARCHAR(50),
	CLIENTE VARCHAR(64),
	TIPOPERSONARIF VARCHAR(1),
	IDENTIFACIONRIF VARCHAR(40),
	TIPOINSTRUMENTO VARCHAR(1),
	CC_CTA_BANCO VARCHAR(50),
	INSTRUMENTOCAPTACION VARCHAR(70),
	FECHAINICIO VARCHAR(8),
	PRODUCTO VARCHAR(1),
	MONEDA VARCHAR(3),
	SALDODIVISAS VARCHAR(36),
	HM_VALOR VARCHAR(36),
	CODCUENTACONTABLE VARCHAR(50),
	SALDOBS VARCHAR(36),
	CT_VALOR VARCHAR(36),
	CONTROL VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.AT44_SALIDA;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO FILE_AT.AT44_SALIDA (
	TIPOCLIENTE,
	IDENTIFICACION,
	CLIENTE,
	TIPOPERSONARIF,
	IDENTIFACIONRIF,
	TIPOINSTRUMENTO,
	CC_CTA_BANCO,
	INSTRUMENTOCAPTACION,
	FECHAINICIO,
	PRODUCTO,
	MONEDA,
	SALDODIVISAS,
	HM_VALOR,
	CODCUENTACONTABLE,
	SALDOBS,
	CT_VALOR,
	CONTROL
	)
	SELECT
		TIPOCLIENTE,
		IDENTIFICACION,
		CLIENTE,
		TIPOPERSONARIF,
		IDENTIFACIONRIF,
		TIPOINSTRUMENTO,
		CC_CTA_BANCO,
		INSTRUMENTOCAPTACION,
		FECHAINICIO,
		PRODUCTO,
		MONEDA,
		SALDODIVISAS,
		HM_VALOR,
		CODCUENTACONTABLE,
		SALDOBS,
		CT_VALOR,
		CONTROL
	FROM ATSUDEBAN.AT44_FINAL
	WHERE FECHACARGA >= CURRENT_DATE
	ORDER BY CC_CTA_BANCO, CODCUENTACONTABLE; '''
	hook.run(sql_query_deftxt)

def AT44_SALIDA_TOTXT(**kwargs):
	# Conexion a la bd
	hook = PostgresHook(postgres_conn_id='at44')
	gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

	# Recuperar las variables definidas en las tareas previas
	FileAT = get_variable('FileAT_at44')
	FileCodSupervisado = get_variable('FileCodSupervisado')
	FechaFileS = get_variable('FechaFileS')

	# Generar txt
	logger.info("Obteniendo registros de la base de datos...")
	registros = hook.get_records('SELECT * FROM FILE_AT.AT44_SALIDA;')
	logger.info(f"Se obtuvieron {len(registros)} registros.")

	# Definir la ruta del archivo de salida en GCS
	gcs_bucket = 'airflow-dags-data'
	gcs_object_path = f"data/AT44/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFileS}.txt"
	
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

dag = DAG(dag_id='AT44',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

wait_for_file = FileSensor(        # En la secuencia de ejecucion, colocar este operador en el orden que convenga (Agregar un operador FileSensor por cada insumo que use el AT)
	task_id='wait_for_file',
	fs_conn_id='nombre_conexion',  # Conexion predefinida en Airflow para el file system (en Admin > Connections)
	filepath='Plantilla_Custodia_AT44.csv', # Colocar nombre del insumo
	poke_interval=10,              # Intervalo en segundos para verificar
	timeout=60 * 10,               # Timeout en segundos (10 minutos en este ejemplo)
	dag=dag
)

FechaFin_Sem2_task = PythonOperator(
	task_id='FechaFin_Sem2_task',
	python_callable=FechaFin_Sem2,
	dag=dag
)

FechaFin_Sem_task = PythonOperator(
	task_id='FechaFin_Sem_task',
	python_callable=FechaFin_Sem,
	dag=dag
)

FechafinS_task = PythonOperator(
	task_id='FechafinS_task',
	python_callable=FechafinS,
	dag=dag
)

FechaFileS_task = PythonOperator(
	task_id='FechaFileS_task',
	python_callable=FechaFileS,
	dag=dag
)

FileCodSupervisado_task = PythonOperator(
	task_id='FileCodSupervisado_task',
	python_callable=FileCodSupervisado,
	dag=dag
)

FechaInicio_Sem_task = PythonOperator(
	task_id='FechaInicio_Sem_task',
	python_callable=FechaInicio_Sem,
	dag=dag
)

FechainicioS_task = PythonOperator(
	task_id='FechainicioS_task',
	python_callable=FechainicioS,
	dag=dag
)

AT44_CB_COTIZACION_task = PythonOperator(
	task_id='AT44_CB_COTIZACION_task',
	python_callable=AT44_CB_COTIZACION,
	dag=dag
)

AT44_CC_HIS_MOVIMIENTO_task = PythonOperator(
	task_id='AT44_CC_HIS_MOVIMIENTO_task',
	python_callable=AT44_CC_HIS_MOVIMIENTO,
	dag=dag
)

AT44_CC_CTACTE_TMP_task = PythonOperator(
	task_id='AT44_CC_CTACTE_TMP_task',
	python_callable=AT44_CC_CTACTE_TMP,
	dag=dag
)

AT44_ALL_task = PythonOperator(
	task_id='AT44_ALL_task',
	python_callable=AT44_ALL,
	dag=dag
)

AT44_ACTUALIZAR_SALDOS_task = PythonOperator(
	task_id='AT44_ACTUALIZAR_SALDOS_task',
	python_callable=AT44_ACTUALIZAR_SALDOS,
	dag=dag
)

AT44_FINAL_task = PythonOperator(
	task_id='AT44_FINAL_task',
	python_callable=AT44_FINAL,
	dag=dag
)

IMPORTAR_INSUMO_task = PythonOperator(
	task_id='IMPORTAR_INSUMO_task',
	python_callable=IMPORTAR_INSUMO,
	dag=dag
)

AT44_IMPORTAR_task = PythonOperator(
	task_id='AT44_IMPORTAR_task',
	python_callable=AT44_IMPORTAR,
	dag=dag
)

FileAT_task = PythonOperator(
	task_id='FileAT_task',
	python_callable=FileAT_at44,
	dag=dag
)

ELIMINAR_REGISTROS_DIARIOS_task = PythonOperator(
	task_id='ELIMINAR_REGISTROS_DIARIOS_task',
	python_callable=ELIMINAR_REGISTROS_DIARIOS,
	dag=dag
)

AT44_TO_FILE_task = PythonOperator(
	task_id='AT44_TO_FILE_task',
	python_callable=AT44_TO_FILE,
	dag=dag
)

AT44_SALIDA_TOTXT_task = PythonOperator(
	task_id='AT44_SALIDA_TOTXT_task',
	python_callable=AT44_SALIDA_TOTXT,
	dag=dag
)

Enviar_Email_task = EmailOperator(
	task_id='Enviar_Email_task',
	to='colocar_correo_aqui@gmail.com',            # correo destino
	subject='DAG {{ dag.dag_id }} completado',   # asunto del correo
	html_content="""                             
		<h3>¡Hola!</h3>
		<p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at44 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFileS }}.txt</p>
	""",
	conn_id="email_conn",
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
wait_for_file >> FechaFin_Sem2_task >> FechaFin_Sem_task >> FechafinS_task >> FechaFileS_task >> FileCodSupervisado_task >> FechaInicio_Sem_task >> FechainicioS_task >> AT44_CB_COTIZACION_task >> AT44_CC_HIS_MOVIMIENTO_task >> AT44_CC_CTACTE_TMP_task >> AT44_ALL_task >> AT44_ACTUALIZAR_SALDOS_task >> AT44_FINAL_task >> IMPORTAR_INSUMO_task >> AT44_IMPORTAR_task >> FileAT_task >> ELIMINAR_REGISTROS_DIARIOS_task >> AT44_TO_FILE_task >> AT44_SALIDA_TOTXT_task >> Enviar_Email_task
