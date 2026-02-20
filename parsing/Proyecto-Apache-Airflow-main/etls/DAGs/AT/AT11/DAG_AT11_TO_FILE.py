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

def FechaFin_Sem2(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)) + 6, 'DD/MM/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_Sem2', serialize_value(result[0][0]))

def FechaFin_Sem(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)) + 6, 'MM/DD/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_Sem', serialize_value(result[0][0]))

def FechafinS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	FechaFin_Sem = get_variable('FechaFin_Sem')

	sql_query = f'''SELECT '{FechaFin_Sem}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechafinS', serialize_value(result[0][0]))

def FechaFileS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	FechafinS = get_variable('FechafinS')

	sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechafinS}', 'MM/DD/YY'), 'YYMMDD');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFileS', serialize_value(result[0][0]))

def FileCodSupervisado(**kwargs):
	value = '01410'
	Variable.set('FileCodSupervisado', serialize_value(value))

def FileAT_at11(**kwargs):
	value = 'AT11'
	Variable.set('FileAT_at11', serialize_value(value))

def Vcorte(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')
	sql_query = '''SELECT co_corte
				FROM ods.cb_corte
				WHERE co_fecha_ini::date = (SELECT TO_DATE(diahabilant2, 'DD/MM/YY') FROM ods.validardiahabil) AND co_empresa = '1';'''
	result = hook.get_records(sql_query)
	if not result:
		# La consulta no devolvió ninguna fila → definimos un comportamiento
		Variable.set('Vcorte', serialize_value(0))
		return

	vcorte = result[0][0]
	Variable.set('Vcorte', serialize_value(vcorte))

def Vperiodo(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')
	sql_query = '''SELECT co_periodo
				FROM ods.cb_corte
				WHERE co_fecha_ini::date = (SELECT TO_DATE(diahabilant2, 'DD/MM/YY') FROM ods.validardiahabil) AND co_empresa = '1';'''
	result = hook.get_records(sql_query)
	if not result:
		# La consulta no devolvió ninguna fila → definimos un comportamiento
		Variable.set('Vperiodo', serialize_value(0))
		return

	vperiodo = result[0][0]
	Variable.set('Vperiodo', serialize_value(vperiodo))

def AT11_CB_COTIZACION(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	FechaFin_Sem2 = get_variable('FechaFin_Sem2')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT11_CB_COTIZACION (
	CT_EMPRESA NUMERIC(3),
	CT_MONEDA NUMERIC(3),
	CT_FECHA DATE,
	CT_VALOR NUMERIC(32,7),
	MO_ABREVIATURA CHAR(3)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT11_CB_COTIZACION;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.AT11_CB_COTIZACION (
	CT_EMPRESA, 
	CT_MONEDA, 
	CT_FECHA, 
	CT_VALOR, 
	MO_ABREVIATURA
	)
	SELECT
		cb_cotizacion.ct_empresa,
		cb_cotizacion.ct_moneda,
		cb_cotizacion.ct_fecha,
		cb_cotizacion.ct_valor,
		cl_moneda.mo_abreviatura
	FROM ods.cb_cotizacion AS cb_cotizacion INNER JOIN ods.cl_moneda AS cl_moneda ON (cb_cotizacion.ct_moneda = cl_moneda.mo_moneda)
	WHERE 
		cb_cotizacion.ct_fecha = TO_DATE('{FechaFin_Sem2}', 'DD/MM/YYYY') - INTERVAL '1 day';'''
	hook.run(sql_query_deftxt)

def AT11_SALDOS_CUENTAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	FechaFin_Sem2 = get_variable('FechaFin_Sem2')
	Vcorte = get_variable('Vcorte')
	Vperiodo = get_variable('Vperiodo')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at11_saldos_cuentas (
	tipoclasificacion	VARCHAR(2),
	hi_cuenta	VARCHAR(20),
	instrumentoinversion	VARCHAR(5),
	hi_saldo_me	NUMERIC(32,7),
	cu_moneda	NUMERIC(3),
	tipocambio	VARCHAR(10),
	codigoemisor	VARCHAR(2),
	codigocustodia	VARCHAR(2),
	cu_nombre	VARCHAR(80),
	activosubyacente	VARCHAR(2),
	valornominalactivo	VARCHAR(10),
	monedaactivosub	VARCHAR(2),
	tipoccactivosub	VARCHAR(15),
	tasacactivosub	VARCHAR(10),
	fechavencactivosub	VARCHAR(12),
	hi_oficina	NUMERIC(10),
	hi_area	NUMERIC(10),
	hi_corte	NUMERIC(10),
	hi_periodo	NUMERIC(10),
	codigoemisoractivo	VARCHAR(2),
	codigocustodioactivo	VARCHAR(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE at_stg.at11_saldos_cuentas;'''
	hook.run(sql_query_deftxt)

	# INSERTAMOS DATOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at11_saldos_cuentas (
	hi_cuenta,
	hi_saldo_me,
	cu_moneda,
	cu_nombre,
	hi_oficina,
	hi_area,
	hi_corte,
	hi_periodo,
	tipoclasificacion,
	instrumentoinversion,
	tipocambio,
	codigoemisor,
	codigocustodia,
	activosubyacente,
	valornominalactivo,
	monedaactivosub,
	tipoccactivosub,
	tasacactivosub,
	fechavencactivosub,
	codigoemisoractivo,
	codigocustodioactivo
	)
	SELECT
		CB_HIST_SALDO_GEN.hi_cuenta,
		CB_HIST_SALDO_GEN.hi_saldo_me,
		CB_CUENTA.cu_moneda,
		CB_CUENTA.cu_nombre,
		CB_HIST_SALDO_GEN.hi_oficina,
		CB_HIST_SALDO_GEN.hi_area,
		CB_HIST_SALDO_GEN.hi_corte,
		CB_HIST_SALDO_GEN.hi_periodo,
		'1',
		'78',
		'0.0000',
		'0',
		'0',
		'0',
		'0.0000',
		'0',
		'0.00000000',
		'0.0000',
		'19000101',
		'0',
		'0'
	FROM ods.cb_hist_saldo_gen AS cb_hist_saldo_gen INNER JOIN ods.cb_cuenta AS cb_cuenta ON (cb_hist_saldo_gen.hi_cuenta = cb_cuenta.cu_cuenta)
	WHERE 
		(CB_CUENTA.CU_ESTADO IN ('V','C'))
		AND (CB_HIST_SALDO_GEN.HI_SALDO <> 0)
		AND (CB_HIST_SALDO_GEN.HI_SALDO_ME <> 0)
		AND (CB_HIST_SALDO_GEN.HI_OFICINA >= 0)
		AND (CB_HIST_SALDO_GEN.HI_EMPRESA = 1)
		AND (CB_HIST_SALDO_GEN.HI_CORTE = '{Vcorte}')
		AND (CB_CUENTA.FECHACARGA = TO_CHAR(TO_DATE('{FechaFin_Sem2}', 'DD/MM/YYYY') - INTERVAL '1 day', 'DDMM'))
		AND (CB_CUENTA.CU_MONEDA <> 0)
		AND (SUBSTRING(CB_HIST_SALDO_GEN.HI_CUENTA FROM 1 FOR 3) = '114')
		AND (CB_HIST_SALDO_GEN.HI_PERIODO = '{Vperiodo}');'''
	hook.run(sql_query_deftxt)

def AT11_ALL(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	# creamos tabla destino 
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at11_all (
	TIPOCLASIFICACION	VARCHAR(2),
	HI_CUENTA	VARCHAR(20),
	INSTRUMENTOINVERSION	VARCHAR(5),
	HI_SALDO_ME	NUMERIC(32,7),
	MO_ABREVIATURA	CHAR(3),
	CT_VALOR	NUMERIC(32,7),
	CODIGOEMISOR	VARCHAR(2),
	CODIGOCUSTODIA	VARCHAR(2),
	CU_NOMBRE	VARCHAR(80),
	ACTIVOSUBYACENTE	VARCHAR(2),
	VALORNOMINALACTIVO	VARCHAR(10),
	MONEDAACTIVOSUB	VARCHAR(2),
	TIPOCCACTIVOSUB	VARCHAR(15),
	CODIGOEMISORACTIVO	VARCHAR(2),
	CODIGOCUSTODIOACTIVO	VARCHAR(2),
	TASACACTIVOSUB	VARCHAR(10),
	FECHAVENCACTIVOSUB	VARCHAR(12),
	HI_OFICINA	NUMERIC(10),
	HI_AREA	NUMERIC(10),
	HI_CORTE	NUMERIC(10),
	HI_PERIODO	NUMERIC(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT11_ALL;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at11_all (
	TIPOCLASIFICACION,
	HI_CUENTA,
	INSTRUMENTOINVERSION,
	HI_SALDO_ME,
	MO_ABREVIATURA,
	CT_VALOR,
	CODIGOEMISOR,
	CODIGOCUSTODIA,
	CU_NOMBRE,
	ACTIVOSUBYACENTE,
	VALORNOMINALACTIVO,
	MONEDAACTIVOSUB,
	TIPOCCACTIVOSUB,
	CODIGOEMISORACTIVO,
	CODIGOCUSTODIOACTIVO,
	TASACACTIVOSUB,
	FECHAVENCACTIVOSUB,
	HI_OFICINA,
	HI_AREA,
	HI_CORTE,
	HI_PERIODO
	)
	SELECT 
		AT11_SALDOS_CUENTAS.TIPOCLASIFICACION,
		SUBSTRING(AT11_SALDOS_CUENTAS.HI_CUENTA FROM 1 FOR 6),
		AT11_SALDOS_CUENTAS.INSTRUMENTOINVERSION,
		AT11_SALDOS_CUENTAS.HI_SALDO_ME,
		AT11_CB_COTIZACION.MO_ABREVIATURA,
		AT11_CB_COTIZACION.CT_VALOR,
		AT11_SALDOS_CUENTAS.CODIGOEMISOR,
		AT11_SALDOS_CUENTAS.CODIGOCUSTODIA,
		AT11_SALDOS_CUENTAS.CU_NOMBRE,
		AT11_SALDOS_CUENTAS.ACTIVOSUBYACENTE,
		AT11_SALDOS_CUENTAS.VALORNOMINALACTIVO,
		AT11_SALDOS_CUENTAS.MONEDAACTIVOSUB,
		AT11_SALDOS_CUENTAS.TIPOCCACTIVOSUB,
		AT11_SALDOS_CUENTAS.CODIGOEMISORACTIVO,
		AT11_SALDOS_CUENTAS.CODIGOCUSTODIOACTIVO,
		AT11_SALDOS_CUENTAS.TASACACTIVOSUB,
		AT11_SALDOS_CUENTAS.FECHAVENCACTIVOSUB,
		AT11_SALDOS_CUENTAS.HI_OFICINA,
		AT11_SALDOS_CUENTAS.HI_AREA,
		AT11_SALDOS_CUENTAS.HI_CORTE,
		AT11_SALDOS_CUENTAS.HI_PERIODO
	FROM at_stg.at11_cb_cotizacion AS AT11_CB_COTIZACION INNER JOIN at_stg.at11_saldos_cuentas AS AT11_SALDOS_CUENTAS
		ON (AT11_CB_COTIZACION.CT_MONEDA = AT11_SALDOS_CUENTAS.CU_MONEDA);'''
	hook.run(sql_query_deftxt)

def AT11_FINAL(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT11_FINAL (
	TIPOCLASIFICACION VARCHAR(2),
	HI_CUENTA VARCHAR(20),
	INSTRUMENTOINVERSION VARCHAR(5),
	HI_SALDO_ME VARCHAR(36),
	MO_ABREVIATURA CHAR(3),
	CT_VALOR VARCHAR(36),
	CODIGOEMISOR VARCHAR(2),
	CODIGOCUSTODIA VARCHAR(2),
	CU_NOMBRE VARCHAR(80),
	ACTIVOSUBYACENTE VARCHAR(2),
	VALORNOMINALACTIVO VARCHAR(10),
	MONEDAACTIVOSUB VARCHAR(2),
	TIPOCCACTIVOSUB VARCHAR(15),
	CODIGOEMISORACTIVO VARCHAR(2),
	CODIGOCUSTODIOACTIVO VARCHAR(2),
	TASACACTIVOSUB VARCHAR(10),
	FECHAVENCACTIVOSUB VARCHAR(12),
	HI_OFICINA NUMERIC(10),
	HI_AREA NUMERIC(10),
	HI_CORTE NUMERIC(10),
	HI_PERIODO NUMERIC(10),
	FECHACARGA DATE
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT11_FINAL;'''
	hook.run(sql_query_deftxt)

	# INSERTAMOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO atsudeban.at11_final (
	TIPOCLASIFICACION,
	HI_CUENTA,
	INSTRUMENTOINVERSION,
	HI_SALDO_ME,
	MO_ABREVIATURA,
	CT_VALOR,
	CODIGOEMISOR,
	CODIGOCUSTODIA,
	CU_NOMBRE,
	ACTIVOSUBYACENTE,
	VALORNOMINALACTIVO,
	MONEDAACTIVOSUB,
	TIPOCCACTIVOSUB,
	CODIGOEMISORACTIVO,
	CODIGOCUSTODIOACTIVO,
	TASACACTIVOSUB,
	FECHAVENCACTIVOSUB,
	HI_OFICINA,
	HI_AREA,
	HI_CORTE,
	HI_PERIODO,
	FECHACARGA
	)
	SELECT
		AT11_ALL.TIPOCLASIFICACION,
		RPAD(AT11_ALL.HI_CUENTA, 10, '0'),
		AT11_ALL.INSTRUMENTOINVERSION,
		replace(trim(to_char(AT11_ALL.hi_saldo_me, 'FM99999999999990D0000')), '.', ','),
		AT11_ALL.MO_ABREVIATURA,
		replace(trim(to_char(AT11_ALL.CT_VALOR, 'FM99999999999990D0000')), '.', ','),
		AT11_ALL.CODIGOEMISOR,
		AT11_ALL.CODIGOCUSTODIA,
		AT11_ALL.CU_NOMBRE,
		AT11_ALL.ACTIVOSUBYACENTE,
		AT11_ALL.VALORNOMINALACTIVO,
		AT11_ALL.MONEDAACTIVOSUB,
		AT11_ALL.TIPOCCACTIVOSUB,
		AT11_ALL.CODIGOEMISORACTIVO,
		AT11_ALL.CODIGOCUSTODIOACTIVO,
		AT11_ALL.TASACACTIVOSUB,
		AT11_ALL.FECHAVENCACTIVOSUB,
		AT11_ALL.HI_OFICINA,
		AT11_ALL.HI_AREA,
		AT11_ALL.HI_CORTE,
		AT11_ALL.HI_PERIODO,
		CURRENT_DATE AS FECHACARGA
	FROM AT_STG.AT11_ALL AS AT11_ALL;'''
	hook.run(sql_query_deftxt)

	# Borramos tablas TEMPORALES
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT11_CB_COTIZACION;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT11_SALDOS_CUENTAS;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT11_ALL;'''
	#hook.run(sql_query)

def ELIMINAR_REGISTROS_DIARIOS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	sql_query_deftxt = '''DELETE FROM ATSUDEBAN.AT11_FINAL WHERE FECHACARGA >= CURRENT_DATE;'''
	hook.run(sql_query_deftxt)

def AT11_TO_FILE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at11')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS FILE_AT.AT11_SALIDA (
	TIPOCLASIFICACION VARCHAR(2),
	HI_CUENTA VARCHAR(20),
	INSTRUMENTOINVERSION VARCHAR(5),
	HI_SALDO_ME VARCHAR(36),
	MO_ABREVIATURA VARCHAR(3),
	CT_VALOR VARCHAR(36),
	CODIGOEMISOR VARCHAR(2),
	CODIGOCUSTODIA VARCHAR(2),
	CU_NOMBRE VARCHAR(80),
	ACTIVOSUBYACENTE VARCHAR(2),
	VALORNOMINALACTIVO VARCHAR(10),
	MONEDAACTIVOSUB VARCHAR(2),
	TIPOCCACTIVOSUB VARCHAR(15),
	CODIGOEMISORACTIVO VARCHAR(2),
	CODIGOCUSTODIOACTIVO VARCHAR(2),
	TASACACTIVOSUB VARCHAR(10),
	FECHAVENCACTIVOSUB VARCHAR(12)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.AT11_SALIDA;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO FILE_AT.AT11_SALIDA (
	TIPOCLASIFICACION,
	HI_CUENTA,
	INSTRUMENTOINVERSION,
	HI_SALDO_ME,
	MO_ABREVIATURA,
	CT_VALOR,
	CODIGOEMISOR,
	CODIGOCUSTODIA,
	CU_NOMBRE,
	ACTIVOSUBYACENTE,
	VALORNOMINALACTIVO,
	MONEDAACTIVOSUB,
	TIPOCCACTIVOSUB,
	CODIGOEMISORACTIVO,
	CODIGOCUSTODIOACTIVO,
	TASACACTIVOSUB,
	FECHAVENCACTIVOSUB
	) 
	SELECT
		AT11_FINAL.TIPOCLASIFICACION,
		AT11_FINAL.HI_CUENTA,
		AT11_FINAL.INSTRUMENTOINVERSION,
		AT11_FINAL.HI_SALDO_ME,
		AT11_FINAL.MO_ABREVIATURA,
		AT11_FINAL.CT_VALOR,
		AT11_FINAL.CODIGOEMISOR,
		AT11_FINAL.CODIGOCUSTODIA,
		AT11_FINAL.CU_NOMBRE,
		AT11_FINAL.ACTIVOSUBYACENTE,
		AT11_FINAL.VALORNOMINALACTIVO,
		AT11_FINAL.MONEDAACTIVOSUB,
		AT11_FINAL.TIPOCCACTIVOSUB,
		AT11_FINAL.CODIGOEMISORACTIVO,
		AT11_FINAL.CODIGOCUSTODIOACTIVO,
		AT11_FINAL.TASACACTIVOSUB,
		AT11_FINAL.FECHAVENCACTIVOSUB
	FROM ATSUDEBAN.AT11_FINAL AS AT11_FINAL
	WHERE (AT11_FINAL.FECHACARGA >= CURRENT_DATE)
	ORDER BY HI_CUENTA;'''
	hook.run(sql_query_deftxt)

def AT11_SALIDA_TOTXT(**kwargs):
	# Conexion a la bd
	hook = PostgresHook(postgres_conn_id='at11')
	gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

	# Recuperar las variables definidas en las tareas previas
	FileAT = get_variable('FileAT_at11')
	FileCodSupervisado = get_variable('FileCodSupervisado')
	FechaFileS = get_variable('FechaFileS')

	# Generar txt
	logger.info("Obteniendo registros de la base de datos...")
	registros = hook.get_records('SELECT * FROM FILE_AT.AT11_SALIDA;')
	logger.info(f"Se obtuvieron {len(registros)} registros.")

	# Definir la ruta del archivo de salida en GCS
	gcs_bucket = 'airflow-dags-data'
	gcs_object_path = f"data/AT11/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFileS}.txt"
	
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

dag = DAG(dag_id='AT11',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

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

FileAT_task = PythonOperator(
	task_id='FileAT_task',
	python_callable=FileAT_at11,
	dag=dag
)

Vcorte_task = PythonOperator(
	task_id='Vcorte_task',
	python_callable=Vcorte,
	dag=dag
)

Vperiodo_task = PythonOperator(
	task_id='Vperiodo_task',
	python_callable=Vperiodo,
	dag=dag
)

AT11_CB_COTIZACION_task = PythonOperator(
	task_id='AT11_CB_COTIZACION_task',
	python_callable=AT11_CB_COTIZACION,
	dag=dag
)

AT11_SALDOS_CUENTAS_task = PythonOperator(
	task_id='AT11_SALDOS_CUENTAS_task',
	python_callable=AT11_SALDOS_CUENTAS,
	dag=dag
)

AT11_ALL_task = PythonOperator(
	task_id='AT11_ALL_task',
	python_callable=AT11_ALL,
	dag=dag
)

AT11_FINAL_task = PythonOperator(
	task_id='AT11_FINAL_task',
	python_callable=AT11_FINAL,
	dag=dag
)

ELIMINAR_REGISTROS_DIARIOS_task = PythonOperator(
	task_id='ELIMINAR_REGISTROS_DIARIOS_task',
	python_callable=ELIMINAR_REGISTROS_DIARIOS,
	dag=dag
)

AT11_TO_FILE_task = PythonOperator(
	task_id='AT11_TO_FILE_task',
	python_callable=AT11_TO_FILE,
	dag=dag
)

AT11_SALIDA_TOTXT_task = PythonOperator(
	task_id='AT11_SALIDA_TOTXT_task',
	python_callable=AT11_SALIDA_TOTXT,
	dag=dag
)

Enviar_Email_task = EmailOperator(
	task_id='Enviar_Email_task',
	to='colocar_correo_aqui@gmail.com',          # correo destino
	subject='DAG {{ dag.dag_id }} completado',   # asunto del correo
	html_content="""                             
		<h3>¡Hola!</h3>
		<p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at11 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFileS }}.txt</p>
	""",
	conn_id="email_conn",
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
FechaFin_Sem2_task >> FechaFin_Sem_task >> FechafinS_task >> FechaFileS_task >> FileCodSupervisado_task >> FileAT_task >> Vcorte_task >> Vperiodo_task >> AT11_CB_COTIZACION_task >> AT11_SALDOS_CUENTAS_task >> AT11_ALL_task >> AT11_FINAL_task >> ELIMINAR_REGISTROS_DIARIOS_task >> AT11_TO_FILE_task >> AT11_SALIDA_TOTXT_task >> Enviar_Email_task
