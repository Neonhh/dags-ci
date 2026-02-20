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

def FechaInicio_Sem(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)), 'MM/DD/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio_Sem', serialize_value(result[0][0]))

def FechaFin_Sem(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	sql_query = '''SELECT to_char(current_date - (7 + (to_char(current_date - INTERVAL '7 days', 'D')::integer - 1)) + 6, 'MM/DD/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_Sem', serialize_value(result[0][0]))

def FechainicioS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechaInicio_Sem = get_variable('FechaInicio_Sem')

	sql_query = f'''SELECT '{FechaInicio_Sem}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechainicioS', serialize_value(result[0][0]))

def FechafinS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechaFin_Sem = get_variable('FechaFin_Sem')

	sql_query = f'''SELECT '{FechaFin_Sem}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechafinS', serialize_value(result[0][0]))

def FechaFileS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')

	sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechafinS}', 'MM/DD/YY'), 'YYMMDD');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFileS', serialize_value(result[0][0]))

def FileAT_at37(**kwargs):
	value = 'AT37'
	Variable.set('FileAT_at37', serialize_value(value))

def AT37_IPATM(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	sql_query = '''SELECT valor FROM ods.ats_tm_parametros WHERE (clave = 'IPATMT37' AND clasificacion = 'AT37');'''
	result = hook.get_records(sql_query)
	Variable.set('AT37_IPATM', serialize_value(result[0][0]))

def AT37_MONTOBS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	sql_query = '''SELECT CAST(VALOR AS NUMERIC) FROM ods.ats_tm_parametros WHERE (CLAVE = 'MONTOAT37' AND CLASIFICACION = 'AT37');'''
	result = hook.get_records(sql_query)
	Variable.set('AT37_MONTOBS', serialize_value(result[0][0]))

def Moneda_Vzla(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	sql_query = '''SELECT valor FROM ods.ats_tm_parametros WHERE (clave = 'MONEDA' AND clasificacion = 'AT');'''
	result = hook.get_records(sql_query)
	Variable.set('Moneda_Vzla', serialize_value(result[0][0]))

def FileCodSupervisado(**kwargs):
	value = '01410'
	Variable.set('FileCodSupervisado', serialize_value(value))

def FileDate(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
	result = hook.get_records(sql_query)
	Variable.set('FileDate', serialize_value(result[0][0]))

def ELIMINA_DUPLICADOS_BC(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_BC_TOTAL_FIN (
	tipotransferencia	NUMERIC(38),
	clasificacion	NUMERIC(38),
	tipocliente	VARCHAR(3),
	identificacioncliente	VARCHAR(15),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	VARCHAR(5),
	cuentacliente	CHAR(20),
	moneda	CHAR(3),
	monto	NUMERIC(38,2),
	fecha	CHAR(8),
	referencia	VARCHAR(30),
	empresaformacion	VARCHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	direccionip_i	VARCHAR(50),
	tipocontraparte	VARCHAR(3),
	identificacioncontraparte	VARCHAR(30),
	nombrecontraparte	VARCHAR(100),
	tipoinstrumentocontraparte	VARCHAR(5),
	cuentacontraparte	VARCHAR(20),
	oficina	NUMERIC(38),
	ubicacion_ip	VARCHAR(2)
	);'''
	hook.run(sql_query_deftxt)

	# truncar
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_BC_TOTAL_FIN;'''
	hook.run(sql_query_deftxt)

	# insertar en destino
	sql_query_deftxt = '''INSERT INTO atsudeban.at37_bc_total_fin (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	moneda,
	monto,
	fecha,
	referencia,
	empresaformacion,
	motivo,
	direccionip,
	direccionip_i,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	oficina,
	ubicacion_ip
	)
	SELECT
		t1.tipotransferencia,
		t1.clasificacion,
		t1.tipocliente,
		t1.identificacioncliente,
		t1.nombrecliente,
		t1.tipoinstrumento,
		t1.cuentacliente,
		t1.moneda,
		t1.monto,
		t1.fecha,
		t1.referencia,
		t1.empresaformacion,
		t1.motivo,
		t1.direccionip,
		t1.direccionip_i,
		t1.tipocontraparte,
		t1.identificacioncontraparte,
		t1.nombrecontraparte,
		t1.tipoinstrumentocontraparte,
		t1.cuentacontraparte,
		t1.oficina,
		t1.ubicacion_ip
	FROM atsudeban.at37_bc_total AS t1
	INNER JOIN (
		SELECT 
			t3.identificacioncliente, 
			t3.referencia, 
			t3.fecha, 
			t3.monto,
			MIN(t3.direccionip) AS direccionip
		FROM atsudeban.at37_bc_total AS t3
		GROUP BY 
			t3.identificacioncliente, 
			t3.referencia, 
			t3.fecha, 
			t3.monto
	) AS t2 
	ON t1.identificacioncliente = t2.identificacioncliente AND t1.referencia = t2.referencia AND t1.fecha = t2.fecha AND t1.direccionip = t2.direccionip AND t1.monto = t2.monto;'''
	hook.run(sql_query_deftxt)

def AT37_ATSS_DATA_CHECK_TO_E(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.E$_AT37_ATSS  (
	TIPOTRANSFERENCIA NUMERIC,
	CLASIFICACION NUMERIC,
	TIPOCLIENTE VARCHAR(3),
	IDENTIFICACIONCLIENTE VARCHAR(15),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO VARCHAR(5),
	CUENTACLIENTE CHAR(20),
	MONEDA CHAR(3),
	MONTO NUMERIC,
	FECHA CHAR(8),
	REFERENCIA VARCHAR(20),
	EMPRESAFORMACION VARCHAR(3),
	MOTIVO VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# Vaciamos la tabla destino antes de la carga
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.E$_AT37_ATSS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.E$_AT37_ATSS (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	moneda,
	monto,
	fecha,
	referencia,
	empresaformacion,
	motivo,
	direccionip,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	oficina
	)
	SELECT	
		tipotransferencia,
		clasificacion,
		tipocliente,
		identificacioncliente,
		nombrecliente,
		tipoinstrumento,
		cuentacliente,
		moneda,
		monto,
		fecha,
		referencia,
		empresaformacion,
		motivo,
		direccionip,
		tipocontraparte,
		identificacioncontraparte,
		nombrecontraparte,
		tipoinstrumentocontraparte,
		cuentacontraparte,
		oficina
	FROM ATSUDEBAN.AT37_ATSS
	WHERE NOT (

		AT37_ATSS.TIPOCONTRAPARTE IN ('V','E','J','G','P','R','C')
		
		AND (
			AT37_ATSS.CLASIFICACION = 1 AND AT37_ATSS.IDENTIFICACIONCONTRAPARTE <> AT37_ATSS.IDENTIFICACIONCLIENTE OR AT37_ATSS.CLASIFICACION = 2
		)
		AND (
			(AT37_ATSS.TIPOCLIENTE = 'V' AND AT37_ATSS.IDENTIFICACIONCLIENTE BETWEEN 1 AND 40000000)
			OR
			(AT37_ATSS.TIPOCLIENTE = 'E' AND AT37_ATSS.IDENTIFICACIONCLIENTE BETWEEN 1 AND 1500000)
			OR
			(AT37_ATSS.TIPOCLIENTE = 'E' AND AT37_ATSS.IDENTIFICACIONCLIENTE BETWEEN 80000000 AND 100000000)
			OR
			AT37_ATSS.TIPOCLIENTE NOT IN ('V','E')
		)
		AND (
			AT37_ATSS.TIPOCLIENTE IN ('E','G','J','P','V','R','I','C')
		)
		AND (
			AT37_ATSS.TIPOINSTRUMENTO IN ('1','2','13','20','14')
		)
		AND (
			AT37_ATSS.TIPOINSTRUMENTO IN ('1','2') AND AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE IN ('1','2','0') AND AT37_ATSS.MONEDA = 'VES'
			OR
			AT37_ATSS.TIPOINSTRUMENTO IN ('13','14') AND AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE IN ('13','14') AND AT37_ATSS.MONEDA <> 'VES'
		)
		AND (
			AT37_ATSS.MONEDA = 'VES' AND AT37_ATSS.MONTO >= 100 OR AT37_ATSS.MONEDA <> 'VES' AND AT37_ATSS.MONTO > 0
		)
		AND (
			AT37_ATSS.TIPOCLIENTE IN ('V','E','P') AND AT37_ATSS.EMPRESAFORMACION IN ('0','1','2')
			OR
			AT37_ATSS.TIPOCLIENTE NOT IN ('V','E','P') AND AT37_ATSS.EMPRESAFORMACION IN ('0')
		)
		AND (
			AT37_ATSS.TIPOTRANSFERENCIA = 1 AND AT37_ATSS.DIRECCIONIP <> '0'
			OR
			AT37_ATSS.TIPOTRANSFERENCIA <> 1 AND AT37_ATSS.DIRECCIONIP = '0'
		)
		AND (
			(AT37_ATSS.NOMBRECLIENTE IS NOT NULL) AND (AT37_ATSS.NOMBRECLIENTE <> ' ') AND (AT37_ATSS.NOMBRECLIENTE <> '0') AND (AT37_ATSS.NOMBRECLIENTE <> '')
		)
		AND (
			AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE IN ('0','1','2','13','14')
		)
			
	);'''
	hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT37_PRINCIPAL',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

FechaInicio_Sem_task = PythonOperator(
	task_id='FechaInicio_Sem_task',
	python_callable=FechaInicio_Sem,
	dag=dag
)

FechaFin_Sem_task = PythonOperator(
	task_id='FechaFin_Sem_task',
	python_callable=FechaFin_Sem,
	dag=dag
)

FechainicioS_task = PythonOperator(
	task_id='FechainicioS_task',
	python_callable=FechainicioS,
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

FileAT_task = PythonOperator(
	task_id='FileAT_task',
	python_callable=FileAT_at37,
	dag=dag
)

AT37_IPATM_task = PythonOperator(
	task_id='AT37_IPATM_task',
	python_callable=AT37_IPATM,
	dag=dag
)

AT37_MONTOBS_task = PythonOperator(
	task_id='AT37_MONTOBS_task',
	python_callable=AT37_MONTOBS,
	dag=dag
)

Moneda_Vzla_task = PythonOperator(
	task_id='Moneda_Vzla_task',
	python_callable=Moneda_Vzla,
	dag=dag
)

FileCodSupervisado_task = PythonOperator(
	task_id='FileCodSupervisado_task',
	python_callable=FileCodSupervisado,
	dag=dag
)

FileDate_task = PythonOperator(
	task_id='FileDate_task',
	python_callable=FileDate,
	dag=dag
)

Execution_of_the_Scenario_AT37_version_002_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT37_version_002_task',
	trigger_dag_id='AT37',  
	wait_for_completion=True,
	dag=dag
)

ELIMINA_DUPLICADOS_BC_task = PythonOperator(
	task_id='ELIMINA_DUPLICADOS_BC_task',
	python_callable=ELIMINA_DUPLICADOS_BC,
	dag=dag
)

Execution_of_the_Scenario_AT37_TO_FILE_version_002_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT37_TO_FILE_version_002_task',
	trigger_dag_id='AT37_TO_FILE',
	wait_for_completion=True,
	dag=dag
)

AT37_ATSS_DATA_CHECK_TO_E_task = PythonOperator(
	task_id='AT37_ATSS_DATA_CHECK_TO_E_task',
	python_callable=AT37_ATSS_DATA_CHECK_TO_E,
	dag=dag
)

Execution_of_the_Scenario_AT37_ATSUDEBAN_TOFILERROR_version_002_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT37_ATSUDEBAN_TOFILERROR_version_002_task',
	trigger_dag_id='AT37_ATSUDEBAN_TOFILERROR',
	wait_for_completion=True,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
FechaInicio_Sem_task >> FechaFin_Sem_task >> FechainicioS_task >> FechafinS_task >> FechaFileS_task >> FileAT_task >> AT37_IPATM_task >> AT37_MONTOBS_task >> Moneda_Vzla_task >> FileCodSupervisado_task >> FileDate_task >> Execution_of_the_Scenario_AT37_version_002_task >> ELIMINA_DUPLICADOS_BC_task >> Execution_of_the_Scenario_AT37_TO_FILE_version_002_task >> AT37_ATSS_DATA_CHECK_TO_E_task >> Execution_of_the_Scenario_AT37_ATSUDEBAN_TOFILERROR_version_002_task
