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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

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

def FileAT_at12(**kwargs):
	value = 'AT12'
	Variable.set('FileAT_at12', serialize_value(value))

def FechaInicio_M(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')
	sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days'), 'MM/DD/YYYY')'''
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio_M', serialize_value(result[0][0]))

def FechaFin_M(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')
	sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month - 1 day', 'mm/dd/yyyy');'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_M', serialize_value(result[0][0]))

def FechaFin(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	FechaFin_M = get_variable('FechaFin_M')

	sql_query = f'''SELECT '{FechaFin_M}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin', serialize_value(result[0][0]))

def FechaInicio(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	FechaInicio_M = get_variable('FechaInicio_M')

	sql_query = f'''SELECT '{FechaInicio_M}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio', serialize_value(result[0][0]))

def FechaFile(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')
	
	FechaFin = get_variable('FechaFin')

	sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechaFin}', 'MM/DD/YY'), 'YYMMDD') AS result;'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFile', serialize_value(result[0][0]))

def FileCodSupervisado(**kwargs):
	value = '01410'
	Variable.set('FileCodSupervisado', serialize_value(value))

def FileDate(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')
	sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
	result = hook.get_records(sql_query)
	Variable.set('FileDate', serialize_value(result[0][0]))

def AT12_FileName_TDD(**kwargs):
	FileAT = get_variable('FileAT_at12')
	FileCodSupervisado = get_variable('FileCodSupervisado')
	FechaFile = get_variable('FechaFile')

	value = f'{FileAT}{FileCodSupervisado}{FechaFile}_TDD.txt'
	Variable.set('AT12_FileName_TDD', serialize_value(value))


def ATS_TH_AT12_DATA_CHECK_TO_E(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.E$_ATS_TH_AT12  (
	PAISCONSUMO VARCHAR(2),
	MUNICIPIO VARCHAR(6),
	EXTRANJERA NUMERIC(2),
	CLASETARJETA NUMERIC(2),
	TIPOTARJETA NUMERIC(2),
	CONCEPTO NUMERIC(2),
	FRANQUICIA NUMERIC(2),
	CANTIDADCONSUMOS NUMERIC(10),
	MONTOCONSUMO NUMERIC(32,2),
	COMISIONESPROPIAS NUMERIC(32,2),
	COMISIONESTERCEROS NUMERIC(32,2),
	RED NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# Vaciamos la tabla destino antes de la carga
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.E$_ATS_TH_AT12;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.E$_ATS_TH_AT12 (
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
	FROM ATSUDEBAN.ATS_TH_AT12
	WHERE NOT (

		(EXTRANJERA <> 2 OR PAISCONSUMO = 'VE')
		AND (EXTRANJERA = 2 OR PAISCONSUMO <> '0')
		AND (PAISCONSUMO <> 'VE' OR MUNICIPIO <> '0')
		AND (EXTRANJERA <> 0)
		AND (EXTRANJERA <> 2 OR CLASETARJETA <> 1 OR TIPOTARJETA <> 0)
		AND (EXTRANJERA = 2 OR CLASETARJETA <> 1 OR  TIPOTARJETA NOT IN (0,5))
		AND (CLASETARJETA = 2 OR TIPOTARJETA <> 0)
		AND (CLASETARJETA <> 1 OR CONCEPTO <> 18)
		AND (CLASETARJETA <> 2 OR CONCEPTO <> 9)
		AND (CLASETARJETA <> 1 OR FRANQUICIA <> 0)
		AND (CLASETARJETA = 1 OR FRANQUICIA = 0)
		AND (CANTIDADCONSUMOS >= 0)
		AND (COMISIONESPROPIAS >= 0)
		AND (MONTOCONSUMO >= 0)
		AND (COMISIONESTERCEROS >= 0)	
	);'''
	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT12_PRINCIPAL',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_file',
    bucket='airflow-dags-data', 
    object='data/AT12/INSUMOS/PMPFP301TT.PMPFP301TT',          
    poke_interval=10,
    timeout=60 * 10,
    dag=dag
)

FileAT_task = PythonOperator(
	task_id='FileAT_task',
	python_callable=FileAT_at12,
	dag=dag
)

FechaInicio_M_task = PythonOperator(
    task_id='FechaInicio_M_task',
    python_callable=FechaInicio_M,
    dag=dag
)

FechaFin_M_task = PythonOperator(
    task_id='FechaFin_M_task',
    python_callable=FechaFin_M,
    dag=dag
)

FechaFin_task = PythonOperator(
	task_id='FechaFin_task',
	python_callable=FechaFin,
	dag=dag
)

FechaInicio_task = PythonOperator(
	task_id='FechaInicio_task',
	python_callable=FechaInicio,
	dag=dag
)

FechaFile_task = PythonOperator(
	task_id='FechaFile_task',
	python_callable=FechaFile,
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

Execution_of_the_Scenario_AT12_version_001_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT12_version_001_task',
	trigger_dag_id='AT12', 
	wait_for_completion=True,
	dag=dag
)

AT12_FileName_TDD_task = PythonOperator(
	task_id='AT12_FileName_TDD_task',
	python_callable=AT12_FileName_TDD,
	dag=dag
)

Execution_of_the_Scenario_AT12_TO_FILE_version_001_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT12_TO_FILE_version_001_task',
	trigger_dag_id='AT12_TO_FILE', 
	wait_for_completion=True,
	dag=dag
)

ATS_TH_AT12_DATA_CHECK_TO_E_task = PythonOperator(
	task_id='ATS_TH_AT12_DATA_CHECK_TO_E_task',
	python_callable=ATS_TH_AT12_DATA_CHECK_TO_E,
	dag=dag
)

Execution_of_the_Scenario_AT12_TO_FILE_ERROR_version_001_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT12_TO_FILE_ERROR_version_001_task',
	trigger_dag_id='AT12_TO_FILE_ERROR',
	wait_for_completion=True,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
wait_for_file >> FileAT_task >> FechaInicio_M_task >> FechaFin_M_task >> FechaFin_task >> FechaInicio_task >> FechaFile_task >> FileCodSupervisado_task >> FileDate_task >> Execution_of_the_Scenario_AT12_version_001_task >> AT12_FileName_TDD_task >> Execution_of_the_Scenario_AT12_TO_FILE_version_001_task >> ATS_TH_AT12_DATA_CHECK_TO_E_task >> Execution_of_the_Scenario_AT12_TO_FILE_ERROR_version_001_task
