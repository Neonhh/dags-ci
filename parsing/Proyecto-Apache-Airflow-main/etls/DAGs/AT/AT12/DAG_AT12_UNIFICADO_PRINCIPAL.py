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

class HolidayCheckSensor(BaseSensorOperator):
	"""
	Sensor que espera hasta que el dia actual NO sea feriado.
	La condicion se determina ejecutando una consulta SQL en la base de datos.
	"""
	def __init__(self, postgres_conn_id, *args, **kwargs):
		super(HolidayCheckSensor, self).__init__(*args, **kwargs)
		self.postgres_conn_id = postgres_conn_id

	def poke(self, context):
		hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
		sql_query = """
			SELECT CASE 
					   WHEN to_char(CURRENT_DATE, 'dd/mm/yy') IN (
						   SELECT to_char(df_fecha, 'dd/mm/yy') 
						   FROM ods.cl_dias_feriados 
						   WHERE SUBSTRING(df_year FROM 3 FOR 2) = SUBSTRING(to_char(CURRENT_DATE, 'dd/mm/yy') FROM 7 FOR 2)
					   )
					   THEN 1 
					   ELSE 0 
				   END AS status;
		"""
		records = hook.get_records(sql_query)
		# Suponiendo que la consulta retorna 1 fila, 1 columna:
		status = records[0][0] if records else 0
		self.log.info("Valor de status (1=feriado, 0=no feriado): %s", status)
		# Esperamos que status sea 0 para continuar con el flujo normal
		return status == 0

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


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT12_UNIFICADO_PRINCIPAL',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

holiday_sensor = HolidayCheckSensor(     # En la secuencia de ejecucion, colocar este operador en el orden que convenga
	task_id='holiday_sensor',
	postgres_conn_id='at12', 
	poke_interval=10,                    # Se verifica cada 10 segundos para la prueba (para produccion seria 86400seg para una verificacion diaria)
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

Execution_of_the_Scenario_AT12_UNIFICADO_version_001_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT12_UNIFICADO_version_001_task',
	trigger_dag_id='AT12_UNIFICADO', 
	wait_for_completion=True,
	dag=dag
)

Execution_of_the_Scenario_AT12_UNIFICADO_TO_FILE_version_001_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT12_UNIFICADO_TO_FILE_version_001_task',
	trigger_dag_id='AT12_UNIFICADO_TO_FILE',
	wait_for_completion=True,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
holiday_sensor >> FileAT_task >> FechaInicio_M_task >> FechaFin_M_task >> FechaFin_task >> FechaInicio_task >> FechaFile_task >> FileCodSupervisado_task >> FileDate_task >> Execution_of_the_Scenario_AT12_UNIFICADO_version_001_task >> Execution_of_the_Scenario_AT12_UNIFICADO_TO_FILE_version_001_task
