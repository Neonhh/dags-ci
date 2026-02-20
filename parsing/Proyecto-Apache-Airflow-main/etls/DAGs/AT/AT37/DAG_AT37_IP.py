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

def AT37_IP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	
	FechainicioS = get_variable('FechainicioS')
	FechafinS = get_variable('FechafinS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.ip (
	ip                     VARCHAR(20),
	tipocliente            VARCHAR(20),
	identificacioncliente  VARCHAR(20),
	fecha                  VARCHAR(20),
	fecha_hora             VARCHAR(25)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE at_stg.ip;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f'''insert into at_stg.ip ( 
	IP,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	FECHA,
	FECHA_HORA 
	)
	SELECT DISTINCT
		auditoria.ip,
		CASE
			WHEN substring(usuario.numdocumento FROM 1 FOR 1) IN ('E','G','I','J','P','R','V','M','F')
				THEN substring(usuario.numdocumento FROM 1 FOR 1)
			ELSE 'P'
		END AS tipocliente,
		CASE
			WHEN substring(usuario.numdocumento FROM 1 FOR 1) IN ('E','G','I','J','P','R','V','M','F')
				THEN substring(usuario.numdocumento FROM 2 FOR char_length(usuario.numdocumento) - 1)
			ELSE usuario.numdocumento
		END AS identificacioncliente,
		to_char(auditoria.fecha, 'DD/MM/YYYY'),
		to_char(auditoria.fecha, 'DD/MM/YYYY HH24:MI:SS')
	FROM ods.auditoria AS auditoria INNER JOIN ods.usuario AS usuario ON auditoria.numtarjetavirtual = usuario.numtarjetavirtual
	WHERE auditoria.codoperacion IN ('15','36')
		AND auditoria.fecha BETWEEN to_date('{FechainicioS}','MM/DD/YY') AND to_date('{FechafinS}','MM/DD/YY');'''
	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT37_IP',
		  default_args=default_args,
		  schedule=None,
		  catchup=False)

AT37_IP_task = PythonOperator(
	task_id='AT37_IP_task',
	python_callable=AT37_IP,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_IP_task