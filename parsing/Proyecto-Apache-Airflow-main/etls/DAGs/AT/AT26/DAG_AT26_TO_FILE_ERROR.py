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

def FileName_Error(**kwargs):
	value = 'ErrorValid'
	Variable.set('FileName_Error', serialize_value(value))

def AT26_ATSUDEBAN_ERROR(**kwargs):
	
	# Conxion a la bd at26
	hook = PostgresHook(postgres_conn_id='repodataprd')
	
	# Creamos tabla destino 
	sql_query_deftxt = '''
	CREATE TABLE IF NOT EXISTS FILE_AT.ATS_TH_AT26_ERROR (
		NROFRAUDE           VARCHAR(20),
		CANALFRAUDE         VARCHAR(255),
		CODIGOCONTABLE      VARCHAR(20),
		CODIGOPARROQUIA     VARCHAR(8),
		GENERO              VARCHAR(50),
		IDCLIENTE           VARCHAR(50),
		FECHAFRAUDE         VARCHAR(8),
		MONTOFRAUDE         VARCHAR(15),
		MONTOFRAUDEINTERNO  VARCHAR(15),
		MONTOFRAUDEEXTERNO  VARCHAR(15),
		NRORECLAMO          VARCHAR(20),
		PENSIONADOIVSS      VARCHAR(50),
		RANGOEDAD           VARCHAR(50),
		RED                 VARCHAR(50),
		TIPOFRANQUICIA      VARCHAR(255),
		TIPOPERSONA         VARCHAR(50)
	);'''

	hook.run(sql_query_deftxt)
	
	# Vaciar la tabla (para no duplicar datos)
	hook.run("TRUNCATE TABLE FILE_AT.ATS_TH_AT26_ERROR;")

	# Insertar cada registro en la tabla FILE_AT.ATS_TH_AT26_ERROR
	sql_query_deftxt = '''
	INSERT INTO FILE_AT.ATS_TH_AT26_ERROR (
		NROFRAUDE,
		CANALFRAUDE,
		CODIGOCONTABLE,
		CODIGOPARROQUIA,
		GENERO,
		IDCLIENTE,
		FECHAFRAUDE,
		MONTOFRAUDE,
		MONTOFRAUDEINTERNO,
		MONTOFRAUDEEXTERNO,
		NRORECLAMO,
		PENSIONADOIVSS,
		RANGOEDAD,
		RED,
		TIPOFRANQUICIA,
		TIPOPERSONA
	)
	SELECT 
		NROFRAUDE,
		CANALFRAUDE,
		CODIGOCONTABLE,
		CODIGOPARROQUIA,
		GENERO,
		IDCLIENTE,
		FECHAFRAUDE,
		MONTOFRAUDE,
		MONTOFRAUDEINTERNO,
		MONTOFRAUDEEXTERNO,
		NRORECLAMO,
		PENSIONADOIVSS,
		RANGOEDAD,
		RED,
		TIPOFRANQUICIA,
		TIPOPERSONA
	FROM ATSUDEBAN.E$_ATS_TH_AT26;
	
	'''
	hook.run(sql_query_deftxt)
	
def ATS_TH_AT26_ERROR_TOTXT(**kwargs):
	
	# Conexion a la bd at26
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# Recuperar las variables definidas en las tareas previas
	FileAT = get_variable('FileAT_at26')
	FechaFile = get_variable('FechaFile')
	FileName_Error = get_variable('FileName_Error')

	# Obtener la fecha de ejecucion del DAG desde el contexto
	#fecha_ejecucion = kwargs.get('execution_date')
	# formatearla a tu gusto (por ejemplo 'YYYYMMDD')
	#FileDate = fecha_ejecucion.strftime('%Y%m%d') if fecha_ejecucion else datetime.now().strftime('%Y%m%d')

	# Generar txt
	registros = hook.get_records("SELECT * FROM FILE_AT.ATS_TH_AT26_ERROR;")

	# Generar el archivo de texto en la ruta indicada
	output_file = f"/home/apacheuser/airflow/reports/AT26/{FileName_Error}{FileAT}{FechaFile}.txt"

	#Escribir los registros en el archivo de texto
	with open(output_file, 'w', encoding='utf-8') as f:
		for row in registros:
			# Convertimos cada fila (tupla) a una cadena separada por comas
			linea = "~".join(str(valor) if valor is not None else "" for valor in row)
			f.write(linea + "\n")

	print(f"Archivo de texto generado en: {output_file}")
	
	
###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT26_TO_FILE_ERROR', default_args=default_args, schedule=None, catchup=False)

FileName_Error_task = PythonOperator(
	task_id='FileName_Error_task',
	python_callable=FileName_Error,
	dag=dag
)

AT26_ATSUDEBAN_ERROR_task = PythonOperator(
	task_id='AT26_ATSUDEBAN_ERROR_task',
	python_callable=AT26_ATSUDEBAN_ERROR,
	dag=dag
)

ATS_TH_AT26_ERROR_TOTXT_task = PythonOperator(
	task_id='ATS_TH_AT26_ERROR_TOTXT_task',
	python_callable=ATS_TH_AT26_ERROR_TOTXT,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
FileName_Error_task >> AT26_ATSUDEBAN_ERROR_task >> ATS_TH_AT26_ERROR_TOTXT_task
