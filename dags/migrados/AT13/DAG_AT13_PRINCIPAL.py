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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from pendulum import timezone



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
def AT_DIA_HABIL(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')
	sql_query = '''SELECT 0;'''
	result = hook.get_records(sql_query)
	Variable.set('AT_DIA_HABIL', serialize_value(result[0][0]))

class HolidayCheckSensor(BaseSensorOperator):
    """
    Sensor que espera hasta que el día actual NO sea feriado.
    La condicion se determina ejecutando una consulta SQL en la base de datos.
    
    Para PRUEBAS: Puedes saltarte este check pasando parámetros en la UI:
    En "Trigger DAG w/ config" → { "skip_holiday_check": true }
    """
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(HolidayCheckSensor, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context):
        # Verificar si se debe saltear el check de feriados (para pruebas)
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
            skip_holiday_check = dag_run.conf.get('skip_holiday_check', False)
            if skip_holiday_check:
                self.log.warning("⚠️  MODO PRUEBA: skip_holiday_check=True - Saltando verificación de feriados")
                return True
        
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

def FechaInicio_M(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')
	sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days'), 'MM/DD/YYYY');'''
	# sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months'), 'MM/DD/YYYY');''' # pruebas de mayo
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio_M', serialize_value(result[0][0]))

def FechaFin_M(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')
	sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month - 1 day', 'mm/dd/yyyy');'''
	# sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months') + INTERVAL '1 month - 1 day', 'MM/DD/YYYY');''' # pruebas de mayo
	result = hook.get_records(sql_query)
	Variable.set('FechaFin_M', serialize_value(result[0][0]))

def FechaFin(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	FechaFin_M = get_variable('FechaFin_M')

	sql_query = f'''SELECT '{FechaFin_M}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin', serialize_value(result[0][0]))

def FechaInicio(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	FechaInicio_M = get_variable('FechaInicio_M')

	sql_query = f'''SELECT '{FechaInicio_M}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio', serialize_value(result[0][0]))

def FechaFile(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')
	
	FechaFin = get_variable('FechaFin')

	sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechaFin}', 'MM/DD/YY'), 'YYMMDD') AS result;'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFile', serialize_value(result[0][0]))

def FileAT_at13(**kwargs):
	value = 'AT13'
	Variable.set('FileAT_at13', serialize_value(value))

def FileCodSupervisado(**kwargs):
	value = '01410'
	Variable.set('FileCodSupervisado', serialize_value(value))

def FileDate(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')
	sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
	result = hook.get_records(sql_query)
	Variable.set('FileDate', serialize_value(result[0][0]))

# def ATS_TH_AT13_DATACHECK_TO_E(**kwargs):
# 	hook = PostgresHook(postgres_conn_id='repodataprd')

# 	# creamos tabla destino
# 	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.E$_ATS_TH_AT13 (
# 	RECLAMO	VARCHAR(20),
# 	OFICINA	VARCHAR(10),
# 	TIPOCLIENTE	VARCHAR(50),
# 	IDCLIENTE	VARCHAR(50),
# 	NOMBRECLIENTE	VARCHAR(255),
# 	FECHARECLAMO	VARCHAR(8),
# 	TIPOINSTRUMENTO	VARCHAR(8),
# 	TIPORECLAMO	VARCHAR(150),
# 	MONTORECLAMO	NUMERIC(15,2),
# 	CODIGOCUENTA	VARCHAR(24),
# 	CREDITOAFECTADO	VARCHAR(20),
# 	TARJETAAFECTADA	VARCHAR(24),
# 	ENTESUPERVISADO	VARCHAR(10),
# 	ESTADORECLAMO	VARCHAR(50),
# 	FECHASOLUCION	VARCHAR(8),
# 	MONTOREINTEGRO	NUMERIC(15,2),
# 	FECHAREINTEGRO	VARCHAR(8),
# 	FECHANOTIFICACION	VARCHAR(8),
# 	CODIGOENTE	VARCHAR(20),
# 	IDPOS	VARCHAR(15),
# 	SISTEMA	VARCHAR(50),
# 	OFICINARECLAMO	VARCHAR(50),
# 	TIPOOPERACION	VARCHAR(50),
# 	CANAL	VARCHAR(50),
# 	FRANQUICIA	VARCHAR(50),
# 	RED	VARCHAR(50),
# 	TIPOCLIENTEDESTINO	VARCHAR(50),
# 	IDCLIENTEDESTINO	VARCHAR(50),
# 	NOMBRECLIENTEDESTINO	VARCHAR(50),
# 	NROCUENTADESTINO	NUMERIC(15),
# 	CODIGOCUENTADESTINO	VARCHAR(20),
# 	MONTODESTINO	NUMERIC(20,2)
# 	);'''
# 	hook.run(sql_query_deftxt)
	
# 	# TRUNCATE
# 	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.E$_ATS_TH_AT13;'''
# 	hook.run(sql_query_deftxt)

# 	# INSERTAR DATOS EN DESTINO
# 	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.E$_ATS_TH_AT13 (
# 	RECLAMO,
# 	OFICINA,
# 	TIPOCLIENTE,
# 	IDCLIENTE,
# 	NOMBRECLIENTE,
# 	FECHARECLAMO,
# 	TIPOINSTRUMENTO,
# 	TIPORECLAMO,
# 	MONTORECLAMO,
# 	CODIGOCUENTA,
# 	CREDITOAFECTADO,
# 	TARJETAAFECTADA,
# 	ENTESUPERVISADO,
# 	ESTADORECLAMO,
# 	FECHASOLUCION,
# 	MONTOREINTEGRO,
# 	FECHAREINTEGRO,
# 	FECHANOTIFICACION,
# 	CODIGOENTE,
# 	IDPOS,
# 	SISTEMA,
# 	OFICINARECLAMO,
# 	TIPOOPERACION,
# 	CANAL,
# 	FRANQUICIA,
# 	RED,
# 	TIPOCLIENTEDESTINO,
# 	IDCLIENTEDESTINO,
# 	NOMBRECLIENTEDESTINO,
# 	NROCUENTADESTINO,
# 	CODIGOCUENTADESTINO,
# 	MONTODESTINO
# 	)
# 	SELECT 	
# 		RECLAMO,
# 		OFICINA,
# 		TIPOCLIENTE,
# 		IDCLIENTE,
# 		NOMBRECLIENTE,
# 		FECHARECLAMO,
# 		TIPOINSTRUMENTO,
# 		TIPORECLAMO,
# 		MONTORECLAMO,
# 		CODIGOCUENTA,
# 		CREDITOAFECTADO,
# 		TARJETAAFECTADA,
# 		ENTESUPERVISADO,
# 		ESTADORECLAMO,
# 		FECHASOLUCION,
# 		MONTOREINTEGRO,
# 		FECHAREINTEGRO,
# 		FECHANOTIFICACION,
# 		CODIGOENTE,
# 		IDPOS,
# 		SISTEMA,
# 		OFICINARECLAMO,
# 		TIPOOPERACION,
# 		CANAL,
# 		FRANQUICIA,
# 		RED,
# 		TIPOCLIENTEDESTINO,
# 		IDCLIENTEDESTINO,
# 		NOMBRECLIENTEDESTINO,
# 		NROCUENTADESTINO,
# 		CODIGOCUENTADESTINO,
# 		MONTODESTINO
# 	FROM ATSUDEBAN.ATS_TH_AT13
# 	WHERE NOT (
# 		(
# 			ATS_TH_AT13.OFICINA != '0' OR ATS_TH_AT13.OFICINA IS NOT NULL
# 		)
# 		AND (ats_th_at13.tipocliente IS NOT NULL OR ats_th_at13.tipocliente NOT IN ('0'))
# 		AND (
# 			(tipocliente IN ('V') AND idcliente::numeric BETWEEN 1 AND 40000000)
# 			OR (tipocliente IN ('E') AND (idcliente::numeric BETWEEN 1 AND 1500000 OR idcliente::numeric BETWEEN 80000000 AND 90000000))
# 			OR (tipocliente IN ('J','G','R','P') AND idcliente::numeric > 0)
# 		)
# 		AND (ats_th_at13.fechareclamo > '20081231')
# 		AND (
# 			(tipoinstrumento IN ('30') AND tiporeclamo IN ('2','4','5','6','8','9','10','11','12','18','21','22','23','25'))
# 			OR
# 			(tipoinstrumento IN ('31') AND tiporeclamo IN ('2','3','5','8','9','11','12','13','21','25'))
# 			OR
# 			(tipoinstrumento IN ('32') AND tiporeclamo IN ('2','5','7','8','9','11','12','21'))
# 			OR
# 			(tipoinstrumento IN ('33') AND tiporeclamo IN ('3','13','21','23'))
# 			OR
# 			(tipoinstrumento IN ('50') AND tiporeclamo IN ('8','15','16','18','19','20','21'))
# 			OR
# 			(tipoinstrumento IN ('34') AND tiporeclamo IN ('1','9','11','12'))
# 			OR
# 			(tipoinstrumento IN ('35') AND tiporeclamo IN ('8','9','11','12','20'))
# 			OR
# 			(tipoinstrumento IN ('36') AND tiporeclamo IN ('1','9','11','12','21'))
# 			OR
# 			(tipoinstrumento IN ('43','44','45','46','47','48','49') AND tiporeclamo IN ('9','10','11','14','23','24'))
# 			OR
# 			(tipoinstrumento IN ('40') AND tiporeclamo IN ('3','7','9','10','11','12','13','18','21','23'))
# 			OR
# 			(tipoinstrumento IN ('41') AND tiporeclamo IN ('3','7','9','11','12','13','21','23'))
# 			OR
# 			(tipoinstrumento IN ('37') AND tiporeclamo IN ('9','11'))
# 			OR
# 			(tipoinstrumento IN ('38') AND tiporeclamo IN ('17','22'))
# 			OR
# 			(tipoinstrumento IN ('39') AND tiporeclamo IN ('3','7','9','11','12'))
# 			OR
# 			(tipoinstrumento IN ('42') AND tiporeclamo IN ('3','7','9','11','12'))
# 		)
# 		AND (
# 			(tiporeclamo IN ('1','2','3','4','5','7','11','13','17','22','23') AND montoreclamo > 0)
# 			OR
# 			(tiporeclamo NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND montoreclamo = 0)
# 		)
# 		AND (
# 			(tipoinstrumento IN ('31','34','41','40') AND codigocuenta IS NOT NULL)
# 			OR
# 			(tipoinstrumento NOT IN ('31','34','41','40') AND codigocuenta IS NULL)
# 		)
# 		AND (
# 			(tiporeclamo IN ('9') AND tipoinstrumento IN ('43','44','45','46','47','48','49') AND creditoafectado IS NULL)
# 			OR
# 			(tiporeclamo NOT IN ('9') AND tipoinstrumento IN ('43','44','45','46','47','48','49') AND creditoafectado IS NOT NULL)
# 			OR
# 			(tipoinstrumento NOT IN ('43','44','45','46','47','48','49') AND creditoafectado IS NULL)
# 		)
# 		AND (
# 			(tiporeclamo IN ('9') AND tipoinstrumento IN ('30','31','32') AND tarjetaafectada IS NULL)
# 			OR
# 			(tiporeclamo NOT IN ('9') AND tipoinstrumento IN ('30','31','32') AND tarjetaafectada IS NOT NULL)
# 			OR
# 			(tipoinstrumento NOT IN ('30','31','32') AND tarjetaafectada IS NULL)
# 		)
# 		AND (
# 			(tiporeclamo IN ('13') AND entesupervisado != '0')
# 			OR
# 			(tiporeclamo NOT IN ('13') AND entesupervisado = '0')
# 		)
# 		AND (
# 			(ATS_TH_AT13.ESTADORECLAMO IN ('2','3') AND CAST(ATS_TH_AT13.FECHASOLUCION AS DATE) >= CAST(ATS_TH_AT13.FECHARECLAMO AS DATE))
# 			OR
# 			(ATS_TH_AT13.ESTADORECLAMO NOT IN ('2','3') AND ATS_TH_AT13.FECHASOLUCION = '19000101')
# 		)
# 		AND (
# 			(tiporeclamo IN ('1','2','3','4','5','7','11','13','17','22','23') 
# 			AND estadoreclamo IN ('2','5') AND montoreintegro > 0)
# 			OR
# 			(tiporeclamo IN ('1','2','3','4','5','7','11','13','17','22','23') 
# 			AND estadoreclamo NOT IN ('2','5') AND montoreintegro = 0)
# 			OR
# 			(tiporeclamo NOT IN ('1','2','3','4','5','7','11','13','17','22','23') 
# 			AND montoreintegro = 0)
# 			OR
# 			(tiporeclamo IN ('14') 
# 			AND estadoreclamo IN ('2','5') AND montoreintegro >= 0)
# 		)
# 		AND (
# 			(TIPORECLAMO IN ('1','2','3','4','5','7','11','13','17','22','23') AND
# 			ESTADORECLAMO IN ('2','5') AND 
# 			FECHAREINTEGRO >= FECHARECLAMO)
# 		OR
# 			(TIPORECLAMO IN ('1','2','3','4','5','7','11','13','17','22','23') AND
# 			ESTADORECLAMO IN ('1','3') AND 
# 			FECHAREINTEGRO = '19000101')
# 		OR
# 			(TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND
# 			MONTOREINTEGRO = 0 AND 
# 			FECHAREINTEGRO = '19000101')
# 		OR
# 			(TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND
# 			MONTOREINTEGRO > 0 AND (TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND
# 			MONTOREINTEGRO > 0))
# 		OR
# 			(MONTOREINTEGRO = 0 AND 
# 			FECHAREINTEGRO = '19000101')
# 		)
# 		AND (
# 			(estadoreclamo IN ('2','3','5') AND 
# 			(fechanotificacion::DATE >= fechasolucion::DATE OR fechanotificacion::DATE <= DATE_TRUNC('month', fechareclamo::DATE) + INTERVAL '1 month - 1 day'))
# 			OR
# 			(estadoreclamo NOT IN ('2','3','5') AND fechanotificacion = '19000101')
# 		)
# 		AND (
# 			(tiporeclamo IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND (codigoente NOT IN ('0') OR codigoente IS NOT NULL))
# 			OR
# 			(tiporeclamo NOT IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND codigoente = '0')
# 			OR
# 			(tipoinstrumento NOT IN ('30','31','32') AND codigoente = '0')
# 		)
# 		AND (
# 			(tiporeclamo IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND idpos IS NOT NULL AND idpos <> '0') 
# 			OR 
# 			(tiporeclamo NOT IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND idpos = '0') 
# 			OR 
# 			(tipoinstrumento NOT IN ('30','31','32') AND idpos = '0')
# 		)
# 		AND (
# 			(tiporeclamo IN ('19') AND sistema IS NOT NULL)
# 			OR
# 			(tiporeclamo NOT IN ('19') AND sistema IS NULL)
# 		)
# 		AND (
# 			(tiporeclamo IN ('15','16','20','21') AND oficinareclamo != '0')
# 			OR
# 			(tiporeclamo NOT IN ('15','16','20','21') AND oficinareclamo = '0')
# 		)
# 		AND (
# 			(tipoinstrumento IN ('30','31','32') AND canal NOT IN ('5')) OR
# 			(tipoinstrumento IN ('33') AND canal NOT IN ('3','5','10')) OR
# 			(tipoinstrumento IN ('34') AND canal NOT IN ('2','3','5')) OR
# 			(tipoinstrumento IN ('35') AND canal NOT IN ('1','2','3','4','5','6','7','9','10')) OR
# 			(tipoinstrumento IN ('36','37') AND canal NOT IN ('1','2','3','5','7','9','10')) OR
# 			(tipoinstrumento IN ('38','39') AND canal NOT IN ('1','2','5','7','9','10')) OR
# 			(tipoinstrumento IN ('40','41') AND canal NOT IN ('1','2','10')) OR
# 			(tipoinstrumento IN ('42','43','44','45','46','47','48','49') AND canal NOT IN ('1','2','5','7','9','10')) OR
# 			(tipoinstrumento IN ('50') AND canal IN ('8','0'))
# 		)
# 		AND (
# 			(tipoinstrumento IN ('30','32') AND franquicia != '0')
# 			OR
# 			(tipoinstrumento NOT IN ('30','32') AND franquicia = '0')
# 		)
# 		AND (
# 			(TIPOINSTRUMENTO IN ('30','31','32') AND RED != '0')
# 			OR
# 			(TIPOINSTRUMENTO NOT IN ('30','31','32') AND RED = '0')
# 		)
# 	);
# 	'''
# 	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 
local_tz = timezone("America/Caracas")

default_args = {
    "owner": "airflow",
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="AT13_PRINCIPAL",
    default_args=default_args,
    schedule=None, #"0 11 * * *", 
    catchup=False,
	tags=['Principal']
)

wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_file',
    bucket='airflow-dags-data', 
    object='data/AT13/INSUMOS/TRANS_PROCESADAS.csv',                    
    poke_interval=10,
    timeout=60 * 10,
    dag=dag
)

AT_DIA_HABIL_task = PythonOperator(
	task_id='AT_DIA_HABIL_task',
	python_callable=AT_DIA_HABIL,
	dag=dag
)

holiday_sensor = HolidayCheckSensor(
    task_id='holiday_sensor',
    postgres_conn_id='ods',  
    poke_interval=10,    # verificamos cada 10 segundos para la prueba pero se verifica al dia, 1 dia = 86400 segundos.
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

FileAT_task = PythonOperator(
	task_id='FileAT_task',
	python_callable=FileAT_at13,
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

Execution_of_the_Scenario_AT13_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT13_task',
	trigger_dag_id='AT13',
	wait_for_completion=True,
	dag=dag
)

Execution_of_the_Scenario_AT13_TO_FILE_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_AT13_TO_FILE_task',
	trigger_dag_id='AT13_TO_FILE', 
	wait_for_completion=True,
	dag=dag
)

# ATS_TH_AT13_DATACHECK_TO_E_task = PythonOperator(
# 	task_id='ATS_TH_AT13_DATACHECK_TO_E_task',
# 	python_callable=ATS_TH_AT13_DATACHECK_TO_E,
## 	dag=dag
# )

# Execution_of_the_Scenario_AT13_TO_FILE_ERROR_task = TriggerDagRunOperator(
# 	task_id='Execution_of_the_Scenario_AT13_TO_FILE_ERROR_task',
# 	trigger_dag_id='AT13_TO_FILE_ERROR',
# 	wait_for_completion=True,
# 	dag=dag
# )


###### SECUENCIA DE EJECUCION ######
wait_for_file >> AT_DIA_HABIL_task >> holiday_sensor >> FechaInicio_M_task >> FechaFin_M_task >> FechaFin_task >> FechaInicio_task >> FechaFile_task >> FileAT_task >> FileCodSupervisado_task >> FileDate_task >> Execution_of_the_Scenario_AT13_task >> Execution_of_the_Scenario_AT13_TO_FILE_task
