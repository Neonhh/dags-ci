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
import time

logger = logging.getLogger(__name__)

### FUNCIONES DE CADA TAREA ###

def AT37_ATM_DUPLICADAS(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_atm_tot (
	tipotransferencia	NUMERIC(1),
	clasificacion	NUMERIC(1),
	tipocliente	VARCHAR(3),
	identificacioncliente	VARCHAR(30),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	VARCHAR(5),
	cuentacliente	VARCHAR(20),
	moneda	CHAR(3),
	monto	NUMERIC(15),
	fecha	CHAR(8),
	referencia	VARCHAR(20),
	empresformacion	CHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	tipocontraparte	VARCHAR(3),
	identificacioncontraparte	VARCHAR(30),
	nombrecontraparte	VARCHAR(250),
	tipoinstrumentocontraparte	VARCHAR(5),
	cuentacontraparte	VARCHAR(20),
	oficina	NUMERIC(5)
	);'''
    hook.run(sql_query_deftxt)

	# TRUNCAR
    sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_ATM_TOT;'''
    hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO at_stg.at37_atm_tot (
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
	empresformacion,
	motivo,
	direccionip,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	oficina
	)
	SELECT DISTINCT
		at37_atm_ats.tipotransferencia,
		at37_atm_ats.clasificacion,
		at37_atm_ats.tipocliente,
		at37_atm_ats.identificacioncliente,
		regexp_replace(at37_atm_ats.nombrecliente,'(^[[:space:]]+|[[:space:]]+$)','','g'),
		CAST(AT37_ATM_ATS.tipoinstrumento AS text),
		at37_atm_ats.cuentacliente,
		at37_atm_ats.moneda,
		at37_atm_ats.monto,
		at37_atm_ats.fecha,
		at37_atm_ats.referencia,
		at37_atm_ats.empresformacion,
		at37_atm_ats.motivo,
		at37_atm_ats.direccionip,
		at37_atm_ats.tipocontraparte,
		at37_atm_ats.identificacioncontraparte,
		regexp_replace(at37_atm_ats.nombrecontraparte,'(^[[:space:]]+|[[:space:]]+$)','','g'),
		CAST(at37_atm_ats.tipoinstrumentocontraparte AS text),
		at37_atm_ats.cuentacontraparte,
		at37_atm_ats.oficina
	FROM at_stg.at37_atm_ats

	UNION
    
	SELECT DISTINCT
		2,
		at37_atm_ats.clasificacion,
		at37_atm_ats.tipocontraparte,
		at37_atm_ats.identificacioncontraparte,
		at37_atm_ats.nombrecontraparte,
		at37_atm_ats.tipoinstrumentocontraparte::text,
		at37_atm_ats.cuentacontraparte,
		at37_atm_ats.moneda,
		at37_atm_ats.monto,
		at37_atm_ats.fecha,
		at37_atm_ats.referencia,
		at37_atm_ats.empresformacion,
		at37_atm_ats.motivo,
		'0',
		at37_atm_ats.tipocliente,
		at37_atm_ats.identificacioncliente,
		at37_atm_ats.nombrecliente,
		at37_atm_ats.tipoinstrumento::text,
		at37_atm_ats.cuentacliente,
		at37_atm_ats.oficina
	FROM at_stg.at37_atm_ats;'''
    hook.run(sql_query_deftxt)

def AT37_ATM_ATSUDEBAN(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_ATMS (
	FECHA_EJECUCION DATE,
	TIPOTRANSFERENCIA NUMERIC,
	CLASIFICACION NUMERIC,
	TIPOCLIENTE VARCHAR(1),
	IDENTIFICACIONCLIENTE VARCHAR(15),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO VARCHAR(5),
	CUENTACLIENTE CHAR(20),
	MONEDA CHAR(3),
	MONTO NUMERIC,
	FECHA CHAR(8),
	REFERENCIA VARCHAR(20),
	EMPRESAFORMACION CHAR(3),
	MOTIVO VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE CHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA NUMERIC
	);'''
    hook.run(sql_query_deftxt)

	# TRUNCAR
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_ATMS;'''
    hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_ATMS (
    FECHA_EJECUCION,
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA
	)
	SELECT
		NULL AS FECHA_EJECUCION,
		AT37_ATM_TOT.TIPOTRANSFERENCIA,
		AT37_ATM_TOT.CLASIFICACION,
		TRIM(AT37_ATM_TOT.TIPOCLIENTE),
		AT37_ATM_TOT.IDENTIFICACIONCLIENTE,
		AT37_ATM_TOT.NOMBRECLIENTE,
		TRIM(AT37_ATM_TOT.TIPOINSTRUMENTO),
		AT37_ATM_TOT.CUENTACLIENTE,
		AT37_ATM_TOT.MONEDA,
		AT37_ATM_TOT.MONTO,
		AT37_ATM_TOT.FECHA,
		AT37_ATM_TOT.REFERENCIA,
		AT37_ATM_TOT.EMPRESAFORMACION,
		AT37_ATM_TOT.MOTIVO,
		AT37_ATM_TOT.DIRECCIONIP,
		AT37_ATM_TOT.TIPOCONTRAPARTE,
		AT37_ATM_TOT.IDENTIFICACIONCONTRAPARTE,
		AT37_ATM_TOT.NOMBRECONTRAPARTE,
		AT37_ATM_TOT.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_ATM_TOT.CUENTACONTRAPARTE,
		AT37_ATM_TOT.OFICINA
	FROM AT_STG.AT37_ATM_TOT;'''
    hook.run(sql_query_deftxt)
   

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_ATM37_DUPLICADAS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

AT37_ATM_DUPLICADAS_task = PythonOperator(
    task_id='AT37_ATM_DUPLICADAS_task',
    python_callable=AT37_ATM_DUPLICADAS,
    dag=dag
)

AT37_ATM_ATSUDEBAN_task = PythonOperator(
    task_id='AT37_ATM_ATSUDEBAN_task',
    python_callable=AT37_ATM_ATSUDEBAN,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_ATM_DUPLICADAS_task >> AT37_ATM_ATSUDEBAN_task
