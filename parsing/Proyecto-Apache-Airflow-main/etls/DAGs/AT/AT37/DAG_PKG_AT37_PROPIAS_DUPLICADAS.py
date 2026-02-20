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

def AT37_PROPIAS_DUPLICADAS(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_propias_tot (
	tipotransferencia NUMERIC(1),
	clasificacion NUMERIC(1),
	tipocliente VARCHAR(3),
	identificacioncliente VARCHAR(15),
	nombrecliente VARCHAR(250),
	tipoinstrumento VARCHAR(5),
	cuentacliente VARCHAR(24),
	moneda CHAR(3),
	monto NUMERIC(15,2),
	fecha CHAR(8),
	referencia VARCHAR(20),
	empresaformacion CHAR(3),
	motivo VARCHAR(100),
	direccionip VARCHAR(50),
	tipocontraparte CHAR(3),
	identificacioncontraparte VARCHAR(30),
	nombrecontraparte VARCHAR(100),
	tipoinstrumentocontraparte VARCHAR(5),
	cuentacontraparte VARCHAR(24),
	oficina NUMERIC(5)
	);'''
    hook.run(sql_query_deftxt)

	# TRUNCAR
    sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_PROPIAS_TOT;'''
    hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO at_stg.at37_propias_tot (
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
		at37_propiasf.tipotransferencia AS tipotransferencia,
		at37_propiasf.clasificacion AS clasificacion,
		at37_propiasf.tipocliente AS tipocliente,
		at37_propiasf.identificacioncliente AS identificacioncliente,
		at37_propiasf.nombrecliente AS nombrecliente,
		at37_propiasf.tipoinstrumento::text AS tipoinstrumento,
		at37_propiasf.cuentacliente AS cuentacliente,
		at37_propiasf.moneda AS moneda,
		at37_propiasf.monto AS monto,
		at37_propiasf.fecha AS fecha,
		at37_propiasf.referencia AS referencia,
		at37_propiasf.empresaformacion AS empresaformacion,
		at37_propiasf.motivo AS motivo,
		at37_propiasf.direccionip AS direccionip,
		at37_propiasf.tipocontraparte AS tipocontraparte,
		at37_propiasf.identificacioncontraparte AS identificacioncontraparte,
		at37_propiasf.nombrecontraparte AS nombrecontraparte,
		at37_propiasf.tipoinstrumentocontraparte::text AS tipoinstrumentocontraparte,
		at37_propiasf.cuentacontraparte AS cuentacontraparte,
		at37_propiasf.oficina AS oficina
	FROM at_stg.at37_propiasf

	UNION

	SELECT
		2 AS tipotransferencia,
		at37_propias.clasificacion AS clasificacion,
		at37_propias.tipocontraparte AS tipocliente,
		at37_propias.identificacioncontraparte AS identificacioncliente,
		at37_propias.nombrecontraparte AS nombrecliente,
		at37_propias.tipoinstrumentocontraparte::text AS tipoinstrumento,
		at37_propias.cuentacontraparte AS cuentacliente,
		at37_propias.moneda AS moneda,
		at37_propias.monto AS monto,
		at37_propias.fecha AS fecha,
		at37_propias.referencia AS referencia,
		at37_propias.empresaformacion AS empresaformacion,
		at37_propias.motivo AS motivo,
		'0' AS direccionip,
		at37_propias.tipocliente AS tipocontraparte,
		at37_propias.identificacioncliente AS identificacioncontraparte,
		at37_propias.nombrecliente AS nombrecontraparte,
		at37_propias.tipoinstrumento::text AS tipoinstrumentocontraparte,
		at37_propias.cuentacliente AS cuentacontraparte,
		at37_propias.oficina AS oficina
	FROM at_stg.at37_propias;'''
    hook.run(sql_query_deftxt)

def AT37_PROPIAS_ATSUDEBAN(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_PROPIAS_ (
	FECHA_EJECUCION DATE,
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
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_PROPIAS_;'''
    hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_PROPIAS_ (
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
		AT37_PROPIAS_TOT.TIPOTRANSFERENCIA,
		AT37_PROPIAS_TOT.CLASIFICACION,
		AT37_PROPIAS_TOT.TIPOCLIENTE,
		AT37_PROPIAS_TOT.IDENTIFICACIONCLIENTE,
		AT37_PROPIAS_TOT.NOMBRECLIENTE,
		AT37_PROPIAS_TOT.TIPOINSTRUMENTO,
		TRIM(AT37_PROPIAS_TOT.CUENTACLIENTE),
		AT37_PROPIAS_TOT.MONEDA,
		AT37_PROPIAS_TOT.MONTO,
		AT37_PROPIAS_TOT.FECHA
		AT37_PROPIAS_TOT.REFERENCIA,
		AT37_PROPIAS_TOT.EMPRESAFORMACION,
		AT37_PROPIAS_TOT.MOTIVO,
		AT37_PROPIAS_TOT.DIRECCIONIP,
		AT37_PROPIAS_TOT.TIPOCONTRAPARTE,
		AT37_PROPIAS_TOT.IDENTIFICACIONCONTRAPARTE,
		AT37_PROPIAS_TOT.NOMBRECONTRAPARTE,
		AT37_PROPIAS_TOT.TIPOINSTRUMENTOCONTRAPARTE,
		TRIM(AT37_PROPIAS_TOT.CUENTACONTRAPARTE),
		AT37_PROPIAS_TOT.OFICINA
	FROM AT_STG.AT37_PROPIAS_TOT AS AT37_PROPIAS_TOT;'''
    hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_AT37_PROPIAS_DUPLICADAS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

AT37_PROPIAS_DUPLICADAS_task = PythonOperator(
    task_id='AT37_PROPIAS_DUPLICADAS_task',
    python_callable=AT37_PROPIAS_DUPLICADAS,
    dag=dag
)

AT37_PROPIAS_ATSUDEBAN_task = PythonOperator(
    task_id='AT37_PROPIAS_ATSUDEBAN_task',
    python_callable=AT37_PROPIAS_ATSUDEBAN,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_PROPIAS_DUPLICADAS_task >> AT37_PROPIAS_ATSUDEBAN_task
