from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import time
import tempfile
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 
from pendulum import timezone

### FUNCIONES DE CADA TAREA ###
def vStgFechaInicio(**kwargs):
    value = '01/01/1801'
    Variable.set('vStgFechaInicio', value)

def vStgFechaInicio(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    vStgFechaInicio = Variable.get('vStgFechaInicio')
    vStgFormatoFecha = Variable.get('vStgFormatoFecha')
    
    sql_query = f'''SELECT COALESCE(MAX(TO_CHAR(DATE_TRUNC('day', CASE WHEN '{vStgFechaInicio}' = 'SYSDATE' THEN CURRENT_DATE ELSE CURRENT_DATE - 1 END), '{vStgFormatoFecha}')), '{vStgFechaInicio}')
    FROM (VALUES (1))
    WHERE UPPER('{vStgFechaInicio}') LIKE '%SYSDATE%';'''
    result = hook.get_records(sql_query)

    Variable.set('vStgFechaInicio', result[0][0])

def vStgFechaFin(**kwargs):
    value = '01/01/1801'
    Variable.set('vStgFechaFin', value)

def vStgFechaFin(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    vStgFechaFin = Variable.get('vStgFechaFin')
    vStgFormatoFecha = Variable.get('vStgFormatoFecha')

    sql_query = f'''SELECT COALESCE(
    (SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_DATE - INTERVAL '1 day'), '{vStgFormatoFecha}') 
    FROM (VALUES (1))
    WHERE UPPER('{vStgFechaFin}') LIKE '%SYSDATE%'), '{vStgFechaFin}');'''
    result = hook.get_records(sql_query)

    Variable.set('vStgFechaFin', result[0][0])

def vStgNombreTablaStg(**kwargs):
    value = 'EMPTY'
    Variable.set('vStgNombreTablaStg', value)

def PROC_UPD_CONFIG(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    vStgFechaInicio = Variable.get('vStgFechaInicio')
    vStgFormatoFecha = Variable.get('vStgFormatoFecha')
    vStgFechaFin = Variable.get('vStgFechaFin')
    vStgNombreTablaStg = Variable.get('vStgNombreTablaStg') 

    # UPD_CONFIG_ONE
    sql_query_deftxt = f'''UPDATE STG.BAN_CONFIG_STG
    SET COS_END_DATE = TO_DATE(CASE WHEN '{vStgFechaFin}' = '01/01/1801' THEN '{vStgFechaInicio}' ELSE '{vStgFechaFin}' END, '{vStgFormatoFecha}'),
        COS_START_DATE = TO_DATE('{vStgFechaInicio}', '{vStgFormatoFecha}')
    WHERE COS_UPDATE_FOR_USER = 'NOUSAR'
    AND COS_SOURCE = '{vStgNombreTablaStg}'
    AND '{vStgFechaInicio}' <> '01/01/1801';'''
    kwargs['ti'].log.info("Accion a ejecutarse: UPD_CONFIG_ONE") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: UPD_CONFIG_ONE, ejecutada exitosamente") 

    # Actualizar eje  Manuales a NOUSAR
    sql_query_deftxt = f'''UPDATE STG.BAN_CONFIG_STG 
    SET COS_UPDATE_FOR_USER = 'NOUSAR';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Actualizar eje  Manuales a NOUSAR") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Actualizar eje  Manuales a NOUSAR, ejecutada exitosamente") 


###### DEFINICION DEL DAG ###### 
local_tz = timezone('America/Caracas')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1).replace(tzinfo=local_tz)
}

dag = DAG(dag_id='PKG_UPD_CONFIG',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

vStgFechaInicio_task = PythonOperator(
    task_id='vStgFechaInicio_task',
    python_callable=vStgFechaInicio,
    provide_context=True,
    dag=dag
)

vStgFechaInicio_task = PythonOperator(
    task_id='vStgFechaInicio_task',
    python_callable=vStgFechaInicio,
    provide_context=True,
    dag=dag
)

vStgFechaFin_task = PythonOperator(
    task_id='vStgFechaFin_task',
    python_callable=vStgFechaFin,
    provide_context=True,
    dag=dag
)

vStgFechaFin_task = PythonOperator(
    task_id='vStgFechaFin_task',
    python_callable=vStgFechaFin,
    provide_context=True,
    dag=dag
)

vStgNombreTablaStg_task = PythonOperator(
    task_id='vStgNombreTablaStg_task',
    python_callable=vStgNombreTablaStg,
    provide_context=True,
    dag=dag
)

PROC_UPD_CONFIG_task = PythonOperator(
    task_id='PROC_UPD_CONFIG_task',
    python_callable=PROC_UPD_CONFIG,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vStgFechaInicio_task >> vStgFechaInicio_task >> vStgFechaFin_task >> vStgFechaFin_task >> vStgNombreTablaStg_task >> PROC_UPD_CONFIG_task
