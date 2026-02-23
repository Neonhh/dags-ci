from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import time

### FUNCIONES DE CADA TAREA ###

def ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    sql_query = ''';'''
    hook.run(sql_query)

    sql_query = ''';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
SET
SALDO = (SELECT SUM(SALDO) FROM ATSUDEBAN.AGRUPACION_BANCARIBE
WHERE CUENTA IN ('712032000000', '712092000000', '712012000000') )
WHERE CUENTA IN ('712002000000');'''
    hook.run(sql_query)

###### DEFINICION DEL DAG ######

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task = PythonOperator(
    task_id='ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task',
    python_callable=ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task
