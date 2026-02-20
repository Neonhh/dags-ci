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

def Actualizar_cuenta_formato_Sudeban_II(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
SET CUENTA = '7330010000'
WHERE CUENTA = '733011000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
SET CUENTA = '7340020000'
WHERE CUENTA = '734012000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
SET CUENTA = '7430020000'
WHERE CUENTA = '743012000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE "ATSUDEBAN"."AT03_OFICINA_FINAL"
SET CUENTA = '7530020000'
WHERE CUENTA = '753042000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE "ATSUDEBAN.AT03_V001_BANCARIBE"
SET CUENTA = '7330010000'
WHERE CUENTA = '733011000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
SET CUENTA = '7340020000'
WHERE CUENTA = '734012000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
SET CUENTA = '7430020000'
WHERE CUENTA = '743012000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
SET CUENTA = '7530020000'
WHERE CUENTA = '753042000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
SET CUENTA = '7340010000'
WHERE CUENTA = '734011000000';'''
    hook.run(sql_query)

    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
SET CUENTA = '7340010000'
WHERE CUENTA = '734011000000';'''
    hook.run(sql_query)

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='Actualizar_cuenta_formato_Sudeban_II',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

Actualizar_cuenta_formato_Sudeban_II_task = PythonOperator(
    task_id='Actualizar_cuenta_formato_Sudeban_II_task',
    python_callable=Actualizar_cuenta_formato_Sudeban_II,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
Actualizar_cuenta_formato_Sudeban_II_task