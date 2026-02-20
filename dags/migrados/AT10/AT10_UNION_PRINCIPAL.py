from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
from io import StringIO
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.sensors.gcs import GCSHook # <-- AGREGAR ESTA IMPORTACION
import time
import os
import tempfile



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
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
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
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days'), 'MM/DD/YYYY')'''
    result = hook.get_records(sql_query)
    Variable.set('FechaInicio_M', serialize_value(result[0][0]))
    print(f"Value of FechaInicio_M: {result[0][0]}")

def FechaFin_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month - 1 day', 'mm/dd/yyyy');'''
    result = hook.get_records(sql_query)
    Variable.set('FechaFin_M', serialize_value(result[0][0]))
    print(f"Value of FechaFin_M: {result[0][0]}")

def FechaInicio(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT '{FechaInicio_M}';'''
    result = hook.get_records(sql_query)
    Variable.set('FechaInicio', serialize_value(result[0][0]))
    print(f"Value of FechaInicio: {result[0][0]}")

def FechaFin(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT '{FechaFin_M}';'''
    result = hook.get_records(sql_query)
    Variable.set('FechaFin', serialize_value(result[0][0]))
    print(f"Value of FechaFin: {result[0][0]}")

def FechaFile(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month - 1 day', 'yymmdd');'''
    result = hook.get_records(sql_query)
    # Variable.set('FechaFile', serialize_value(result[0][0]))
    Variable.set('FechaFile', "250630")  # <-- CAMBIO TEMPORAL PARA PRUEBAS (usamos fecha del 30 de Junio 2025) SE DEBE CAMBIAR LUEGO
    print(f"Value of FechaFile: {result[0][0]}")

def FileAT(**kwargs):
    value = 'AT10'
    Variable.set('FileAT', serialize_value(value))

def Moneda_Vzla(**kwargs):
    hook = PostgresHook(postgres_conn_id='repodataprd')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT VALOR FROM ATSUDEBAN.ATS_TM_PARAMETROS
WHERE (CLAVE = 'MONEDA' AND CLASIFICACION = 'AT');'''
    result = hook.get_records(sql_query)
    Variable.set('Moneda_Vzla', serialize_value(result[0][0]))

def FileCodSupervisado(**kwargs):
    value = '01410'
    Variable.set('FileCodSupervisado', serialize_value(value))

def FileDate(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
    result = hook.get_records(sql_query)
    Variable.set('FileDate', serialize_value(result[0][0]))

def FileName_Error(**kwargs):
    hook = PostgresHook(postgres_conn_id='repodataprd')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    sql_query = '''SELECT 'ErrorValid';'''
    result = hook.get_records(sql_query)
    Variable.set('FileName_Error', serialize_value(result[0][0]))

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT10_UNIFICADO_PRINCIPAL',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False,
          tags=['ATs', 'AT10','Unificado','Principal'])

wait_for_file_fideicomiso = GCSObjectExistenceSensor(        # En la secuencia de ejecucion, colocar este operador en el orden que convenga (Agregar un operador FileSensor por cada insumo que use el AT)
    task_id='wait_for_file_fideicomiso',
    bucket='airflow-dags-data',
    object='data/AT10/UNIFICADO/{{ var.value.FileAT }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}_FIDEICOMISO.txt',
    poke_interval=10,
    timeout=60 * 10,
    dag=dag
)

wait_for_file_la = GCSObjectExistenceSensor(        # En la secuencia de ejecucion, colocar este operador en el orden que convenga (Agregar un operador FileSensor por cada insumo que use el AT)
    task_id='wait_for_file_la',
    bucket='airflow-dags-data',
    object='data/AT10/UNIFICADO/{{ var.value.FileAT }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}_LA.txt',
    poke_interval=10,
    timeout=60 * 10,
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

FechaInicio_task = PythonOperator(
    task_id='FechaInicio_task',
    python_callable=FechaInicio,
    dag=dag
)

FechaFin_task = PythonOperator(
    task_id='FechaFin_task',
    python_callable=FechaFin,
    dag=dag
)

FechaFile_task = PythonOperator(
    task_id='FechaFile_task',
    python_callable=FechaFile,
    dag=dag
)

FileAT_task = PythonOperator(
    task_id='FileAT_task',
    python_callable=FileAT,
    dag=dag
)

Moneda_Vzla_task = PythonOperator(
    task_id='Moneda_Vzla_task',
    python_callable=Moneda_Vzla,
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

FileName_Error_task = PythonOperator(
    task_id='FileName_Error_task',
    python_callable=FileName_Error,
    dag=dag
)

Execution_of_the_Scenario_AT10_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT10_version_001_task',
    trigger_dag_id='AT10',  # Acomodar ID del DAG a disparar (Se usa el PackName)
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_AT10_ATSUDEBAN_TOFILE_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT10_ATSUDEBAN_TOFILE_version_001_task',
    trigger_dag_id='AT10_ATSUDEBAN_TOFILE',  # Acomodar ID del DAG a disparar (Se usa el PackName)
    wait_for_completion=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
FechaInicio_M_task >> FechaFin_M_task >> FechaInicio_task >> FechaFin_task >> FechaFile_task >> FileAT_task >> Moneda_Vzla_task >> FileCodSupervisado_task >> FileDate_task >> FileName_Error_task >> wait_for_file_fideicomiso >> wait_for_file_la >> Execution_of_the_Scenario_AT10_version_001_task >> Execution_of_the_Scenario_AT10_ATSUDEBAN_TOFILE_version_001_task
