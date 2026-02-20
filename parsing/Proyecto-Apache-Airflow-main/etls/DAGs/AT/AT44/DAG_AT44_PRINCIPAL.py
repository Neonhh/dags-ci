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

class AT44_PRIMER_DIA_HABIL(BaseSensorOperator):
    """
    Sensor que espera hasta que el dia actual NO sea feriado.
    La condicion se determina ejecutando una consulta SQL en la base de datos.
    """
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(AT44_PRIMER_DIA_HABIL, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        sql_query = """ SELECT eshabil1 FROM ods.validardiahabil;"""

        records = hook.get_records(sql_query)
        # Suponiendo que la consulta retorna 1 fila, 1 columna:
        status = records[0][0] if records else 0
        self.log.info("Valor de status (1=feriado, 0=no feriado): %s", status)
        # Esperamos que status sea 0 para continuar con el flujo normal
        return status == 0


###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT44_PRINCIPAL',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

holiday_sensor = AT44_PRIMER_DIA_HABIL(     # En la secuencia de ejecucion, colocar este operador en el orden que convenga
    task_id='holiday_sensor',
    postgres_conn_id='at44', 
    poke_interval=10,                    # Se verifica cada 10 segundos para la prueba (para produccion seria 86400seg para una verificacion diaria)
    dag=dag
)

Execution_of_the_Scenario_AT44_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT44_version_001_task',
    trigger_dag_id='AT44',
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_PCK_VALIDACION_RIF_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PCK_VALIDACION_RIF_version_001_task',
    trigger_dag_id='PCK_VALIDACION_RIF',
    wait_for_completion=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
holiday_sensor >> Execution_of_the_Scenario_AT44_version_001_task >> Execution_of_the_Scenario_PCK_VALIDACION_RIF_version_001_task
