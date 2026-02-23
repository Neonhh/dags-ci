from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
from airflow.operators.python import PythonOperator



logger = logging.getLogger(__name__)
def fail_task():
    raise Exception("Error simulado")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 1),
    'retries': 0
}

with DAG(
    dag_id='DAG_TEST_OK_KO',
    default_args=default_args,
    schedule=None,  # Manual para pruebas
    catchup=False
) as dag:

    # ======== TAREAS PRINCIPALES ==========
    vStgNombreTablaStg_task = EmptyOperator(task_id='vStgNombreTablaStg_task')
    vStgFechaCarga_task = EmptyOperator(task_id='vStgFechaCarga_task')
    vStgFechaCargaBan_task = EmptyOperator(task_id='vStgFechaCargaBan_task')
    vStgCargaStatus_task = EmptyOperator(task_id='vStgCargaStatus_task')

    IT_STG_ACTIVITY_B_INTF_ODS_STG_task = EmptyOperator(task_id='IT_STG_ACTIVITY_B_INTF_ODS_STG_task')

    IT_STG_ACTIVITY_TL_INTF_ODS_STG_task = EmptyOperator(task_id='IT_STG_ACTIVITY_TL_INTF_ODS_STG_task')

    IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task = EmptyOperator(task_id='IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task')

    # ======== RAMA DE ERROR (KO) ==========
    vStgCargaStatus_Error_task = EmptyOperator(
        task_id='vStgCargaStatus_Error_task',
        trigger_rule=TriggerRule.ONE_FAILED  # se activa si alguna de sus dependencias falla
    )

    # ======== ACTUALIZAR STATUS ==========
    Actualizar_status_carga_task = EmptyOperator(
        task_id='Actualizar_status_carga_task',
        trigger_rule=TriggerRule.ALL_DONE  # ejecuta la tarea siempre al final, sin importar el estado de las tareas previas.
        # trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # para ejecutarlo en ambas ramas de ok y ko.
        # Ejecuta esta tarea si al menos una dependencia fue exitosa
    )


    # ======== SECUENCIA ==========
    vStgNombreTablaStg_task >> vStgFechaCarga_task
    vStgFechaCarga_task >> vStgFechaCargaBan_task
    vStgFechaCargaBan_task >> vStgCargaStatus_task
    vStgCargaStatus_task >> IT_STG_ACTIVITY_B_INTF_ODS_STG_task

    # Bifurcacion de dependencias tipo ok y ko
    IT_STG_ACTIVITY_B_INTF_ODS_STG_task >> IT_STG_ACTIVITY_TL_INTF_ODS_STG_task
    IT_STG_ACTIVITY_B_INTF_ODS_STG_task >> vStgCargaStatus_Error_task

    IT_STG_ACTIVITY_TL_INTF_ODS_STG_task >> IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task
    IT_STG_ACTIVITY_TL_INTF_ODS_STG_task >> vStgCargaStatus_Error_task

    IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task >> Actualizar_status_carga_task
    IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task >> vStgCargaStatus_Error_task

    vStgCargaStatus_Error_task >> Actualizar_status_carga_task
