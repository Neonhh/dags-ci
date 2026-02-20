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
def vStgFormatoFecha(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT 'MM/DD/YYYY';'''
    result = hook.get_records(sql_query)

    Variable.set('vStgSysdate', result[0][0])

def vStgSysdate(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    vStgFormatoFecha = Variable.get('vStgFormatoFecha')

    sql_query = f'''SELECT TO_CHAR(CURRENT_DATE - INTERVAL '20 days', '{vStgFormatoFecha}') AS result;'''
    result = hook.get_records(sql_query)

    Variable.set('vStgSysdate', result[0][0])

def vStgNombreTablaStg(**kwargs):
    value = 'EMPTY'
    Variable.set('vStgNombreTablaStg', value)

def vStgFechaFin(**kwargs):
    value = '01/01/1801'
    Variable.set('vStgFechaFin', value)

def vStgFechaInicio(**kwargs):
    value = '01/01/1801'
    Variable.set('vStgFechaInicio', value)

def vStgTipoFechaCarga(**kwargs):
    value = 0
    Variable.set('vStgTipoFechaCarga', value)

def vStgFechaCarga(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    vStgFormatoFecha = Variable.get('vStgFormatoFecha')
    vStgFechaInicio = Variable.get('vStgFechaInicio')
    vStgNombreTablaStg = Variable.get('vStgNombreTablaStg')
    vStgTipoFechaCarga = Variable.get('vStgTipoFechaCarga')

    sql_query = f'''SELECT 
    CASE 
    WHEN {vStgTipoFechaCarga} = 3 THEN 
        TO_CHAR({vStgFechaInicio}, {vStgFormatoFecha}) 
    ELSE 
        (SELECT COALESCE(TO_CHAR(MIN(bcsd.cds_date), {vStgFormatoFecha}), '01/01/1801')
        FROM stg.BAN_CONFIG_STG_DATE AS bcsd
        INNER JOIN stg.BAN_CONFIG_STG AS d 
        ON bcsd.cds_source = d.cos_source
        WHERE bcsd.cds_source = {vStgNombreTablaStg}
        AND bcsd.cds_status_carga = 'POR PROCESAR' 
        AND bcsd.cds_date BETWEEN d.cos_start_date AND d.cos_end_date)
    END;'''
    result = hook.get_records(sql_query)

    Variable.set('vStgFechaCarga', result[0][0])

def Actualizar_Status_ulltima_Carga(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    vStgNombreTablaStg = Variable.get('vStgNombreTablaStg')
    vStgFormatoFecha = Variable.get('vStgFormatoFecha')
    vStgFechaCarga = Variable.get('vStgFechaCarga')

    # Actualiza el status  de STG  en la tabla ban_config_stg
    sql_query_deftxt = f'''DO $$
    DECLARE
    contEstatus INTEGER;
    nuevoEstatus VARCHAR(255);
    BEGIN
    SELECT COUNT(1) INTO contEstatus 
    FROM stg.ban_config_stg_date AS bcsd
    INNER JOIN stg.ban_config_stg AS bcs ON bcs.cos_source = bcsd.cds_source
    WHERE bcsd.cds_status_carga = 'ERROR'
        AND bcs.cos_source = '{vStgNombreTablaStg}'
        AND bcsd.CDS_SESSION_PROCCES = current_setting('SESS_NO')
        AND bcsd.cds_date BETWEEN bcs.cos_start_date AND bcs.cos_end_date;

    IF contEstatus = 0 THEN
        SELECT COUNT(1) INTO contEstatus
        FROM stg.ban_config_stg_date AS bcsd
        INNER JOIN stg.ban_config_stg AS bcs ON bcs.cos_source = bcsd.cds_source
        WHERE bcsd.cds_status_carga = 'CONTINGENCIA'
        AND bcs.cos_source = '{vStgNombreTablaStg}'
        AND bcsd.CDS_SESSION_PROCCES = current_setting('SESS_NO')
        AND bcsd.cds_date BETWEEN bcs.cos_start_date AND bcs.cos_end_date;

        IF contEstatus = 0 THEN
        nuevoEstatus := 'PROCESADO';
        ELSE
        nuevoEstatus := 'CONTINGENCIA';
        END IF;
    ELSE
        nuevoEstatus := 'ERROR';
    END IF;

    SELECT COUNT(1) INTO contEstatus 
    FROM stg.ban_config_stg_date AS bcsd
    INNER JOIN stg.ban_config_stg AS bcs ON bcs.cos_source = bcsd.cds_source
    WHERE bcs.cos_source = '{vStgNombreTablaStg}'
        AND bcsd.CDS_SESSION_PROCCES = current_setting('SESS_NO');

    UPDATE stg.BAN_CONFIG_STG
    SET COS_STATUS_CARGA = nuevoEstatus
    WHERE COS_SOURCE = '{vStgNombreTablaStg}' AND contEstatus != 0;
    END $$;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Actualiza el status  de STG  en la tabla ban_config_stg") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Actualiza el status  de STG  en la tabla ban_config_stg, ejecutada exitosamente") 

    # Actualiza el status  de OFSAA  en la tabla ban_config_stg
    sql_query_deftxt = f''' UPDATE stg.ban_config_stg
    SET cos_status_carga = SUBSTRING(cos_status_carga FROM 5)
    WHERE cos_source IN (
        SELECT con.cos_source
        FROM stg.ban_config_stg con
        JOIN ofsaa.ban_predece_load_plan_ofsaa ofs
            ON ofs.plp_tabla_stg = con.cos_source
        WHERE ofs.plp_plan_load_ofsaa = '{vStgNombreTablaStg}'
        AND con.cos_status_carga LIKE 'STG_%'
        AND con.cos_start_date = TO_DATE('{vStgFechaCarga}', '{vStgFormatoFecha}')
    );'''
    kwargs['ti'].log.info("Accion a ejecutarse: Actualiza el status  de OFSAA  en la tabla ban_config_stg") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Actualiza el status  de OFSAA  en la tabla ban_config_stg, ejecutada exitosamente") 

###### DEFINICION DEL DAG ###### 
local_tz = timezone('America/Caracas')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1).replace(tzinfo=local_tz)
}

dag = DAG(dag_id='PKG_FL_DIM_PROCCESSVAL_STG_ACTIVITY_COMISION',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

vStgFormatoFecha_task = PythonOperator(
    task_id='vStgFormatoFecha_task',
    python_callable=vStgFormatoFecha,
    provide_context=True,
    dag=dag
)

vStgSysdate_task = PythonOperator(
    task_id='vStgSysdate_task',
    python_callable=vStgSysdate,
    provide_context=True,
    dag=dag
)

vStgNombreTablaStg_task = PythonOperator(
    task_id='vStgNombreTablaStg_task',
    python_callable=vStgNombreTablaStg,
    provide_context=True,
    dag=dag
)

vStgFechaFin_task = PythonOperator(
    task_id='vStgFechaFin_task',
    python_callable=vStgFechaFin,
    provide_context=True,
    dag=dag
)

vStgFechaInicio_task = PythonOperator(
    task_id='vStgFechaInicio_task',
    python_callable=vStgFechaInicio,
    provide_context=True,
    dag=dag
)

vStgTipoFechaCarga_task = PythonOperator(
    task_id='vStgTipoFechaCarga_task',
    python_callable=vStgTipoFechaCarga,
    provide_context=True,
    dag=dag
)

PKG_UPD_CONFIG_task = TriggerDagRunOperator(
    task_id='PKG_UPD_CONFIG_task',
    trigger_dag_id='PKG_UPD_CONFIG',
    wait_for_completion=True,
    dag=dag
)

vStgFechaCarga_task = PythonOperator(
    task_id='vStgFechaCarga_task',
    python_callable=vStgFechaCarga,
    provide_context=True,
    dag=dag
)

PKG_FL_COM_LOAD_STG_DIM_COMISION_task = TriggerDagRunOperator(
    task_id='PKG_FL_COM_LOAD_STG_DIM_COMISION_task',
    trigger_dag_id='PKG_FL_COM_LOAD_STG_DIM_COMISION', 
    wait_for_completion=True,
    dag=dag
)

Actualizar_Status_ulltima_Carga_task = PythonOperator(
    task_id='Actualizar_Status_ulltima_Carga_task',
    python_callable=Actualizar_Status_ulltima_Carga,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vStgFormatoFecha_task >> vStgSysdate_task >> vStgNombreTablaStg_task >> vStgFechaFin_task >> vStgFechaInicio_task >> vStgTipoFechaCarga_task >> PKG_UPD_CONFIG_task >> vStgFechaCarga_task >> PKG_FL_COM_LOAD_STG_DIM_COMISION_task >> Actualizar_Status_ulltima_Carga_task
