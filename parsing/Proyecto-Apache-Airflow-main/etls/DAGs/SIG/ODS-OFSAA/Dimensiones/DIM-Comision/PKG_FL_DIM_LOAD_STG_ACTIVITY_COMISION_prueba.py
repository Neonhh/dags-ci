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
from airflow.utils.trigger_rule import TriggerRule

### FUNCIONES DE CADA TAREA ###

def vStgNombreTablaStg(**kwargs):
    value = 'EMPTY'
    Variable.set('vStgNombreTablaStg', value)

def vStgFechaCarga(**kwargs):
    value = '01/01/1801'
    Variable.set('vStgFechaCarga', value)

def vStgSessionPadre(**kwargs):
    value = 'null'
    Variable.set('vStgSessionPadre', value)

def vStgFechaCargaBan(**kwargs):
    value = '01/01/1850'
    Variable.set('vStgFechaCargaBan', value)

def vStgCargaStatus(**kwargs):
    value = 'ERROR'
    Variable.set('vStgCargaStatus', value)

def IT_STG_ACTIVITY_B_INTF_ODS_STG(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ofsaa.stg_activity_b_intf;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ofsaa.stg_activity_b_intf (
	n_activity_display_code,
	v_leaf_only_flag,
	v_enabled_flag,
	v_activity_code,
	v_created_by,
	v_last_modified_by
)
SELECT 
	n_activity_display_code,
	v_leaf_only_flag,
	v_enabled_flag,
	v_activity_code,
	'FILE',
	'FILE'
FROM <?=flowTableName?> AS source_table;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Commit transaction
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Commit transaction") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Commit transaction, ejecutada exitosamente") 

    # Comprimir partition
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Comprimir partition") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Comprimir partition, ejecutada exitosamente") 

    # Drop table I$ view
    sql_query_coltxt = f'''DROP TABLE IF EXISTS W.INT_PRFSTG_ACTIVITY_B_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop table I$ view") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop table I$ view, ejecutada exitosamente") 

    # Drop table I$ view
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop table I$ view") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop table I$ view, ejecutada exitosamente") 

    # Declarar variable sesion paralelismo
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Declarar variable sesion paralelismo") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Declarar variable sesion paralelismo, ejecutada exitosamente") 

    # Drop flow table I$ on staging
    sql_query_coltxt = f'''DROP TABLE IF EXISTS W.INT_PRFSTG_ACTIVITY_B_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow table I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop flow table I$ on staging, ejecutada exitosamente") 

    # Drop flow table I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow table I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop flow table I$ on staging, ejecutada exitosamente") 

    # Drop flow view  I$ on staging
    sql_query_coltxt = f'''DROP VIEW IF EXISTS W.INT_PRFSTG_ACTIVITY_B_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow view  I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop flow view  I$ on staging, ejecutada exitosamente") 

    # Drop flow view  I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow view  I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop flow view  I$ on staging, ejecutada exitosamente") 

    # Create flow table I$ on staging
    sql_query_coltxt = f'''CREATE TABLE IF NOT EXISTS INT_PRFSTG_ACTIVITY_B_INTF
AS
SELECT DISTINCT
	VC.CodNivel_n AS N_ACTIVITY_DISPLAY_CODE,
	CASE WHEN VC.LEAF = 0 THEN 'N' ELSE 'Y' END AS V_LEAF_ONLY_FLAG,
	CASE WHEN VC.STATUS IN ('A', 'V') THEN 'Y' ELSE 'N' END AS V_ENABLED_FLAG,
	VC.CodNivel AS V_ACTIVITY_CODE
FROM STG.BAN_DIM_COMISION_V AS VC
WHERE 1=1;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Create flow table I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Create flow table I$ on staging, ejecutada exitosamente") 

    # Create flow table I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Create flow table I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Create flow table I$ on staging, ejecutada exitosamente") 


def IT_STG_ACTIVITY_TL_INTF_ODS_STG(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Declarar variable sesion paralelismo
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Declarar variable sesion paralelismo") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Declarar variable sesion paralelismo, ejecutada exitosamente") 

    # Drop flow table I$ on staging
    sql_query_coltxt = f'''DROP TABLE IF EXISTS W.INT_PRFSTG_ACTIVITY_TL_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow table I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop flow table I$ on staging, ejecutada exitosamente") 

    # Drop flow table I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow table I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop flow table I$ on staging, ejecutada exitosamente") 

    # Drop flow view  I$ on staging
    sql_query_coltxt = f'''DROP VIEW IF EXISTS W.INT_PRFSTG_ACTIVITY_TL_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow view  I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop flow view  I$ on staging, ejecutada exitosamente") 

    # Drop flow view  I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow view  I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop flow view  I$ on staging, ejecutada exitosamente") 

    # Create flow table I$ on staging
    sql_query_coltxt = f'''CREATE TABLE IF NOT EXISTS W."INT_PRFSTG_ACTIVITY_TL_INTF"
AS
SELECT DISTINCT
	VC.CodNivel_n AS N_ACTIVITY_DISPLAY_CODE,
	VC.Descripcion AS V_ACTIVITY_CODE_NAME,
	VC.Descripcion AS V_DESCRIPTION
FROM STG.BAN_DIM_COMISION_V AS VC
WHERE (1=1);'''
    kwargs['ti'].log.info("Accion a ejecutarse: Create flow table I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Create flow table I$ on staging, ejecutada exitosamente") 

    # Create flow table I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Create flow table I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Create flow table I$ on staging, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ofsaa.stg_activity_tl_intf;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ofsaa.stg_activity_tl_intf (
	n_activity_display_code,
	v_activity_code_name,
	v_description,
	v_language,
	v_created_by,
	v_last_modified_by
)
SELECT 
	n_activity_display_code,
	v_activity_code_name,
	v_description,
	'US',
	'FILE',
	'FILE'
FROM <?=flowTableName?> AS source_table;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Commit transaction
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Commit transaction") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Commit transaction, ejecutada exitosamente") 

    # Comprimir partition
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Comprimir partition") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Comprimir partition, ejecutada exitosamente") 

    # Drop table I$ view
    sql_query_coltxt = f'''DROP TABLE IF EXISTS W.INT_PRFSTG_ACTIVITY_TL_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop table I$ view") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop table I$ view, ejecutada exitosamente") 

    # Drop table I$ view
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop table I$ view") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop table I$ view, ejecutada exitosamente") 


def IT_STG_ACTIVITY_HIER_INTF_ODS_STG(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Declarar variable sesion paralelismo
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Declarar variable sesion paralelismo") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Declarar variable sesion paralelismo, ejecutada exitosamente") 

    # Drop flow table I$ on staging
    sql_query_coltxt = f'''DROP TABLE IF EXISTS W.INT_PRFSTG_ACTIVITY_HIER_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow table I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop flow table I$ on staging, ejecutada exitosamente") 

    # Drop flow table I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow table I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop flow table I$ on staging, ejecutada exitosamente") 

    # Drop flow view  I$ on staging
    sql_query_coltxt = f'''DROP VIEW IF EXISTS W.INT_PRFSTG_ACTIVITY_HIER_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow view  I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop flow view  I$ on staging, ejecutada exitosamente") 

    # Drop flow view  I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop flow view  I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop flow view  I$ on staging, ejecutada exitosamente") 

    # Create flow table I$ on staging
    sql_query_coltxt = f'''CREATE TABLE IF NOT EXISTS W."INT_PRFSTG_ACTIVITY_HIER_INTF"
AS
SELECT
    COALESCE(VC.CodPadre, VC.CodNivel_n) AS N_PARENT_DISPLAY_CODE,
    VC.CodNivel_n AS N_CHILD_DISPLAY_CODE,
    VC.Nivel AS N_DISPLAY_ORDER_NUM
FROM STG.BAN_DIM_COMISION_V AS VC
WHERE (1=1);'''
    kwargs['ti'].log.info("Accion a ejecutarse: Create flow table I$ on staging") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Create flow table I$ on staging, ejecutada exitosamente") 

    # Create flow table I$ on staging
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Create flow table I$ on staging") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Create flow table I$ on staging, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE OFSAA.STG_ACTIVITY_HIER_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ofsaa.stg_activity_hier_intf (n_parent_display_code, n_child_display_code, n_display_order_num, v_hierarchy_object_name, v_created_by, v_last_modified_by)
SELECT n_parent_display_code, n_child_display_code, n_display_order_num, 'ACTIVIDAD', 'FILE', 'FILE'
FROM <?=flowTableName?> AS source_table;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Commit transaction
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Commit transaction") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Commit transaction, ejecutada exitosamente") 

    # Comprimir partition
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Comprimir partition") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Comprimir partition, ejecutada exitosamente") 

    # Drop table I$ view
    sql_query_coltxt = f'''DROP TABLE IF EXISTS W.INT_PRFSTG_ACTIVITY_HIER_INTF;'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop table I$ view") 
    hook.run(sql_query_coltxt)
    kwargs['ti'].log.info("Accion: Drop table I$ view, ejecutada exitosamente") 

    # Drop table I$ view
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Drop table I$ view") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Drop table I$ view, ejecutada exitosamente") 


def Actualizar_status_carga(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # UPD BAN_CONF_STG Last Date Sucess
    sql_query_deftxt = f'''DO $$
BEGIN
  IF '{vStgCargaStatus}' = 'PROCESADO' THEN
    UPDATE stg.ban_config_stg AS bcs
    SET cos_last_date_sucess = '{vStgFechaCargaBan}'::DATE
    WHERE cos_source = '{vStgNombreTablaStg}'
      AND cos_last_date_sucess < '{vStgFechaCarga}'::DATE;
  END IF;
END $$;'''
    kwargs['ti'].log.info("Accion a ejecutarse: UPD BAN_CONF_STG Last Date Sucess") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: UPD BAN_CONF_STG Last Date Sucess, ejecutada exitosamente") 

    # LOAD UPD BAN_CONF_STG_DATE Status Carga
    sql_query_deftxt = f'''UPDATE stg.ban_config_stg_date
SET cds_status_carga = '{vStgCargaStatus}', 
    CDS_SESSION_PROCCES = {vStgSessionPadre}, 
    CDS_SESSION_LOAD = {vStgSessionHija}, 
    cds_fecha_mod = CURRENT_TIMESTAMP
WHERE 
    cds_source = '{vStgNombreTablaStg}' 
    AND cds_date = TO_DATE('{vStgFechaCargaBan}', '{vStgFormatoFecha}')
    AND cds_fecha_mod = (SELECT MAX(A.cds_fecha_mod) FROM stg.ban_config_stg_date AS A WHERE A.cds_source = '{vStgNombreTablaStg}' AND A.cds_date = TO_DATE('{vStgFechaCargaBan}', '{vStgFormatoFecha}'));'''
    kwargs['ti'].log.info("Accion a ejecutarse: LOAD UPD BAN_CONF_STG_DATE Status Carga") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: LOAD UPD BAN_CONF_STG_DATE Status Carga, ejecutada exitosamente") 

    # Commit
    sql_query_deftxt = f''';'''
    kwargs['ti'].log.info("Accion a ejecutarse: Commit") 
    hook.run(sql_query_deftxt)
    kwargs['ti'].log.info("Accion: Commit, ejecutada exitosamente") 


def vStgCargaStatus_Error(**kwargs):
    value = 'ERROR'
    Variable.set('vStgCargaStatus', value)

###### DEFINICION DEL DAG ###### 
local_tz = timezone('America/Caracas')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1).replace(tzinfo=local_tz)
}

dag = DAG(dag_id='PKG_FL_DIM_LOAD_STG_ACTIVITY_COMISION',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

vStgNombreTablaStg_task = PythonOperator(
    task_id='vStgNombreTablaStg_task',
    python_callable=vStgNombreTablaStg,
    provide_context=True,
    dag=dag
)

vStgFechaCarga_task = PythonOperator(
    task_id='vStgFechaCarga_task',
    python_callable=vStgFechaCarga,
    provide_context=True,
    dag=dag
)

vStgSessionPadre_task = PythonOperator(
    task_id='vStgSessionPadre_task',
    python_callable=vStgSessionPadre,
    provide_context=True,
    dag=dag
)

vStgFechaCargaBan_task = PythonOperator(
    task_id='vStgFechaCargaBan_task',
    python_callable=vStgFechaCargaBan,
    provide_context=True,
    dag=dag
)

vStgCargaStatus_task = PythonOperator(
    task_id='vStgCargaStatus_task',
    python_callable=vStgCargaStatus,
    provide_context=True,
    dag=dag
)

IT_STG_ACTIVITY_B_INTF_ODS_STG_task = PythonOperator(
    task_id='IT_STG_ACTIVITY_B_INTF_ODS_STG_task',
    python_callable=IT_STG_ACTIVITY_B_INTF_ODS_STG,
    provide_context=True,
    dag=dag
)

IT_STG_ACTIVITY_TL_INTF_ODS_STG_task = PythonOperator(
    task_id='IT_STG_ACTIVITY_TL_INTF_ODS_STG_task',
    python_callable=IT_STG_ACTIVITY_TL_INTF_ODS_STG,
    provide_context=True,
    dag=dag
)

IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task = PythonOperator(
    task_id='IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task',
    python_callable=IT_STG_ACTIVITY_HIER_INTF_ODS_STG,
    provide_context=True,
    dag=dag
)

Actualizar_status_carga_task = PythonOperator(
    task_id='Actualizar_status_carga_task',
    python_callable=Actualizar_status_carga,
    provide_context=True,
    dag=dag
)

vStgCargaStatus_Error_task = PythonOperator(
    task_id='vStgCargaStatus_Error_task',
    python_callable=vStgCargaStatus_Error,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vStgNombreTablaStg_task >> vStgFechaCarga_task
vStgFechaCarga_task >> vStgSessionPadre_task
vStgSessionPadre_task >> vStgFechaCargaBan_task
vStgFechaCargaBan_task >> vStgCargaStatus_task
vStgCargaStatus_task >> IT_STG_ACTIVITY_B_INTF_ODS_STG_task
IT_STG_ACTIVITY_B_INTF_ODS_STG_task >> IT_STG_ACTIVITY_TL_INTF_ODS_STG_task
IT_STG_ACTIVITY_B_INTF_ODS_STG_task >> vStgCargaStatus_Error_task  # Rama KO
IT_STG_ACTIVITY_TL_INTF_ODS_STG_task >> IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task
IT_STG_ACTIVITY_TL_INTF_ODS_STG_task >> vStgCargaStatus_Error_task  # Rama KO
IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task >> Actualizar_status_carga_task
IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task >> vStgCargaStatus_Error_task  # Rama KO
vStgCargaStatus_Error_task >> Actualizar_status_carga_task
