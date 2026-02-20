from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
import time
import tempfile
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 
from pendulum import timezone
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule



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
# tabla origen: STG.BAN_DIM_COMISION_V 

### FUNCIONES DE CADA TAREA ###
def vStgNombreTablaStg(**kwargs):
    value = 'EMPTY'
    Variable.set('vStgNombreTablaStg', serialize_value(value))

def vStgFechaCarga(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vStgNombreTablaStg = get_variable('vStgNombreTablaStg')
    vStgFormatoFecha = get_variable('vStgFormatoFecha')

    sql_query = f'''SELECT COALESCE(TO_CHAR(MIN(bcsd.cds_date), '{vStgFormatoFecha}'),'01/01/1801')
    FROM stg.ban_config_stg_date bcsd
    JOIN stg.ban_config_stg d 
        ON d.cos_source = bcsd.cds_source
    WHERE bcsd.cds_source = '{vStgNombreTablaStg}'
    AND bcsd.cds_status_carga = 'POR PROCESAR'
    AND bcsd.cds_date BETWEEN d.cos_start_date AND d.cos_end_date;'''
    result = hook.get_records(sql_query)

    Variable.set('vStgFechaCarga', serialize_value(result[0][0]))

def vStgFechaCargaBan(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vStgNombreTablaStg = get_variable('vStgNombreTablaStg')
    vStgFormatoFecha = get_variable('vStgFormatoFecha')

    sql_query = f'''SELECT COALESCE(TO_CHAR(MIN(bcsd.cds_date), '{vStgFormatoFecha}'),'01/01/1801')
    FROM stg.ban_config_stg_date bcsd
    JOIN stg.ban_config_stg s 
        ON s.cos_source = bcsd.cds_source
    WHERE bcsd.cds_source = '{vStgNombreTablaStg}'
    AND bcsd.cds_status_carga = 'POR PROCESAR'
    AND bcsd.cds_date BETWEEN s.cos_start_date AND s.cos_end_date;'''
    result = hook.get_records(sql_query)

    Variable.set('vStgFechaCargaBan', serialize_value(result[0][0]))

def vStgCargaStatus(**kwargs):
    value = 'ERROR'
    Variable.set('vStgCargaStatus', serialize_value(value))

def IT_STG_ACTIVITY_B_INTF_ODS_STG(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    # Creamos la tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ofsaa.stg_activity_b_intf (
    n_activity_display_code numeric(14),
	v_leaf_only_flag varchar(1),
    v_created_by varchar(40),
	v_last_modified_by varchar(30),
	v_enabled_flag varchar(1),
	v_activity_code varchar(20)
    );'''

    logger.info("Creando/verificando tabla")
    hook.run(sql_query_deftxt)
    logger.info("Tabla verificada/creada.")


    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ofsaa.stg_activity_b_intf;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ofsaa.stg_activity_b_intf (
	n_activity_display_code,
	v_leaf_only_flag,
    v_created_by,
	v_last_modified_by,
	v_enabled_flag,
	v_activity_code
    )
    SELECT 
        VC.CodNivel_n AS N_ACTIVITY_DISPLAY_CODE,
        CASE WHEN VC.LEAF = 0 THEN 'N' ELSE 'Y' END AS V_LEAF_ONLY_FLAG,
        'FILE',
        'FILE',
        CASE WHEN VC.STATUS IN ('A', 'V') THEN 'Y' ELSE 'N' END AS V_ENABLED_FLAG,
        VC.CodNivel AS V_ACTIVITY_CODE
    FROM STG.BAN_DIM_COMISION_V AS VC;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

def IT_STG_ACTIVITY_TL_INTF_ODS_STG(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    # Creamos la tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ofsaa.stg_activity_tl_intf (
    n_activity_display_code numeric(14), 
    v_activity_code_name varchar(150), 
    v_description varchar(255), 
    v_language varchar(4), 
    v_created_by varchar(40), 
    v_last_modified_by varchar(30)
    );'''
    logger.info("Creando/verificando tabla")
    hook.run(sql_query_deftxt)
    logger.info("Tabla verificada/creada.")

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ofsaa.stg_activity_tl_intf;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

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
        VC.CodNivel_n AS N_ACTIVITY_DISPLAY_CODE,
        VC.Descripcion AS V_ACTIVITY_CODE_NAME,
	    VC.Descripcion AS V_DESCRIPTION,
        'US', 
        'FILE', 
        'FILE' 
    FROM STG.BAN_DIM_COMISION_V AS VC;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

def IT_STG_ACTIVITY_HIER_INTF_ODS_STG(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    # Creamos la tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ofsaa.stg_activity_hier_intf (
    n_parent_display_code numeric(14), 
    n_child_display_code numeric(14), 
    n_display_order_num numeric(22), 
    v_hierarchy_object_name varchar(150), 
    v_created_by varchar(40), 
    v_last_modified_by varchar(30)
    );'''
    logger.info("Creando/verificando tabla")
    hook.run(sql_query_deftxt)
    logger.info("Tabla verificada/creada.")

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ofsaa.stg_activity_hier_intf;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ofsaa.stg_activity_hier_intf (
    n_parent_display_code, 
    n_child_display_code, 
    n_display_order_num, 
    v_hierarchy_object_name, 
    v_created_by, 
    v_last_modified_by
    )
    SELECT 
        COALESCE(VC.CodPadre, VC.CodNivel_n) AS N_PARENT_DISPLAY_CODE,
        VC.CodNivel_n AS N_CHILD_DISPLAY_CODE,
        VC.Nivel AS N_DISPLAY_ORDER_NUM,
        'ACTIVIDAD', 
        'FILE', 
        'FILE'
    FROM STG.BAN_DIM_COMISION_V AS VC;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def Actualizar_status_carga(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vStgNombreTablaStg = get_variable('vStgNombreTablaStg')
    vStgFormatoFecha = get_variable('vStgFormatoFecha')
    vStgCargaStatus = get_variable('vStgCargaStatus')
    vStgFechaCargaBan = get_variable('vStgFechaCargaBan')
    vStgFechaCarga = get_variable('vStgFechaCarga')
    
    # UPD BAN_CONF_STG Last Date Sucess
    sql_query_deftxt = f'''DO $$
    BEGIN
    IF '{vStgCargaStatus}' = 'PROCESADO' THEN
        UPDATE stg.ban_config_stg AS bcs
        SET cos_last_date_sucess = TO_DATE('{vStgFechaCargaBan}', '{vStgFormatoFecha}')
        WHERE cos_source = '{vStgNombreTablaStg}'
        AND cos_last_date_sucess < TO_DATE('{vStgFechaCarga}', '{vStgFormatoFecha}');
    END IF;
    END $$;'''
    logger.info("Accion a ejecutarse: UPD BAN_CONF_STG Last Date Sucess") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: UPD BAN_CONF_STG Last Date Sucess, ejecutada exitosamente") 

    # OFSAA UPD BAN_CONF_STG_DATE Status Carga
    sql_query_deftxt = f''' UPDATE stg.ban_config_stg_date
    SET cds_status_carga = SUBSTRING(cds_status_carga FROM 5),
        cds_fecha_mod = CURRENT_TIMESTAMP
    WHERE cds_fecha_mod IN (
        SELECT MAX(con.cds_fecha_mod)
        FROM stg.ban_config_stg_date con
        JOIN stg.ban_predece_load_plan_ofsaa ofs
            ON ofs.plp_tabla_stg = con.cds_source
        WHERE ofs.plp_plan_load_ofsaa = '{vStgNombreTablaStg}'
        AND con.cds_status_carga LIKE 'STG_%'
        AND con.cds_date = TO_DATE('{vStgFechaCarga}', '{vStgFormatoFecha}')
        GROUP BY con.cds_source);'''
    logger.info("Accion a ejecutarse: OFSAA UPD BAN_CONF_STG_DATE Status Carga") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: OFSAA UPD BAN_CONF_STG_DATE Status Carga, ejecutada exitosamente") 

def vStgCargaStatus_Error(**kwargs):
    value = 'ERROR'
    Variable.set('vStgCargaStatus', serialize_value(value))

###### DEFINICION DEL DAG ###### 
local_tz = timezone('America/Caracas')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_FL_DIM_LOAD_STG_ACTIVITY_COMISION',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

vStgNombreTablaStg_task = PythonOperator(
    task_id='vStgNombreTablaStg_task',
    python_callable=vStgNombreTablaStg,
    dag=dag
)

vStgFechaCarga_task = PythonOperator(
    task_id='vStgFechaCarga_task',
    python_callable=vStgFechaCarga,
    dag=dag
)

vStgFechaCargaBan_task = PythonOperator(
    task_id='vStgFechaCargaBan_task',
    python_callable=vStgFechaCargaBan,
    dag=dag
)

vStgCargaStatus_task = PythonOperator(
    task_id='vStgCargaStatus_task',
    python_callable=vStgCargaStatus,
    dag=dag
)

IT_STG_ACTIVITY_B_INTF_ODS_STG_task = PythonOperator(
    task_id='IT_STG_ACTIVITY_B_INTF_ODS_STG_task',
    python_callable=IT_STG_ACTIVITY_B_INTF_ODS_STG,
    dag=dag
)

IT_STG_ACTIVITY_TL_INTF_ODS_STG_task = PythonOperator(
    task_id='IT_STG_ACTIVITY_TL_INTF_ODS_STG_task',
    python_callable=IT_STG_ACTIVITY_TL_INTF_ODS_STG,
    dag=dag
)

IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task = PythonOperator(
    task_id='IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task',
    python_callable=IT_STG_ACTIVITY_HIER_INTF_ODS_STG,
    dag=dag
)

Actualizar_status_carga_task = PythonOperator(
    task_id='Actualizar_status_carga_task',
    python_callable=Actualizar_status_carga,
    trigger_rule=TriggerRule.ALL_DONE,  # ejecuta la tarea siempre al final, sin importar el estado de las tareas previas.
    dag=dag
)

vStgCargaStatus_Error_task = PythonOperator(
    task_id='vStgCargaStatus_Error_task',
    python_callable=vStgCargaStatus_Error,
    trigger_rule=TriggerRule.ONE_FAILED,  # se activa si alguna de sus dependencias falla
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vStgNombreTablaStg_task >> vStgFechaCarga_task >> vStgFechaCargaBan_task >> vStgCargaStatus_task >> IT_STG_ACTIVITY_B_INTF_ODS_STG_task

# Bifurcacion de dependencias tipo ok y ko

# Rama ok
IT_STG_ACTIVITY_B_INTF_ODS_STG_task >> IT_STG_ACTIVITY_TL_INTF_ODS_STG_task
# Rama ko
IT_STG_ACTIVITY_B_INTF_ODS_STG_task >> vStgCargaStatus_Error_task

# Rama ok
IT_STG_ACTIVITY_TL_INTF_ODS_STG_task >> IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task
# Rama ko
IT_STG_ACTIVITY_TL_INTF_ODS_STG_task >> vStgCargaStatus_Error_task

# Rama ok
IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task >> Actualizar_status_carga_task
# Rama ko
IT_STG_ACTIVITY_HIER_INTF_ODS_STG_task >> vStgCargaStatus_Error_task

# Si alguna falla, caemos en error
vStgCargaStatus_Error_task >> Actualizar_status_carga_task

