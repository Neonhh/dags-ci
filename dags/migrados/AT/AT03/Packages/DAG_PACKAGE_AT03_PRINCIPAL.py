from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import time
import tempfile
import os
import logging
from decimal import Decimal
import json

logger = logging.getLogger(__name__)

def serialize_value(value):
    """
    Helper para serializar valores de PostgreSQL a JSON.
    Convierte Decimal, date, datetime a string para preservar precision.
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
    
    Para conversiones especificas:
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
    Sensor que espera hasta que el dia actual NO sea feriado.
    La condicion se determina ejecutando una consulta SQL en la base de datos.
    """
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(HolidayCheckSensor, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
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

def GenerateGuid(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT gen_random_uuid();'''
    result = hook.get_records(sql_query)

    Variable.set('GenerateGuid', serialize_value(result[0][0]))

def FileAT(**kwargs):
    value = 'AT12'
    Variable.set('FileAT', serialize_value(value))

def FileCodSupervisado(**kwargs):
    value = '01410'
    Variable.set('FileCodSupervisado', serialize_value(value))

def FechaInicio_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT TO_CHAR(TRUNC(CURRENT_DATE - INTERVAL '28 days', 'month'), 'mm/dd/yyyy') FROM DUAL;'''
    result = hook.get_records(sql_query)

    Variable.set('FechaInicio_M', serialize_value(result[0][0]))

def FechaFin_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT TO_CHAR(LAST_DAY(CURRENT_DATE - INTERVAL '28 days'), 'mm/dd/yyyy') FROM DUAL;'''
    result = hook.get_records(sql_query)

    Variable.set('FechaFin_M', serialize_value(result[0][0]))

def FechaInicio(**kwargs):
    value = '08/26/2018'
    Variable.set('FechaInicio', serialize_value(value))

def FileName_Error(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT 'ErrorValid';'''
    result = hook.get_records(sql_query)

    Variable.set('FileName_Error', serialize_value(result[0][0]))

def FechaFin(**kwargs):
    value = '09/01/2018'
    Variable.set('FechaFin', serialize_value(value))

def FileDate(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
    result = hook.get_records(sql_query)

    Variable.set('FileDate', serialize_value(result[0][0]))

def RutaDirectorio(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT directorio
FROM RUTA_DIRAT
WHERE nombreat = '{FileAT}';'''
    result = hook.get_records(sql_query)

    Variable.set('RutaDirectorio', serialize_value(result[0][0]))

def RutaDirectorioAnterior(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT (SUBSTR(directorio, 1, LENGTH(directorio) - LENGTH('{FileAT}')))
FROM RUTA_DIRAT
WHERE nombreat = '{FileAT}';'''
    result = hook.get_records(sql_query)

    Variable.set('RutaDirectorioAnterior', serialize_value(result[0][0]))

def FechaFile(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechaFin}', 'MM/DD/YY'), 'YYMMDD');'''
    result = hook.get_records(sql_query)

    Variable.set('FechaFile', serialize_value(result[0][0]))

def AT03_Max_Periodo(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT
    CASE
        WHEN TO_CHAR(CURRENT_DATE, 'MM') = '01' THEN '2' || (TO_CHAR(CURRENT_DATE, 'YY') - 1)
        WHEN TO_CHAR(CURRENT_DATE, 'MM') < '08' THEN '1' || TO_CHAR(CURRENT_DATE, 'YY')
        ELSE '2' || TO_CHAR(CURRENT_DATE, 'YY')
    END;'''
    result = hook.get_records(sql_query)

    Variable.set('AT03_Max_Periodo', serialize_value(result[0][0]))

def AT03_Max_Corte(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT MAX(co_corte)
FROM cb_corte
WHERE TO_CHAR(co_periodo) = '{AT03_Max_Periodo}'
  AND TO_CHAR(co_fecha_fin, 'yyyymmdd') = TO_CHAR(LAST_DAY(CURRENT_DATE - INTERVAL '28 days'), 'yyyymmdd')
GROUP BY co_periodo;'''
    result = hook.get_records(sql_query)

    Variable.set('AT03_Max_Corte', serialize_value(result[0][0]))




def ATS_TH_AT03(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # create error table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS "%ERR_PRFATS_TH_AT03" (
    ODI_ROW_ID UROWID,
    ODI_ERR_TYPE VARCHAR(1) NULL,
    ODI_ERR_MESS VARCHAR(250) NULL,
    ODI_CHECK_DATE DATE NULL,
    OFICINA VARCHAR(5) NULL,
    CODIGO_CONTABLE VARCHAR(10) NULL,
    SALDO NUMERIC(32, 7) NULL,
    CONSECUTIVO VARCHAR(1) NULL,
    ODI_ORIGIN VARCHAR(100) NULL,
    ODI_CONS_NAME VARCHAR(128) NULL,
    ODI_CONS_TYPE VARCHAR(2) NULL,
    ODI_PK VARCHAR(32) PRIMARY KEY,
    ODI_SESS_NO VARCHAR(19)
);'''
    logger.info("Accion a ejecutarse: create error table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: create error table, ejecutada exitosamente") 

    # insert CK errors
    sql_query_deftxt = f'''INSERT INTO "ERR_PRFATS_TH_AT03" (ODI_PK, ODI_SESS_NO, ODI_ROW_ID, ODI_ERR_TYPE, ODI_ERR_MESS, ODI_CHECK_DATE, ODI_ORIGIN, ODI_CONS_NAME, ODI_CONS_TYPE, OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO)
SELECT SYS_GUID(), <?=snpRef.getSession("SESS_NO") ?>, rowid, 'S', '1- El valor de este campo debe ser <=0 cuando las cta. contables esten en los parametros Sudeban AT03-1', sysdate, 'ATS_TH_AT03', 'SALDO_1', 'CK', OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO
FROM "ATS_TH_AT03" ATS_TH_AT03
WHERE NOT (SUBSTR(ATS_TH_AT03.OFICINA, 1, 1) IN ('V', 'I') AND (SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 5) IN ('11901', '12125', '12225', '12325', '12425', '12625', '17249', '17349', '17449', '17549') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 3) IN ('129', '139', '149', '159', '169') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 8) IN ('18101102', '18101202', '18102102', '18102202', '18103102', '18103202', '18105102', '18105202', '18105104', '18105204', '18105106', '18105206', '18106102', '18106202', '18107102', '18107202', '18902102', '18902202', '18902101', '18902201', '13136102', '13236102', '13336102', '13436102', '13136202', '13236202', '13336202', '13436202', '13138102', '13238102', '13338102', '13438102', '13138202', '13238202', '13338202', '13438202', '13140102', '13240102', '13340102', '13440102', '13140202', '13240202', '13340202', '13440202', '13142102', '13242102', '13342102', '13442102', '13142202', '13242202', '13342202', '13442202', '13144102', '13244102', '13344102', '13444102', '13144202', '13244202', '13344202', '13444202', '13146102', '13246102', '13346102', '13446102', '13146202', '13246202', '13346202', '13446202', '13148102', '13248102', '13348102', '13448102', '13148202', '13248202', '13348202', '13448202', '13150102', '13250102', '13350102', '13450102', '13150202', '13250202', '13350202', '13450202', '13152102', '13252102', '13352102', '13452102', '13152202', '13252202', '13352202', '13452202', '13154102', '13254102', '13354102', '13454102', '13154202', '13254202', '13354202', '13454202') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 6) IN ('189011', '189012') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 10) IN ('1810810102', '1810820102', '1810810202', '1810820202', '1810810302', '1810820302', '1810810402', '1810820402', '1810810502', '1810820502', '1881610102', '1881620102', '1881610202', '1881620202', '1881610302', '1881620302', '1881610402', '1881620402', '1881610502', '1881620502', '1881610602', '1881620602', '1881610702', '1881620702', '1881610802', '1881620802')) AND ATS_TH_AT03.SALDO <= 0);'''
    logger.info("Accion a ejecutarse: insert CK errors") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: insert CK errors, ejecutada exitosamente") 

    # insert CK errors
    sql_query_deftxt = f'''INSERT INTO <?=snpRef.getObjectName("L", "%ERR_PRFATS_TH_AT03", "W") ?> (ODI_PK, ODI_SESS_NO, ODI_ROW_ID, ODI_ERR_TYPE, ODI_ERR_MESS, ODI_CHECK_DATE, ODI_ORIGIN, ODI_CONS_NAME, ODI_CONS_TYPE, OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO)
SELECT SYS_GUID(), <?=snpRef.getSession("SESS_NO") ?>, rowid, 'S', '2- El valor de este campo debe ser >=0 cuando las cta. contables esten en los parametros Sudeban AT03-2', sysdate, 'ATS_TH_AT03', 'SALDO_2', 'CK', OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO
FROM <?=snpRef.getObjectName("L", "ATS_TH_AT03", "D") ?> ATS_TH_AT03
WHERE NOT (SUBSTR(ATS_TH_AT03.OFICINA, 1, 1) IN ('V', 'I') AND SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 1) = '1' AND (SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 5) NOT IN ('11901', '12125', '12225', '12325', '12425', '12625', '17249', '17349', '17449', '17549') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 3) NOT IN ('129', '139', '149', '159', '169') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 8) NOT IN ('18101102', '18101202', '18102102', '18102202', '18103102', '18103202', '18105102', '18105202', '18105104', '18105204', '18105106', '18105206', '18106102', '18106202', '18107102', '18107202', '18902102', '18902202', '18902101', '18902201', '13136102', '13236102', '13336102', '13436102', '13136202', '13236202', '13336202', '13436202', '13138102', '13238102', '13338102', '13438102', '13138202', '13238202', '13338202', '13438202', '13140102', '13240102', '13340102', '13440102', '13140202', '13240202', '13340202', '13440202', '13142102', '13242102', '13342102', '13442102', '13142202', '13242202', '13342202', '13442202', '13144102', '13244102', '13344102', '13444102', '13144202', '13244202', '13344202', '13444202', '13146102', '13246102', '13346102', '13446102', '13146202', '13246202', '13346202', '13446202', '13148102', '13248102', '13348102', '13448102', '13148202', '13248202', '13348202', '13448202', '13150102', '13250102', '13350102', '13450102', '13150202', '13250202', '13350202', '13450202', '13152102', '13252102', '13352102', '13452102', '13152202', '13252202', '13352202', '13452202', '13154102', '13254102', '13354102', '13454102', '13154202', '13254202', '13354202', '13454202') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 6) NOT IN ('189011', '189012') OR SUBSTR(ATS_TH_AT03.CODIGO_CONTABLE, 1, 10) NOT IN ('1810810102', '1810820102', '1810810202', '1810820202', '1810810302', '1810820302', '1810810402', '1810820402', '1810810502', '1810820502', '1881610102', '1881620102', '1881610202', '1881620202', '1881610302', '1881620302', '1881610402', '1881620402', '1881610502', '1881620502', '1881610602', '1881620602', '1881610702', '1881620702', '1881610802', '1881620802')) AND ATS_TH_AT03.SALDO >= 0);'''
    logger.info("Accion a ejecutarse: insert CK errors") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: insert CK errors, ejecutada exitosamente") 

    # create index on error table
    sql_query_deftxt = f''';'''
    logger.info("Accion a ejecutarse: create index on error table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: create index on error table, ejecutada exitosamente") 

    # delete errors from controlled table
    sql_query_deftxt = f'''DELETE FROM ATS_TH_AT03
WHERE EXISTS (
    SELECT 1
    FROM "%ERR_PRFATS_TH_AT03"
    WHERE ODI_SESS_NO = <?=snpRef.getSession("SESS_NO") ?>
    AND ATS_TH_AT03.rowid = "%ERR_PRFATS_TH_AT03".ODI_ROW_ID
);'''
    logger.info("Accion a ejecutarse: delete errors from controlled table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: delete errors from controlled table, ejecutada exitosamente") 



def AT03_CHECK_HIS(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # CHECK AND DELETE
    sql_query_deftxt = f'''DELETE FROM <?=snpRef.getObjectName('L', 'ATS_TH_AT03_H', '', '', 'D')?> WHERE FECHA_CIERRE IN ('{FechaFin}');'''
    logger.info("Accion a ejecutarse: CHECK AND DELETE") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: CHECK AND DELETE, ejecutada exitosamente") 

    # CHECK AND DELETE BG
    sql_query_deftxt = f'''DELETE FROM ATS_TH_AT03BG_H WHERE FECHA_CIERRE IN ('{FechaFin}');'''
    logger.info("Accion a ejecutarse: CHECK AND DELETE BG") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: CHECK AND DELETE BG, ejecutada exitosamente") 


def ATS_TH_AT03_H(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ATS_TH_AT03_H (OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO, FECHA_CIERRE)
SELECT OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO, '{FechaFin}'
FROM (
    SELECT ATS_TH_AT03.OFICINA, ATS_TH_AT03.CODIGO_CONTABLE, ATS_TH_AT03.SALDO, ATS_TH_AT03.CONSECUTIVO
    FROM ATS_TH_AT03
    WHERE (1 = 1)
) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def ATS_TH_AT03BG_H(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ATS_TH_AT03BG_H (OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO, FECHA_CIERRE)
SELECT
    OFICINA,
    CODIGO_CONTABLE,
    SALDO,
    CONSECUTIVO,
    '{FechaFin}'
FROM (
    SELECT
        OFICINA,
        CODIGO_CONTABLE,
        SALDO,
        CONSECUTIVO
    FROM ATS_TH_AT03BG
    WHERE (1 = 1)
) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 



def VALIDAR_DIA_HABIL(**kwargs):


def COMPARACION_FECHA(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT to_char(CURRENT_DATE, 'YY-MM-DD');'''
    result = hook.get_records(sql_query)

    Variable.set('COMPARACION_FECHA', serialize_value(result[0][0]))

def EXIST_COMPARACION(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT 
	CASE 
		WHEN '{COMPARACION_FECHA}' IN (SELECT MES_DIA FROM AT_DIAS_HABILES_FIN) THEN 1
		ELSE 0
	END 
FROM AT_DIAS_HABILES_FIN;'''
    result = hook.get_records(sql_query)

    Variable.set('EXIST_COMPARACION', serialize_value(result[0][0]))

def EXIST_COMPARACION_FINAL(**kwargs):

def COMPARACION_FECHA_FINAL(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT ORDINAL FROM AT_DIAS_HABILES_FIN WHERE '{COMPARACION_FECHA}' = MES_DIA;'''
    result = hook.get_records(sql_query)

    Variable.set('COMPARACION_FECHA_FINAL', serialize_value(result[0][0]))



###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT03_PRINCIPAL',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

wait_for_file = GCSObjectExistenceSensor(                  # En la secuencia de ejecucion, colocar este operador en el orden que convenga (Agregar un operador por cada insumo que use el AT)
    task_id='wait_for_file',
    bucket='airflow-dags-data', 
    object='data/AT#/INSUMOS/nombre_insumo.csv',           # Colocar nombre del insumo
    poke_interval=10,                                      # Intervalo en segundos para verificar
    timeout=60 * 10,                                       # Timeout en segundos (10 minutos para la prueba)
    dag=dag
)

holiday_sensor = HolidayCheckSensor(     # En la secuencia de ejecucion, colocar este operador en el orden que convenga
    task_id='holiday_sensor',
    postgres_conn_id='nombre_conexion',  # Colocar nombre de la conexion a la bd
    poke_interval=10,                    # Se verifica cada 10 segundos para la prueba (para produccion seria 86400seg para una verificacion diaria)
    dag=dag
)

GenerateGuid_task = PythonOperator(
    task_id='GenerateGuid_task',
    python_callable=GenerateGuid,
    dag=dag
)

FileAT_task = PythonOperator(
    task_id='FileAT_task',
    python_callable=FileAT,
    dag=dag
)

FileCodSupervisado_task = PythonOperator(
    task_id='FileCodSupervisado_task',
    python_callable=FileCodSupervisado,
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

FileName_Error_task = PythonOperator(
    task_id='FileName_Error_task',
    python_callable=FileName_Error,
    dag=dag
)

FechaFin_task = PythonOperator(
    task_id='FechaFin_task',
    python_callable=FechaFin,
    dag=dag
)

FileDate_task = PythonOperator(
    task_id='FileDate_task',
    python_callable=FileDate,
    dag=dag
)

RutaDirectorio_task = PythonOperator(
    task_id='RutaDirectorio_task',
    python_callable=RutaDirectorio,
    dag=dag
)

RutaDirectorioAnterior_task = PythonOperator(
    task_id='RutaDirectorioAnterior_task',
    python_callable=RutaDirectorioAnterior,
    dag=dag
)

FechaFile_task = PythonOperator(
    task_id='FechaFile_task',
    python_callable=FechaFile,
    dag=dag
)

AT03_Max_Periodo_task = PythonOperator(
    task_id='AT03_Max_Periodo_task',
    python_callable=AT03_Max_Periodo,
    dag=dag
)

AT03_Max_Corte_task = PythonOperator(
    task_id='AT03_Max_Corte_task',
    python_callable=AT03_Max_Corte,
    dag=dag
)

Execution_of_the_Scenario_AT03_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT03_version_001_task',
    trigger_dag_id='Execution_of_the_Scenario_AT03_version_001',  # Acomodar ID del DAG a disparar (Se usa el PackName)
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_AT03_TO_FILE_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT03_TO_FILE_version_001_task',
    trigger_dag_id='Execution_of_the_Scenario_AT03_TO_FILE_version_001',  # Acomodar ID del DAG a disparar (Se usa el PackName)
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_AT03_TO_FILE_BG_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT03_TO_FILE_BG_version_001_task',
    trigger_dag_id='Execution_of_the_Scenario_AT03_TO_FILE_BG_version_001',  # Acomodar ID del DAG a disparar (Se usa el PackName)
    wait_for_completion=True,
    dag=dag
)

ATS_TH_AT03_task = PythonOperator(
    task_id='ATS_TH_AT03_task',
    python_callable=ATS_TH_AT03,
    dag=dag
)

Execution_of_the_Scenario_AT03_TO_FILE_ERROR_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT03_TO_FILE_ERROR_version_001_task',
    trigger_dag_id='Execution_of_the_Scenario_AT03_TO_FILE_ERROR_version_001',  # Acomodar ID del DAG a disparar (Se usa el PackName)
    wait_for_completion=True,
    dag=dag
)

AT03_CHECK_HIS_task = PythonOperator(
    task_id='AT03_CHECK_HIS_task',
    python_callable=AT03_CHECK_HIS,
    dag=dag
)

ATS_TH_AT03_H_task = PythonOperator(
    task_id='ATS_TH_AT03_H_task',
    python_callable=ATS_TH_AT03_H,
    dag=dag
)

ATS_TH_AT03BG_H_task = PythonOperator(
    task_id='ATS_TH_AT03BG_H_task',
    python_callable=ATS_TH_AT03BG_H,
    dag=dag
)

COMPARACION_FECHA_task = PythonOperator(
    task_id='COMPARACION_FECHA_task',
    python_callable=COMPARACION_FECHA,
    dag=dag
)

EXIST_COMPARACION_task = PythonOperator(
    task_id='EXIST_COMPARACION_task',
    python_callable=EXIST_COMPARACION,
    dag=dag
)

COMPARACION_FECHA_FINAL_task = PythonOperator(
    task_id='COMPARACION_FECHA_FINAL_task',
    python_callable=COMPARACION_FECHA_FINAL,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
wait_for_file >> holiday_sensor >> GenerateGuid_task >> FileAT_task >> FileCodSupervisado_task >> FechaInicio_M_task >> FechaFin_M_task >> FechaInicio_task >> FileName_Error_task >> FechaFin_task >> FileDate_task >> RutaDirectorio_task >> RutaDirectorioAnterior_task >> FechaFile_task >> AT03_Max_Periodo_task >> AT03_Max_Corte_task >> Execution_of_the_Scenario_AT03_version_001_task >> Execution_of_the_Scenario_AT03_TO_FILE_version_001_task >> Execution_of_the_Scenario_AT03_TO_FILE_BG_version_001_task >> ATS_TH_AT03_task >> Execution_of_the_Scenario_AT03_TO_FILE_ERROR_version_001_task >> AT03_CHECK_HIS_task >> ATS_TH_AT03_H_task >> ATS_TH_AT03BG_H_task >> COMPARACION_FECHA_task >> EXIST_COMPARACION_task >> COMPARACION_FECHA_FINAL_task
