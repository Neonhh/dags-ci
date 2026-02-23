from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
import time



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

def AT_DIA_HABIL(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sql_query = '''SELECT 0;'''
    result = hook.get_records(sql_query)
    Variable.set('AT_DIA_HABIL', serialize_value(result[0][0]))

def COMPARACION_FECHA(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YY-MM-DD');'''
    result = hook.get_records(sql_query)
    Variable.set('COMPARACION_FECHA', serialize_value(result[0][0]))

# def EXIST_COMPARACION(**kwargs):
#     hook = PostgresHook(postgres_conn_id='repodataprd')
#     sql_query = '''SELECT
#     CASE
#         WHEN 'COMPARACION_FECHA' IN (SELECT MES_DIA FROM REPODATAPRD.AT_DIAS_HABILES_FIN) THEN 1
#         ELSE 0
#     END;'''
#     result = hook.get_records(sql_query)
#     Variable.set('EXIST_COMPARACION', serialize_value(result[0][0]))

# def COMPARACION_FECHA_FINAL(**kwargs):
#     hook = PostgresHook(postgres_conn_id='repodataprd')
#     sql_query = '''SELECT ORDINAL FROM REPODATAPRD.AT_DIAS_HABILES_FIN WHERE ('COMPARACION_FECHA' = AT_DIAS_HABILES_FIN.MES_DIA);'''
#     result = hook.get_records(sql_query)
#     Variable.set('COMPARACION_FECHA_FINAL', serialize_value(result[0][0]))

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

def FileAT_at03(**kwargs):
    value = 'AT03'
    Variable.set('FileAT_at03', serialize_value(value))

def FileCodSupervisado(**kwargs):
    value = '01410'
    Variable.set('FileCodSupervisado', serialize_value(value))

def FechaInicio_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days'), 'MM/DD/YYYY')'''
    result = hook.get_records(sql_query)
    Variable.set('FechaInicio_M', serialize_value(result[0][0]))

def FechaFin_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month - 1 day', 'mm/dd/yyyy');'''
    result = hook.get_records(sql_query)
    Variable.set('FechaFin_M', serialize_value(result[0][0]))

def FechaFin(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	FechaFin_M = get_variable('FechaFin_M')

	sql_query = f'''SELECT '{FechaFin_M}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechaFin', serialize_value(result[0][0]))

def FechaInicio(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	FechaInicio_M = get_variable('FechaInicio_M')

	sql_query = f'''SELECT '{FechaInicio_M}'; '''
	result = hook.get_records(sql_query)
	Variable.set('FechaInicio', serialize_value(result[0][0]))

def FechaFile(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	FechaFin = get_variable('FechaFin')

	sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechaFin}', 'MM/DD/YY'), 'YYMMDD') AS result;'''
	result = hook.get_records(sql_query)
	Variable.set('FechaFile', serialize_value(result[0][0]))

def FileDate(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
    result = hook.get_records(sql_query)
    Variable.set('FileDate', serialize_value(result[0][0]))

def AT03_Max_Periodo(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sql_query = '''SELECT CASE 
                        WHEN TO_CHAR(CURRENT_DATE, 'MM') = '01' 
                            THEN '2' || (TO_CHAR(CURRENT_DATE, 'YY')::INTEGER - 1) 
                        WHEN TO_CHAR(CURRENT_DATE, 'MM') < '08' 
                            THEN '1' || TO_CHAR(CURRENT_DATE, 'YY') 
                        ELSE 
                            '2' || TO_CHAR(CURRENT_DATE, 'YY') 
                    END;'''
    result = hook.get_records(sql_query)
    Variable.set('AT03_Max_Periodo', serialize_value(result[0][0]))

def AT03_Max_Corte(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    AT03_Max_Periodo = get_variable('AT03_Max_Periodo')

    sql_query = f'''
    SELECT MAX(co_corte)
    FROM ods.cb_corte
    WHERE co_periodo::TEXT = '{AT03_Max_Periodo}'
    AND TO_CHAR(co_fecha_fin::date, 'yyyymmdd') = TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month' - INTERVAL '1 day','YYYYMMDD')
    GROUP BY co_periodo;'''

    result = hook.get_records(sql_query)  # Puede ser [] si no hay datos
    if not result:
        # La consulta no devolvió ninguna fila → definimos un comportamiento
        Variable.set('AT03_Max_Corte', serialize_value(0))
        return

    max_corte = result[0][0]
    Variable.set('AT03_Max_Corte', serialize_value(max_corte))

# def ATS_TH_AT03_DATACHECK_TO_E(**kwargs):
#     hook = PostgresHook(postgres_conn_id='ods')

#     # CREAR TABLA DESTINO
#     sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.E$_ATS_TH_AT03 (
# 	OFICINA VARCHAR(5),
# 	CODIGO_CONTABLE VARCHAR(10),
# 	SALDO NUMERIC(32,7),
# 	CONSECUTIVO VARCHAR(1)
#     );'''
#     hook.run(sql_query_deftxt)

#     # Vaciamos la tabla destino antes de la carga
#     sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.E$_ATS_TH_AT03;'''
#     hook.run(sql_query_deftxt)

#     # INSERTAR DATOS EN DESTINO
#     sql_query_deftxt = '''INSERT INTO ATSUDEBAN.E$_ATS_TH_AT03 (
# 	OFICINA,
# 	CODIGO_CONTABLE,
# 	SALDO,
# 	CONSECUTIVO
#     )
#     SELECT
#         OFICINA,
#         CODIGO_CONTABLE,
#         SALDO,
#         CONSECUTIVO
#     FROM ATSUDEBAN.ATS_TH_AT03
#     WHERE NOT (
#         (
#             SUBSTRING(OFICINA FROM 1 FOR 1) IN ('V','I')
#         )
#         AND (
#             SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 5) IN ('11901','12125','12225','12325','12425','12625','17249','17349','17449','17549')
#             OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 3) IN ('129','139','149','159','169')
#             OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 8) IN (
#                 '18101102','18101202','18102102','18102202','18103102','18103202','18105102','18105202','18105104',
#                 '18105204','18105106','18105206','18106102','18106202','18107102','18107202','18902102','18902202','18902101',
#                 '18902201','13136102','13236102','13336102','13436102','13136202','13236202','13336202','13436202','13138102',
#                 '13238102','13338102','13438102','13138202','13238202','13338202','13438202','13140102','13240102','13340102',
#                 '13440102','13140202','13240202','13340202','13440202','13142102','13242102','13342102','13442102','13142202',
#                 '13242202','13342202','13442202','13144102','13244102','13344102','13444102','13144202','13244202','13344202',
#                 '13444202','13146102','13246102','13346102','13446102','13146202','13246202','13346202','13446202','13148102',
#                 '13248102','13348102','13448102','13148202','13248202','13348202','13448202','13150102','13250102','13350102',
#                 '13450102','13150202','13250202','13350202','13450202','13152102','13252102','13352102','13452102','13152202',
#                 '13252202','13352202','13452202','13154102','13254102','13354102','13454102','13154202','13254202','13354202',
#                 '13454202'
#             )
#             OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 6) IN ('189011','189012')
#             OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 10) IN (
#                 '1810810102','1810820102','1810810202','1810820202','1810810302','1810820302','1810810402','1810820402','1810810502','1810820502','1881610102',
#                 '1881620102','1881610202','1881620202','1881610302','1881620302','1881610402','1881620402','1881610502','1881620502','1881610602',
#                 '1881620602','1881610702','1881620702','1881610802','1881620802'
#             )
#         )
#         AND SALDO <= 0
#         AND ( SUBSTRING(oficina FROM 1 FOR 1) IN ('V','I') ) AND ( SUBSTRING(codigo_contable FROM 1 FOR 1) = '1' )
#         AND (
#             SUBSTRING(codigo_contable FROM 1 FOR 5) NOT IN ('11901','12125','12225','12325','12425','12625','17249','17349','17449','17549')
#             OR SUBSTRING(codigo_contable FROM 1 FOR 3) NOT IN ('129','139','149','159','169')
#             OR SUBSTRING(codigo_contable FROM 1 FOR 8) NOT IN (
#                 '18101102','18101202','18102102','18102202','18103102','18103202','18105102','18105202','18105104',
#                 '18105204','18105106','18105206','18106102','18106202','18107102','18107202','18902102','18902202','18902101',
#                 '18902201','13136102','13236102','13336102','13436102','13136202','13236202','13336202','13436202','13138102',
#                 '13238102','13338102','13438102','13138202','13238202','13338202','13438202','13140102','13240102','13340102',
#                 '13440102','13140202','13240202','13340202','13440202','13142102','13242102','13342102','13442102','13142202',
#                 '13242202','13342202','13442202','13144102','13244102','13344102','13444102','13144202','13244202','13344202',
#                 '13444202','13146102','13246102','13346102','13446102','13146202','13246202','13346202','13446202','13148102',
#                 '13248102','13348102','13448102','13148202','13248202','13348202','13448202','13150102','13250102','13350102',
#                 '13450102','13150202','13250202','13350202','13450202','13152102','13252102','13352102','13452102','13152202',
#                 '13252202','13352202','13452202','13154102','13254102','13354102','13454102','13154202','13254202','13354202',
#                 '13454202'
#             )
#             OR SUBSTRING(codigo_contable FROM 1 FOR 6) NOT IN ('189011','189012')
#             OR SUBSTRING(codigo_contable FROM 1 FOR 10) NOT IN (
#                 '1810810102','1810820102','1810810202','1810820202','1810810302','1810820302','1810810402','1810820402','1810810502','1810820502','1881610102',
#                 '1881620102','1881610202','1881620202','1881610302','1881620302','1881610402','1881620402','1881610502','1881620502','1881610602',
#                 '1881620602','1881610702','1881620702','1881610802','1881620802'
#             )
#         )
#         AND saldo >= 0
#     );'''
#     hook.run(sql_query_deftxt)


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
          catchup=False,
          tags=['Principal'])

AT_DIA_HABIL_task = PythonOperator(
    task_id='AT_DIA_HABIL_task',
    python_callable=AT_DIA_HABIL,
    dag=dag
)

holiday_sensor = HolidayCheckSensor(
    task_id='holiday_sensor',
    postgres_conn_id='ods',
    poke_interval=10,                    # Se verifica cada 10 segundos para la prueba (para produccion seria 86400seg para una verificacion diaria)
    dag=dag
)

COMPARACION_FECHA_task = PythonOperator(
    task_id='COMPARACION_FECHA_task',
    python_callable=COMPARACION_FECHA,
    dag=dag
)

# EXIST_COMPARACION_task = PythonOperator(
#     task_id='EXIST_COMPARACION_task',
#     python_callable=EXIST_COMPARACION,
##     dag=dag
# )

# COMPARACION_FECHA_FINAL_task = PythonOperator(
#     task_id='COMPARACION_FECHA_FINAL_task',
#     python_callable=COMPARACION_FECHA_FINAL,
##     dag=dag
# )

FileAT_task = PythonOperator(
    task_id='FileAT_task',
    python_callable=FileAT_at03,
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

FechaFin_task = PythonOperator(
	task_id='FechaFin_task',
	python_callable=FechaFin,
	dag=dag
)

FechaInicio_task = PythonOperator(
	task_id='FechaInicio_task',
	python_callable=FechaInicio,
	dag=dag
)

FechaFile_task = PythonOperator(
	task_id='FechaFile_task',
	python_callable=FechaFile,
	dag=dag
)

FileDate_task = PythonOperator(
    task_id='FileDate_task',
    python_callable=FileDate,
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
    trigger_dag_id='AT03',
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_AT03_TO_FILE_version_001_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT03_TO_FILE_version_001_task',
    trigger_dag_id='AT03_TO_FILE',
    wait_for_completion=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT_DIA_HABIL_task >> holiday_sensor >> COMPARACION_FECHA_task >> FileAT_task >> FileCodSupervisado_task >> FechaInicio_M_task >> FechaFin_M_task >> FechaFin_task >> FechaInicio_task >> FechaFile_task >> FileDate_task >> AT03_Max_Periodo_task >> AT03_Max_Corte_task >> Execution_of_the_Scenario_AT03_version_001_task >> Execution_of_the_Scenario_AT03_TO_FILE_version_001_task
