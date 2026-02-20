from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
from psycopg2.extras import execute_values
from decimal import Decimal
import json



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

def vSrcNombreTablaOds(**kwargs):
    value = 'CB_HIST_SALDO'
    Variable.set('vSrcNombreTablaOds', serialize_value(value))

def vSrcFechaCarga(**kwargs):
    value = '05/30/2025' # SELECT TO_DATE(CURRENT_DATE, 'MM/DD/YYYY')
    Variable.set('vSrcFechaCarga', serialize_value(value))

def vSrcFormatoFecha(**kwargs):
	value = 'MM/DD/YYYY'
	Variable.set('vSrcFormatoFecha', serialize_value(value))

def vSrcPeriodoFechaCarga(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

    sql_query = f'''SELECT cbc.co_periodo
    FROM ods.CB_CORTE AS cbc
    WHERE cbc.co_fecha_ini = TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}');'''
    result = hook.get_records(sql_query)

    if not result:
        # La consulta no devolvió ninguna fila → definimos un comportamiento
        logger.info(f"No se encontró co_periodo para vSrcPeriodoFechaCarga, asignando 0")
        Variable.set('vSrcPeriodoFechaCarga', serialize_value(0))
        return

    Variable.set('vSrcPeriodoFechaCarga', serialize_value(result[0][0]))

def vSrcCorteFechaCarga(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

    sql_query = f'''SELECT cbc.co_corte
    FROM ods.CB_CORTE AS cbc
    WHERE cbc.co_fecha_ini = TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}');'''
    result = hook.get_records(sql_query)

    if not result:
        # La consulta no devolvió ninguna fila → definimos un comportamiento
        logger.info(f"No se encontró co_corte para vSrcCorteFechaCarga, asignando 0")
        Variable.set('vSrcCorteFechaCarga', serialize_value(0))
        return

    Variable.set('vSrcCorteFechaCarga', serialize_value(result[0][0]))

def vSrcTipoCarga(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcNombreTablaOds = get_variable('vSrcNombreTablaOds')

    sql_query = f'''SELECT bcsd.COO_ATTRIBUTE1
    FROM ods.BAN_CONFIG_ODS AS bcsd
    WHERE bcsd.COO_SOURCE = '{vSrcNombreTablaOds}';'''
    result = hook.get_records(sql_query)

    Variable.set('vSrcTipoCarga', serialize_value(result[0][0]))

def IT_CB_HIST_SALDO_ODS_DIARIA(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='ods')
    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    vSrcPeriodoFechaCarga = get_variable('vSrcPeriodoFechaCarga')
    vSrcCorteFechaCarga = get_variable('vSrcCorteFechaCarga')
    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

    # Create work table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.COL_PRF0CB_HIST_SALDO (
	C1_HI_EMPRESA NUMERIC(3) NULL,
	C2_HI_CUENTA VARCHAR(20) NULL,
	C3_HI_OFICINA NUMERIC(10) NULL,
	C4_HI_AREA NUMERIC(10) NULL,
	C5_HI_CORTE NUMERIC(10) NULL,
	C6_HI_PERIODO NUMERIC(10) NULL,
	C7_HI_SALDO NUMERIC(32,7) NULL,
	C8_HI_SALDO_ME NUMERIC(32,7) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table") 
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
        CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
        LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
        CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
        CB_HIST_SALDO.hi_area AS C4_HI_AREA,
        CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
        CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
        CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
        CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
    FROM bancaribe_core.dbo.cb_hist_saldo AS CB_HIST_SALDO
    WHERE (CB_HIST_SALDO.hi_periodo = CONVERT(INT, '{vSrcPeriodoFechaCarga}'))
    AND (CB_HIST_SALDO.hi_empresa = 1)
    AND (CB_HIST_SALDO.hi_corte = CONVERT(INT, '{vSrcCorteFechaCarga}'))'''
    logger.info("Accion a ejecutarse: Load data") 

    # conexion a sybase
    sybase_conn = sybase_hook.get_conn()
    sybase_cursor = sybase_conn.cursor()
    sybase_cursor.execute(sql_query_coltxt)

    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0CB_HIST_SALDO (
    C1_HI_EMPRESA, 
    C2_HI_CUENTA, 
    C3_HI_OFICINA, 
    C4_HI_AREA, 
    C5_HI_CORTE, 
    C6_HI_PERIODO, 
    C7_HI_SALDO, 
    C8_HI_SALDO_ME
    ) 
    VALUES %s'''
    logger.info("Accion a ejecutarse: Load data") 

    # insercion por lotes en postgres
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    batch_size = 10000
    total_inserted = 0
    batch_num = 1

    while True:
        rows = sybase_cursor.fetchmany(batch_size)
        if not rows:
            break
        execute_values(pg_cursor, sql_query_deftxt, rows)
        pg_conn.commit()

        total_inserted += len(rows)
        logger.info(f"Lote {batch_num}: insertados {len(rows)} registros (Total acumulado: {total_inserted})")

        batch_num += 1

    # cerrar conexiones
    sybase_cursor.close()
    sybase_conn.close()
    pg_cursor.close()
    pg_conn.close()

    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target  table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.cb_hist_saldo (
	HI_EMPRESA NUMERIC(3),
	HI_CUENTA VARCHAR(20),
	HI_OFICINA NUMERIC(10),
	HI_AREA NUMERIC(10),
	HI_CORTE NUMERIC(10),
	HI_PERIODO NUMERIC(10),
	HI_SALDO NUMERIC(32,7),
	HI_SALDO_ME NUMERIC(32,7),
    HI_DEBITO NUMERIC(32,7),
    HI_CREDITO NUMERIC(32,7),
    HI_DEBITO_ME NUMERIC(32,7),
    HI_CREDITO_ME NUMERIC(32,7),
    HI_FECHA_INI DATE,
    HI_FECHA_FIN DATE
    );'''
    logger.info("Accion a ejecutarse: create target table") 
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''DELETE FROM ods.cb_hist_saldo
    WHERE hi_fecha_ini = TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}');'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ods.cb_hist_saldo (
	hi_empresa,
	hi_cuenta,
	hi_oficina,
	hi_area,
	hi_corte,
	hi_periodo,
	hi_saldo,
	hi_saldo_me,
	hi_fecha_ini,
	hi_fecha_fin
    )
    SELECT
        hi_empresa,
        hi_cuenta,
        hi_oficina,
        hi_area,
        hi_corte,
        hi_periodo,
        hi_saldo,
        hi_saldo_me,
        TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}') AS hi_fecha_ini,
        TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}') AS hi_fecha_fin
    FROM (
        SELECT
            c1_hi_empresa AS hi_empresa,
            c2_hi_cuenta AS hi_cuenta,
            c3_hi_oficina AS hi_oficina,
            c4_hi_area AS hi_area,
            c5_hi_corte AS hi_corte,
            c6_hi_periodo AS hi_periodo,
            c7_hi_saldo AS hi_saldo,
            c8_hi_saldo_me AS hi_saldo_me
        FROM ods.col_prf0cb_hist_saldo
    );'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Drop work table
    sql_query_deftxt = f'''DROP TABLE ods.COL_PRF0CB_HIST_SALDO;'''
    logger.info("Accion a ejecutarse: Drop work table") 
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Drop work table, ejecutada exitosamente") 


###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_CON_LOAD_CB_HIST_SALDO_ODS_DIARIA_version2',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

vSrcNombreTablaOds_task = PythonOperator(
    task_id='vSrcNombreTablaOds_task',
    python_callable=vSrcNombreTablaOds,
    dag=dag
)

vSrcFechaCarga_task = PythonOperator(
    task_id='vSrcFechaCarga_task',
    python_callable=vSrcFechaCarga,
    dag=dag
)

vSrcFormatoFecha_task = PythonOperator(
	task_id='vSrcFormatoFecha_task',
	python_callable=vSrcFormatoFecha,
	dag=dag
)

vSrcPeriodoFechaCarga_task = PythonOperator(
    task_id='vSrcPeriodoFechaCarga_task',
    python_callable=vSrcPeriodoFechaCarga,
    dag=dag
)

vSrcCorteFechaCarga_task = PythonOperator(
    task_id='vSrcCorteFechaCarga_task',
    python_callable=vSrcCorteFechaCarga,
    dag=dag
)

vSrcTipoCarga_task = PythonOperator(
    task_id='vSrcTipoCarga_task',
    python_callable=vSrcTipoCarga,
    dag=dag
)

# IT_CB_HIST_SALDO_ODS_INICIAL_task = PythonOperator(
#     task_id='IT_CB_HIST_SALDO_ODS_INICIAL_task',
#     python_callable=IT_CB_HIST_SALDO_ODS_INICIAL,
##     dag=dag
# )

IT_CB_HIST_SALDO_ODS_DIARIA_task = PythonOperator(
    task_id='IT_CB_HIST_SALDO_ODS_DIARIA_task',
    python_callable=IT_CB_HIST_SALDO_ODS_DIARIA,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vSrcNombreTablaOds_task >> vSrcFechaCarga_task >> vSrcFormatoFecha_task >> vSrcPeriodoFechaCarga_task >> vSrcCorteFechaCarga_task >> vSrcTipoCarga_task >> IT_CB_HIST_SALDO_ODS_DIARIA_task
