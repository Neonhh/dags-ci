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
def vSrcNombreTablaOds(**kwargs):
    value = 'CL_DIAS_FERIADOS'
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
        logger.info("No se encontró co_periodo para vSrcPeriodoFechaCarga, asignando 0")
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
        logger.info("No se encontró co_corte para vSrcCorteFechaCarga, asignando 0")
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


def vSrcFirstDayMonth(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

    sql_query = f'''SELECT TO_CHAR(date_trunc('month', TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}')), 'MM/DD/YYYY');'''
    #sql_query = f'''SELECT TO_CHAR(TRUNC(TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}'), 'MM'), '{vSrcFormatoFecha}') FROM dual;'''
    result = hook.get_records(sql_query)

    if not result:
        # La consulta no devolvió ninguna fila → definimos un comportamiento
        logger.info("No se encontró registros, asignando 0")
        Variable.set('vSrcFirstDayMonth', serialize_value(0))
        return

    Variable.set('vSrcFirstDayMonth', serialize_value(result[0][0]))

def vSrcLastDayMonth(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

    sql_query = f'''SELECT TO_CHAR((DATE_TRUNC('month', TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}')) + INTERVAL '1 month' - INTERVAL '1 day'),'MM/DD/YYYY');'''
    #sql_query = f'''SELECT TO_CHAR(LAST_DAY(TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}')), '{vSrcFormatoFecha}') FROM dual;'''
    result = hook.get_records(sql_query)

    if not result:
        # La consulta no devolvió ninguna fila → definimos un comportamiento
        logger.info("No se encontró registros, asignando 0")
        Variable.set('vSrcLastDayMonth', serialize_value(0))
        return

    Variable.set('vSrcLastDayMonth', serialize_value(result[0][0]))


def IT_CL_DIAS_FERIADOS_COBIS_DIARIA(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='ods')
    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')
    vSrcFirstDayMonth = get_variable('vSrcFirstDayMonth')
    vSrcLastDayMonth = get_variable('vSrcLastDayMonth')

    # Create work table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ODS.CL_DIAS_FERIADOS_TMP_1 (
	DF_CIUDAD NUMERIC(5) NULL,
	DF_FECHA DATE NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente")

    # Load data
    sql_query_coltxt = f'''SELECT
        CL_DIAS_FERIADOS.df_ciudad C1_DF_CIUDAD,
        CL_DIAS_FERIADOS.df_fecha AS C2_DF_FECHA
    FROM bancaribe_core.dbo.cl_dias_feriados AS CL_DIAS_FERIADOS
    WHERE CL_DIAS_FERIADOS.df_ciudad = 0
    AND (CL_DIAS_FERIADOS.df_fecha>= CONVERT(DATETIME,'{vSrcFirstDayMonth}')  AND CL_DIAS_FERIADOS.df_fecha<= CONVERT(DATETIME,'{vSrcLastDayMonth}'))'''
    logger.info("Accion a ejecutarse: Load data")

    # conexion a sybase
    sybase_conn = sybase_hook.get_conn()
    sybase_cursor = sybase_conn.cursor()
    sybase_cursor.execute(sql_query_coltxt)

    logger.info("Accion: Load data, ejecutada exitosamente")

    # Load data
    sql_query_deftxt = '''INSERT INTO ODS.CL_DIAS_FERIADOS_TMP_1 (
    DF_CIUDAD, 
    DF_FECHA
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


        # Create tmp 2  table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ODS.CL_DIAS_FERIADOS_TMP_2 (
	DF_CIUDAD NUMERIC(5) NULL,
	DF_FECHA DATE NULL,
	DF_YEAR VARCHAR(10) NULL
    );'''
    logger.info("Accion a ejecutarse: create target table")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''TRUNCATE TABLE ODS.CL_DIAS_FERIADOS_TMP_2;'''
    logger.info("Accion a ejecutarse: Truncate target table")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ODS.CL_DIAS_FERIADOS_TMP_2 (
	DF_CIUDAD,
	DF_FECHA,
	DF_YEAR
    )
    SELECT
        DF_CIUDAD,
        DF_FECHA,
        TO_CHAR(TO_DATE('{vSrcFirstDayMonth}', '{vSrcFormatoFecha}'), 'YYYYMM')        
    FROM ODS.CL_DIAS_FERIADOS_TMP_1;'''
    logger.info("Accion a ejecutarse: Insert new rows")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")


    # Create target  table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ODS.CL_DIAS_FERIADOS (
	DF_CIUDAD NUMERIC(5) NULL,
	DF_FECHA DATE NULL,
	DF_YEAR VARCHAR(10) NULL
    );'''
    logger.info("Accion a ejecutarse: create target table")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''DELETE FROM ODS.CL_DIAS_FERIADOS a
    WHERE EXISTS (SELECT 1 FROM ODS.CL_DIAS_FERIADOS_TMP_2 b WHERE a.DF_YEAR = b.DF_YEAR);'''
    logger.info("Accion a ejecutarse: Truncate target table")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = '''INSERT INTO ODS.CL_DIAS_FERIADOS (
	DF_CIUDAD,
	DF_FECHA,
	DF_YEAR
    )
    SELECT
        DF_CIUDAD,
        DF_FECHA,
        DF_YEAR
        FROM ODS.CL_DIAS_FERIADOS_TMP_2;'''
    logger.info("Accion a ejecutarse: Insert new rows")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Drop work table
    sql_query_deftxt = '''DROP TABLE ODS.CL_DIAS_FERIADOS_TMP_1;'''
    logger.info("Accion a ejecutarse: Drop work table")
    pg_hook.run(sql_query_deftxt)
    logger.info("Accion: Drop work table, ejecutada exitosamente")

        # Drop work table
    sql_query_deftxt = '''DROP TABLE ODS.CL_DIAS_FERIADOS_TMP_2;'''
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

dag = DAG(dag_id='PKG_COB_LOAD_CL_DIAS_FERIADOS_COBIS_ODS',
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

vSrcFirstDayMonth_task = PythonOperator(
    task_id='vSrcFirstDayMonth_task',
    python_callable=vSrcFirstDayMonth,
    dag=dag
)

vSrcLastDayMonth_task = PythonOperator(
    task_id='vSrcLastDayMonth_task',
    python_callable=vSrcLastDayMonth,
    dag=dag
)

IT_CL_DIAS_FERIADOS_COBIS_DIARIA_task = PythonOperator(
    task_id='IT_CL_DIAS_FERIADOS_COBIS_DIARIA_task',
    python_callable=IT_CL_DIAS_FERIADOS_COBIS_DIARIA,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vSrcNombreTablaOds_task >> vSrcFechaCarga_task >> vSrcFormatoFecha_task >> vSrcPeriodoFechaCarga_task >> vSrcCorteFechaCarga_task >> vSrcTipoCarga_task >> vSrcFirstDayMonth_task >> vSrcLastDayMonth_task >> IT_CL_DIAS_FERIADOS_COBIS_DIARIA_task
