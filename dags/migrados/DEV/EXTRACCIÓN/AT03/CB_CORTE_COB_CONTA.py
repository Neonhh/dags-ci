from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from psycopg2.extras import execute_values
from decimal import Decimal



logger = logging.getLogger(__name__)
### FUNCIONES DE CADA TAREA ###
def IT_CB_CORTE_COB_CONTA__ODS_DIARIA(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    # Create work table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.COL_PRF0CB_CORTE (
	c1_co_corte NUMERIC(10) NULL,
	c2_co_periodo NUMERIC(10) NULL,
	c3_co_empresa NUMERIC(10) NULL,
	c4_co_fecha_ini DATE NULL,
	c5_co_fecha_fin DATE NULL,
	c6_co_estado VARCHAR(1) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente")

    # Load data
    sql_query_coltxt = '''SELECT
        cb_corte.co_corte AS c1_co_corte,
        cb_corte.co_periodo AS c2_co_periodo,
        cb_corte.co_empresa AS c3_co_empresa,
        cb_corte.co_fecha_ini AS c4_co_fecha_ini,
        cb_corte.co_fecha_fin AS c5_co_fecha_fin,
        cb_corte.co_estado AS c6_co_estado
    FROM bancaribe_core.dbo.cb_corte AS cb_corte
    WHERE (cb_corte.co_empresa = 1)
    AND (cb_corte.co_fecha_fin >= convert(char(10) , dateadd(day,-365, convert(date,getdate()) ) , 23 ))'''
    logger.info("Accion a ejecutarse: Load data")

    # conexion a sybase
    sybase_conn = sybase_hook.get_conn()
    sybase_cursor = sybase_conn.cursor()
    sybase_cursor.execute(sql_query_coltxt)

    logger.info("Accion: Load data, ejecutada exitosamente")

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0CB_CORTE (
    C1_CO_CORTE, 
    C2_CO_PERIODO, 
    C3_CO_EMPRESA, 
    C4_CO_FECHA_INI, 
    C5_CO_FECHA_FIN, 
    C6_CO_ESTADO
    ) 
    VALUES %s'''
    logger.info("Accion a ejecutarse: Load data")

    # insercion por lotes en postgres
    pg_conn = hook.get_conn()
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


    # create target table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.cb_corte (
	co_corte NUMERIC(10),
	co_periodo NUMERIC(10),
	co_empresa NUMERIC(10),
	co_fecha_ini DATE,
	co_fecha_fin DATE,
	co_estado VARCHAR(1)
    );'''
    logger.info("Accion a ejecutarse: create target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''TRUNCATE TABLE ods.cb_corte;'''
    logger.info("Accion a ejecutarse: Truncate target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = '''INSERT INTO ods.cb_corte (
	co_corte,
	co_periodo,
	co_empresa,
	co_fecha_ini,
	co_fecha_fin,
	co_estado
    )
    SELECT
        co_corte,
        co_periodo,
        co_empresa,
        co_fecha_ini,
        co_fecha_fin,
        co_estado
    FROM (
        SELECT
            c1_co_corte AS co_corte,
            c2_co_periodo AS co_periodo,
            c3_co_empresa AS co_empresa,
            c4_co_fecha_ini AS co_fecha_ini,
            c5_co_fecha_fin AS co_fecha_fin,
            c6_co_estado AS co_estado
        FROM ods.COL_PRF0CB_CORTE
    );'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Drop work table
    sql_query_deftxt = '''DROP TABLE ods.COL_PRF0CB_CORTE;'''
    logger.info("Accion a ejecutarse: Drop work table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Drop work table, ejecutada exitosamente")



###### DEFINICION DEL DAG ######

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_CON_LOAD_CB_CORTE_COB_CONTA_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_CB_CORTE_COB_CONTA__ODS_DIARIA_task = PythonOperator(
    task_id='IT_CB_CORTE_COB_CONTA__ODS_DIARIA_task',
    python_callable=IT_CB_CORTE_COB_CONTA__ODS_DIARIA,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_CB_CORTE_COB_CONTA__ODS_DIARIA_task
