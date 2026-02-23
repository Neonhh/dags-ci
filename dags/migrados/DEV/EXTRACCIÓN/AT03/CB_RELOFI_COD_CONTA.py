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



logger = logging.getLogger(__name__)
### FUNCIONES DE CADA TAREA ###
def IT_CB_RELOFI_COB_CONTA_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    # Create work table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.COL_PRF0CB_RELOFI (
	C1_RE_FILIAL	NUMERIC(3) NULL,
	C2_RE_EMPRESA	NUMERIC(3) NULL,
	C3_RE_OFADMIN	NUMERIC(5) NULL,
	C4_RE_OFCONTA	NUMERIC(8) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente")

    # Load data
    sql_query_coltxt = '''SELECT
        CB_RELOFI.re_filial AS C1_RE_FILIAL,
        CB_RELOFI.re_empresa AS C2_RE_EMPRESA,
        CB_RELOFI.re_ofadmin AS C3_RE_OFADMIN,
        CB_RELOFI.re_ofconta AS C4_RE_OFCONTA
    FROM bancaribe_core.dbo.cb_relofi AS CB_RELOFI
    WHERE (CB_RELOFI.re_empresa = 1)'''
    logger.info("Accion a ejecutarse: Load data")

    # conexion a sybase
    sybase_conn = sybase_hook.get_conn()
    sybase_cursor = sybase_conn.cursor()
    sybase_cursor.execute(sql_query_coltxt)

    logger.info("Accion: Load data, ejecutada exitosamente")

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0CB_RELOFI (
    C1_RE_FILIAL, 
    C2_RE_EMPRESA, 
    C3_RE_OFADMIN, 
    C4_RE_OFCONTA
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
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.cb_relofi (
	RE_FILIAL	NUMERIC(3),
	RE_EMPRESA	NUMERIC(3),
	RE_OFADMIN	NUMERIC(5),
	RE_OFCONTA	NUMERIC(8)
    );'''
    logger.info("Accion a ejecutarse: create target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''TRUNCATE TABLE ods.cb_relofi;'''
    logger.info("Accion a ejecutarse: Truncate target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = '''INSERT INTO ods.cb_relofi (
	re_filial,
	re_empresa,
	re_ofadmin,
	re_ofconta
    )
    SELECT
        re_filial,
        re_empresa,
        re_ofadmin,
        re_ofconta
    FROM (
        SELECT
            c1_re_filial AS re_filial,
            c2_re_empresa AS re_empresa,
            c3_re_ofadmin AS re_ofadmin,
            c4_re_ofconta AS re_ofconta
        FROM ods.col_prf0cb_relofi AS col_prf0cb_relofi
    );'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Drop work table
    sql_query_deftxt = '''DROP TABLE ods.COL_PRF0CB_RELOFI;'''
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

dag = DAG(dag_id='PKG_LOAD_CB_RELOFI_COB_CONTA_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_CB_RELOFI_COB_CONTA_ODS_task = PythonOperator(
    task_id='IT_CB_RELOFI_COB_CONTA_ODS_task',
    python_callable=IT_CB_RELOFI_COB_CONTA_ODS,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_CB_RELOFI_COB_CONTA_ODS_task
