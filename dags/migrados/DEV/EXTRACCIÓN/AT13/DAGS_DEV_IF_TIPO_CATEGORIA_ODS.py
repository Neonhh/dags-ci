from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from decimal import Decimal



logger = logging.getLogger(__name__)
### FUNCIONES DE CADA TAREA ###
def IT_IF_TIPO_CATEGORIA_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    # Create work table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.COL_PRF0IF_TIPO_CATEGORIA (
	c1_tc_codigo VARCHAR(5) NULL,
	c2_tc_categoria VARCHAR(50) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente")

    # Load data
    sql_query_coltxt = '''SELECT
        if_tipo_categoria.tc_codigo AS c1_tc_codigo,
        if_tipo_categoria.tc_categoria AS c2_tc_categoria
    FROM bancaribe_core.dbo.if_tipo_categoria AS if_tipo_categoria'''

    conn = sybase.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_query_coltxt)
    registros = cursor.fetchall()  # <-- Aqui estan los datos
    cursor.close()
    conn.close()

    logger.info("Accion: Load data, ejecutada exitosamente")

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0IF_TIPO_CATEGORIA (
    C1_tc_codigo, 
    C2_tc_categoria  
    ) 
    VALUES 
    (%s, %s);'''
    logger.info("Accion a ejecutarse: Load data")
    for row in registros:
        hook.run(sql_query_deftxt, parameters=row)
    logger.info("Accion: Load data, ejecutada exitosamente")


    # create target table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.if_tipo_categoria (
	tc_codigo VARCHAR(5),
	tc_categoria VARCHAR(50)
    );'''
    logger.info("Accion a ejecutarse: create target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''TRUNCATE TABLE ods.if_tipo_categoria;'''
    logger.info("Accion a ejecutarse: Truncate target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = '''INSERT INTO ods.if_tipo_categoria (
	tc_codigo,
	tc_categoria
    )
    SELECT
        tc_codigo,
        tc_categoria
    FROM (
        SELECT
            c1_tc_codigo AS tc_codigo,
            c2_tc_categoria AS tc_categoria
        FROM ods.COL_PRF0IF_TIPO_CATEGORIA
    );'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Drop work table
    sql_query_deftxt = '''DROP TABLE ods.COL_PRF0IF_TIPO_CATEGORIA;'''
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

dag = DAG(dag_id='PKG_CON_LOAD_IF_TIPO_CATEGORIA_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_IF_TIPO_CATEGORIA_ODS_task = PythonOperator(
    task_id='IT_IF_TIPO_CATEGORIA_ODS_task',
    python_callable=IT_IF_TIPO_CATEGORIA_ODS,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_IF_TIPO_CATEGORIA_ODS_task
