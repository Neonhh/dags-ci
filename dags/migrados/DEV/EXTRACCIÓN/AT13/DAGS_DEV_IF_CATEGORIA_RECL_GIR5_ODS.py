from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal



logger = logging.getLogger(__name__)
### FUNCIONES DE CADA TAREA ###
def IT_IF_CATEGORIA_RECL_GIR5_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    # Create work table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.COL_PRF0IF_CATEGORIA_RECL_GIR5 (
	c1_tc_secuencial NUMERIC(10) NULL,
	c2_tc_cod_categ VARCHAR(5) NULL,
	c3_tc_cod_recl VARCHAR(3) NULL,
	c4_tc_tipo_recl VARCHAR(65) NULL,
	c5_tc_estado VARCHAR(10) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente")

    # Load data
    sql_query_coltxt = '''SELECT
        if_categoria_recl_gir5.tc_secuencial AS c1_tc_secuencial,
        if_categoria_recl_gir5.tc_cod_categ AS c2_tc_cod_categ,
		if_categoria_recl_gir5.tc_cod_recl AS c3_tc_cod_recl,
		if_categoria_recl_gir5.tc_tipo_recl AS c4_tc_tipo_recl,
		if_categoria_recl_gir5.tc_estado AS c5_tc_estado		
	FROM bancaribe_core.dbo.if_categoria_recl_gir5 AS if_categoria_recl_gir5'''

    conn = sybase.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_query_coltxt)
    registros = cursor.fetchall()  # <-- Aqui estan los datos
    cursor.close()
    conn.close()

    logger.info("Accion: Load data, ejecutada exitosamente")

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0IF_CATEGORIA_RECL_GIR5 (
    C1_tc_secuencial, 
    C2_tc_cod_categ,
	C3_tc_cod_recl,
	C4_tc_tipo_recl,
	C5_tc_estado
    ) 
    VALUES 
    (%s, %s, %s, %s, %s);'''
    logger.info("Accion a ejecutarse: Load data")
    for row in registros:
        hook.run(sql_query_deftxt, parameters=row)
    logger.info("Accion: Load data, ejecutada exitosamente")


    # create target table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.if_categoria_recl_gir5 (
	tc_secuencial NUMERIC(10),
	tc_cod_categ VARCHAR(5),
	tc_cod_recl VARCHAR(3),
	tc_tipo_recl VARCHAR(65),
	tc_estado VARCHAR(10)
    );'''
    logger.info("Accion a ejecutarse: create target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''TRUNCATE TABLE ods.if_categoria_recl_gir5;'''
    logger.info("Accion a ejecutarse: Truncate target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = '''INSERT INTO ods.if_categoria_recl_gir5 (
	tc_secuencial,
	tc_cod_categ,
	tc_cod_recl,
	tc_tipo_recl,
	tc_estado
    )
    SELECT
        tc_secuencial,
		tc_cod_categ,
		tc_cod_recl,
		tc_tipo_recl,
		tc_estado
    FROM (
        SELECT
            c1_tc_secuencial AS tc_secuencial,
            c2_tc_cod_categ AS tc_cod_categ,
			c3_tc_cod_recl AS tc_cod_recl,
			c4_tc_tipo_recl AS tc_tipo_recl,
			c5_tc_estado AS tc_estado
        FROM ods.COL_PRF0IF_CATEGORIA_RECL_GIR5
    );'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Drop work table
    sql_query_deftxt = '''DROP TABLE ods.COL_PRF0IF_CATEGORIA_RECL_GIR5;'''
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

dag = DAG(dag_id='PKG_CON_LOAD_IF_CATEGORIA_RECL_GIR5_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_IF_CATEGORIA_RECL_GIR5_ODS_task = PythonOperator(
    task_id='IT_IF_CATEGORIA_RECL_GIR5_ODS_task',
    python_callable=IT_IF_CATEGORIA_RECL_GIR5_ODS,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_IF_CATEGORIA_RECL_GIR5_ODS_task
