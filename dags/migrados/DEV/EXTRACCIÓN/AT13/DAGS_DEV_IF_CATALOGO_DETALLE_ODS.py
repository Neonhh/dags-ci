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
def IT_IF_CATALOGO_DETALLE_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    # Create work table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.COL_PRF0IF_CATALOGO_DETALLE (
	c1_ca_tabla NUMERIC(5) NULL,
	c2_ca_codigo VARCHAR(11) NULL,
    c3_ca_valor VARCHAR(66) NULL,
	c4_ca_estado VARCHAR(1) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente")

    # Load data
    sql_query_coltxt = '''SELECT
        if_catalogo_detalle.ca_tabla AS c1_ca_tabla,
        if_catalogo_detalle.ca_codigo AS c2_ca_codigo,
        if_catalogo_detalle.ca_valor AS c3_ca_valor,
        if_catalogo_detalle.ca_estado AS c4_ca_estado
    FROM bancaribe_core.dbo.if_catalogo_detalle AS if_catalogo_detalle'''

    conn = sybase.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_query_coltxt)
    registros = cursor.fetchall()  # <-- Aqui estan los datos
    cursor.close()
    conn.close()

    logger.info("Accion: Load data, ejecutada exitosamente")

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0IF_CATALOGO_DETALLE (
    C1_ca_tabla, 
    C2_ca_codigo, 
    C3_ca_valor,
    C4_ca_estado
    ) 
    VALUES 
    (%s, %s, %s, %s);'''
    logger.info("Accion a ejecutarse: Load data")
    for row in registros:
        hook.run(sql_query_deftxt, parameters=row)
    logger.info("Accion: Load data, ejecutada exitosamente")


    # create target table
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ods.if_catalogo_detalle (
	ca_tabla NUMERIC(5),
	ca_codigo VARCHAR(11),
    ca_valor VARCHAR(66),
	ca_estado VARCHAR(1)
    );'''
    logger.info("Accion a ejecutarse: create target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente")

    # Truncate target table
    sql_query_deftxt = '''TRUNCATE TABLE ods.if_catalogo_detalle;'''
    logger.info("Accion a ejecutarse: Truncate target table")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Insert new rows
    sql_query_deftxt = '''INSERT INTO ods.if_catalogo_detalle (
	ca_tabla,
	ca_codigo,
	ca_valor,
    ca_estado
    )
    SELECT
        ca_tabla,
	    ca_codigo,
	    ca_valor,
        ca_estado
    FROM (
        SELECT
            c1_ca_tabla AS ca_tabla,
            c2_ca_codigo AS ca_codigo,
            c3_ca_valor AS ca_valor,
            c4_ca_estado AS ca_estado
        FROM ods.COL_PRF0IF_CATALOGO_DETALLE
    );'''
    logger.info("Accion a ejecutarse: Insert new rows")
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente")

    # Drop work table
    sql_query_deftxt = '''DROP TABLE ods.COL_PRF0IF_CATALOGO_DETALLE;'''
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

dag = DAG(dag_id='PKG_CON_LOAD_IF_CATALOGO_DETALLE_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_IF_CATALOGO_DETALLE_ODS_task = PythonOperator(
    task_id='IT_IF_CATALOGO_DETALLE_ODS_task',
    python_callable=IT_IF_CATALOGO_DETALLE_ODS,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_IF_CATALOGO_DETALLE_ODS_task
