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
def IT_IF_TABLA_CATALOGO_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase = JdbcHook(jdbc_conn_id='sybase_ase_conn') 

    # Create work table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.COL_PRF0IF_TABLA_CATALOGO (
	c1_ta_codigo NUMERIC(5) NULL,
	c2_ta_tabla VARCHAR(35) NULL,
	c3_ta_descripcion VARCHAR(80) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
        if_tabla_catalogo.ta_codigo AS c1_ta_codigo,
        if_tabla_catalogo.ta_tabla AS c2_ta_tabla,
        if_tabla_catalogo.ta_descripcion AS c3_ta_descripcion
    FROM bancaribe_core.dbo.if_tabla_catalogo AS if_tabla_catalogo'''
    
    conn = sybase.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_query_coltxt)
    registros = cursor.fetchall()  # <-- Aqui estan los datos
    cursor.close()
    conn.close()

    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0IF_TABLA_CATALOGO (
    C1_ta_codigo, 
    C2_ta_tabla, 
    C3_ta_descripcion 
    ) 
    VALUES 
    (%s, %s, %s);'''
    logger.info("Accion a ejecutarse: Load data") 
    for row in registros:
        hook.run(sql_query_deftxt, parameters=row)
    logger.info("Accion: Load data, ejecutada exitosamente") 


    # create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.if_tabla_catalogo (
	ta_codigo NUMERIC(5),
	ta_tabla VARCHAR(35),
	ta_descripcion VARCHAR(80)
    );'''
    logger.info("Accion a ejecutarse: create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ods.if_tabla_catalogo;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ods.if_tabla_catalogo (
	ta_codigo,
	ta_tabla,
	ta_descripcion
    )
    SELECT
        ta_codigo,
        ta_tabla,
        ta_descripcion
    FROM (
        SELECT
            c1_ta_codigo AS ta_codigo,
            c2_ta_tabla AS ta_tabla,
            c3_ta_descripcion AS ta_descripcion
        FROM ods.COL_PRF0IF_TABLA_CATALOGO
    );'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Drop work table
    sql_query_deftxt = f'''DROP TABLE ods.COL_PRF0IF_TABLA_CATALOGO;'''
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

dag = DAG(dag_id='PKG_CON_LOAD_IF_TABLA_CATALOGO_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_IF_TABLA_CATALOGO_ODS_task = PythonOperator(
    task_id='IT_IF_TABLA_CATALOGO_ODS_task',
    python_callable=IT_IF_TABLA_CATALOGO_ODS,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_IF_TABLA_CATALOGO_ODS_task