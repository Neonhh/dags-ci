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
    value = 'CB_CUENTA'
    Variable.set('vSrcNombreTablaOds', serialize_value(value))

def vSrcFechaCarga(**kwargs):
    value = '05/30/2025'
    Variable.set('vSrcFechaCarga', serialize_value(value))

def vSrcFormatoFecha(**kwargs):
	value = 'MM/DD/YYYY'
	Variable.set('vSrcFormatoFecha', serialize_value(value))

def vSrcPartition(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

    sql_query = f'''SELECT TO_CHAR(TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}'), 'DDMM');'''
    result = hook.get_records(sql_query)

    Variable.set('vSrcPartition', serialize_value(result[0][0]))

def vSrcTipoCarga(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')

    vSrcNombreTablaOds = get_variable('vSrcNombreTablaOds')

    sql_query = f'''SELECT bcsd.COO_ATTRIBUTE1
    FROM ods.BAN_CONFIG_ODS AS bcsd
    WHERE bcsd.COO_SOURCE = '{vSrcNombreTablaOds}';'''
    result = hook.get_records(sql_query)

    Variable.set('vSrcTipoCarga', serialize_value(result[0][0]))

# comentar
# def IT_CB__CUENTA__COB_CONTA__ODS_INICIAL(**kwargs):
#     hook = PostgresHook(postgres_conn_id='ods')
#     sybase = JdbcHook(jdbc_conn_id='sybase_ase_conn')

#     vSrcFechaCarga = get_variable('vSrcFechaCarga')
#     vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

#     # Create work table
#     sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.COL_PRF0CB_CUENTA (
#     C1_CU_EMPRESA INT NULL,
#     C2_CU_CUENTA VARCHAR(34) NULL,
#     C3_CU_CUENTA_PADRE VARCHAR(20) NULL,
#     C4_CU_NOMBRE VARCHAR(80) NULL,
#     C5_CU_DESCRIPCION VARCHAR(255) NULL,
#     C6_CU_ESTADO VARCHAR(1) NULL,
#     C7_CU_FECHA_ESTADO DATE NULL,
#     C8_CU_MONEDA INT NULL
#     );'''
#     logger.info("Accion a ejecutarse: Create work table") 
#     hook.run(sql_query_deftxt)
#     logger.info("Accion: Create work table, ejecutada exitosamente") 

#     # Load data
#     sql_query_coltxt = f'''SELECT
#         CB_CUENTA.cu_empresa AS C1_CU_EMPRESA,
#         LTRIM(RTRIM(CB_CUENTA.cu_cuenta)) AS C2_CU_CUENTA,
#         LTRIM(RTRIM(CB_CUENTA.cu_cuenta_padre)) AS C3_CU_CUENTA_PADRE,
#         LTRIM(RTRIM(CB_CUENTA.cu_nombre)) AS C4_CU_NOMBRE,
#         LTRIM(RTRIM(CB_CUENTA.cu_descripcion)) AS C5_CU_DESCRIPCION,
#         CB_CUENTA.cu_estado AS C6_CU_ESTADO,
#         CB_CUENTA.cu_fecha_estado AS C7_CU_FECHA_ESTADO,
#         CB_CUENTA.cu_moneda AS C8_CU_MONEDA
#     FROM bancaribe_core.dbo.cb_cuenta AS CB_CUENTA
#     WHERE (CB_CUENTA.cu_empresa = 1)'''
#     logger.info("Accion a ejecutarse: Load data") 

#     conn = sybase.get_conn()
#     cursor = conn.cursor()
#     cursor.execute(sql_query_coltxt)
#     registros = cursor.fetchall()  # <-- Aqui estan los datos
#     cursor.close()
#     conn.close()

#     logger.info("Accion: Load data, ejecutada exitosamente") 

#     # Load data
#     sql_query_deftxt = '''INSERT INTO ods.col_prf0cb_cuenta (
# 	c1_cu_empresa,
# 	c2_cu_cuenta,
# 	c3_cu_cuenta_padre,
# 	c4_cu_nombre,
# 	c5_cu_descripcion,
# 	c6_cu_estado,
# 	c7_cu_fecha_estado,
# 	c8_cu_moneda
#     )
#     VALUES
#     (%s,%s,%s,%s,%s,%s,%s,%s);'''
#     logger.info("Accion a ejecutarse: Load data") 
#     for row in registros:
#         hook.run(sql_query_deftxt, parameters=row)
#     logger.info("Accion: Load data, ejecutada exitosamente") 

#     # Truncate target table
#     sql_query_deftxt = f'''TRUNCATE TABLE ods.cb_cuenta;'''
#     logger.info("Accion a ejecutarse: Truncate target table") 
#     hook.run(sql_query_deftxt)
#     logger.info("Accion: Truncate target table, ejecutada exitosamente") 

#     # Insert new rows
#     sql_query_deftxt = f'''INSERT INTO ods.cb_cuenta (
# 	cu_empresa,
# 	cu_cuenta,
# 	cu_cuenta_padre,
# 	cu_nombre,
# 	cu_descripcion,
# 	cu_estado,
# 	cu_fecha_estado,
# 	cu_moneda,
# 	fechacarga
#     )
#     SELECT
#         cu_empresa,
#         cu_cuenta,
#         cu_cuenta_padre,
#         cu_nombre,
#         cu_descripcion,
#         cu_estado,
#         cu_fecha_estado,
#         cu_moneda,
#         TO_CHAR(TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}'), 'DDMM')
#     FROM (
#         SELECT
#             c1_cu_empresa AS cu_empresa,
#             c2_cu_cuenta AS cu_cuenta,
#             c3_cu_cuenta_padre AS cu_cuenta_padre,
#             c4_cu_nombre AS cu_nombre,
#             c5_cu_descripcion AS cu_descripcion,
#             c6_cu_estado AS cu_estado,
#             c7_cu_fecha_estado AS cu_fecha_estado,
#             c8_cu_moneda AS cu_moneda
#         FROM ods.col_prf0cb_cuenta);'''
#     logger.info("Accion a ejecutarse: Insert new rows") 
#     hook.run(sql_query_deftxt)
#     logger.info("Accion: Insert new rows, ejecutada exitosamente") 

#     # Drop work table
#     sql_query_deftxt = f'''DROP TABLE ods.COL_PRF0CB_CUENTA;'''
#     logger.info("Accion a ejecutarse: Drop work table") 
#     hook.run(sql_query_deftxt)
#     logger.info("Accion: Drop work table, ejecutada exitosamente") 

def IT_CB__CUENTA__COB_CONTA__ODS_DIARIA(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')
    vSrcPartition = get_variable('vSrcPartition')

    # Create work table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.COL_PRF0CB_CUENTA (
	C1_CU_EMPRESA INT NULL,
	C2_CU_CUENTA VARCHAR(34) NULL,
	C3_CU_CUENTA_PADRE VARCHAR(20) NULL,
	C4_CU_NOMBRE VARCHAR(80) NULL,
	C5_CU_DESCRIPCION VARCHAR(255) NULL,
	C6_CU_ESTADO VARCHAR(1) NULL,
	C7_CU_MOVIMIENTO VARCHAR(1) NULL,
	C8_CU_CATEGORIA VARCHAR(1) NULL,
	C9_CU_FECHA_ESTADO DATE NULL,
	C10_CU_MONEDA INT NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
        CB_CUENTA.cu_empresa AS C1_CU_EMPRESA,
        LTRIM(RTRIM(CB_CUENTA.cu_cuenta)) AS C2_CU_CUENTA,
        LTRIM(RTRIM(CB_CUENTA.cu_cuenta_padre)) AS C3_CU_CUENTA_PADRE,
        LTRIM(RTRIM(CB_CUENTA.cu_nombre)) AS C4_CU_NOMBRE,
        LTRIM(RTRIM(CB_CUENTA.cu_descripcion)) AS C5_CU_DESCRIPCION,
        CB_CUENTA.cu_estado AS C6_CU_ESTADO,
        CB_CUENTA.cu_movimiento AS C7_CU_MOVIMIENTO,
        CB_CUENTA.cu_categoria AS C8_CU_CATEGORIA,
        CB_CUENTA.cu_fecha_estado AS C9_CU_FECHA_ESTADO,
        CB_CUENTA.cu_moneda AS C10_CU_MONEDA
    FROM bancaribe_core.dbo.cb_cuenta AS CB_CUENTA
    WHERE (CB_CUENTA.cu_empresa=1)'''
    logger.info("Accion a ejecutarse: Load data") 

    # conexion a sybase
    sybase_conn = sybase_hook.get_conn()
    sybase_cursor = sybase_conn.cursor()
    sybase_cursor.execute(sql_query_coltxt)

    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.col_prf0cb_cuenta (
	c1_cu_empresa,
	c2_cu_cuenta,
	c3_cu_cuenta_padre,
	c4_cu_nombre,
	c5_cu_descripcion,
	c6_cu_estado,
	c7_cu_movimiento,
	c8_cu_categoria,
	c9_cu_fecha_estado,
	c10_cu_moneda
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

    # Truncate target table
    sql_query_deftxt = f'''DELETE FROM ods.cb_cuenta
    WHERE fechacarga = '{vSrcPartition}';'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ods.cb_cuenta (
	cu_empresa,
	cu_cuenta,
	cu_cuenta_padre,
	cu_nombre,
	cu_descripcion,
	cu_estado,
	cu_movimiento,
	cu_categoria,
	cu_fecha_estado,
	cu_moneda,
	fechacarga
    )
    SELECT
        c1_cu_empresa AS cu_empresa,
        c2_cu_cuenta AS cu_cuenta,
        c3_cu_cuenta_padre AS cu_cuenta_padre,
        c4_cu_nombre AS cu_nombre,
        c5_cu_descripcion AS cu_descripcion,
        c6_cu_estado AS cu_estado,
        c7_cu_movimiento AS cu_movimiento,
        c8_cu_categoria AS cu_categoria,
        c9_cu_fecha_estado AS cu_fecha_estado,
        c10_cu_moneda AS cu_moneda,
        to_char(to_date('{vSrcFechaCarga}', '{vSrcFormatoFecha}'), 'DDMM') AS fechacarga
    FROM ods.col_prf0cb_cuenta;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Drop work table
    sql_query_deftxt = f'''DROP TABLE ods.col_prf0cb_cuenta;'''
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

dag = DAG(dag_id='PKG_CON_LOAD_CB_CUENTA_COB_CONTA_ODS',
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

vSrcPartition_task = PythonOperator(
    task_id='vSrcPartition_task',
    python_callable=vSrcPartition,
    dag=dag
)

vSrcTipoCarga_task = PythonOperator(
    task_id='vSrcTipoCarga_task',
    python_callable=vSrcTipoCarga,
    dag=dag
)

# IT_CB__CUENTA__COB_CONTA__ODS_INICIAL_task = PythonOperator(
#     task_id='IT_CB__CUENTA__COB_CONTA__ODS_INICIAL_task',
#     python_callable=IT_CB__CUENTA__COB_CONTA__ODS_INICIAL,
##     dag=dag
# )

IT_CB__CUENTA__COB_CONTA__ODS_DIARIA_task = PythonOperator(
    task_id='IT_CB__CUENTA__COB_CONTA__ODS_DIARIA_task',
    python_callable=IT_CB__CUENTA__COB_CONTA__ODS_DIARIA,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
vSrcNombreTablaOds_task >> vSrcFechaCarga_task >> vSrcFormatoFecha_task >> vSrcPartition_task >> vSrcTipoCarga_task >> IT_CB__CUENTA__COB_CONTA__ODS_DIARIA_task
