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

### FUNCIONES DE CADA TAREA ###
def IT_IF_SOL_RECLAMOS_GIR5_AT_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='ods')
    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')

    vSrcFechaCarga = get_variable('vSrcFechaCarga')
    vSrcFormatoFecha = get_variable('vSrcFormatoFecha')
    vSrcPartition = get_variable('vSrcPartition')

# Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ods.COL_PRF0IF_SOL_RECLAMOS_GIR5_AT;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente")

    # Create work table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.COL_PRF0IF_SOL_RECLAMOS_GIR5_AT (
	c1_sr_secuencial NUMERIC(100) NULL, 
	c2_sr_fecha DATE NULL,
	c3_sr_usuario_ing VARCHAR(14) NULL,
	c4_sr_categ VARCHAR(5) NULL,
	c5_sr_cod_area_trami NUMERIC(100) NULL, 
	c6_sr_tipo_rcl VARCHAR(10) NULL,
	c7_sr_estatus NUMERIC(30) NULL,
	c8_sr_estado VARCHAR(4) NULL,
	c9_sr_sobre VARCHAR(9) NULL,
	c10_sr_valor_reclamo NUMERIC(202) NULL,
	c11_sr_comision NUMERIC(222) NULL,
	c12_sr_idb_monto NUMERIC(200) NULL,
	c13_sr_cedula VARCHAR(20) NULL,
	c14_sr_cliente NUMERIC(100) NULL,
	c15_sr_nombre VARCHAR(100) NULL,
	c16_sr_tarjeta VARCHAR(50) NULL,
	c17_sr_cuenta VARCHAR(25) NULL,
	c18_sr_producto_cta NUMERIC(30) NULL,
	c19_sr_moneda NUMERIC(30) NULL,
	c20_sr_fecha_dbt DATE NULL, 
	c21_sr_secuencial_dbt NUMERIC(100) NULL,
	c22_sr_referencia VARCHAR(15) NULL,
	c23_sr_causa VARCHAR(5) NULL,
	c24_sr_valor_dbt NUMERIC(202) NULL,
	c25_sr_cajero_dbt VARCHAR(30) NULL,
	c26_sr_banco_dbt VARCHAR(11) NULL,
	c27_sr_tipo_cajero VARCHAR(15) NULL,
	c28_sr_oficina NUMERIC(50) NULL,
	c29_sr_oficina_tran NUMERIC(50) NULL,
	c30_sr_interes NUMERIC(202) NULL,
	c31_sr_mora NUMERIC(200) NULL, 
	c32_sr_producto VARCHAR(40) NULL,
	c33_sr_fecha_corte DATE NULL, 
	c34_sr_comercio VARCHAR(65) NULL,
	c35_sr_fcha_desde DATE NULL, 
	c36_sr_fcha_hasta DATE NULL, 
	c37_sr_tasa_aprobada NUMERIC(30) NULL,
	c38_sr_tasa_aplicada NUMERIC(30) NULL,
	c39_sr_intereses NUMERIC(202) NULL,
	c40_sr_MontoCanc NUMERIC(200) NULL,
	c41_sr_cant_respuesta NUMERIC(50) NULL,
	c42_sr_tipo VARCHAR(1) NULL,
	c43_sr_modo_valoracion NUMERIC(30) NULL,
	c44_sr_movimiento VARCHAR(1) NULL,
	c45_sr_fuente VARCHAR(20) NULL,
	c46_sr_usuario_act VARCHAR(14) NULL,
	c47_sr_fecha_act DATE NULL, 
	c48_sr_fecha_int DATE NULL, 
	c49_sr_fecha_est DATE NULL, 
	c50_sr_fecha_cierre DATE NULL, 
	c51_sr_fecha_reintegro DATE NULL, 
	c52_sr_fecha_notif DATE NULL, 
	c53_sr_usuario_notif VARCHAR(14) NULL,
	c54_sr_procesada VARCHAR(1) NULL,
	c55_sr_aviso VARCHAR(1) NULL,
	c56_sr_int1 NUMERIC(100) NULL,
	c57_sr_int2 NUMERIC(100) NULL,
	c58_sr_int3 NUMERIC(100) NULL,
	c59_sr_segmento VARCHAR(30) NULL,
	c60_sr_num_tarjeta VARCHAR(24) NULL,
	c61_sr_hora TIMESTAMP(9) NULL
    );'''
    logger.info("Accion a ejecutarse: Create work table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create work table, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
        if_sol_reclamos_gir5.sr_secuencial AS c1_sr_secuencial,
		if_sol_reclamos_gir5.sr_fecha AS c2_sr_fecha,
		if_sol_reclamos_gir5.sr_usuario_ing AS c3_sr_usuario_ing,
		if_sol_reclamos_gir5.sr_categ AS c4_sr_categ,
		if_sol_reclamos_gir5.sr_cod_area_trami AS c5_sr_cod_area_trami,
		if_sol_reclamos_gir5.sr_tipo_rcl AS c6_sr_tipo_rcl,
		if_sol_reclamos_gir5.sr_estatus AS c7_sr_estatus,
		if_sol_reclamos_gir5.sr_estado AS c8_sr_estado,
		if_sol_reclamos_gir5.sr_sobre AS c9_sr_sobre,
		if_sol_reclamos_gir5.sr_valor_reclamo AS c10_sr_valor_reclamo,
		if_sol_reclamos_gir5.sr_comision AS c11_sr_comision,
		if_sol_reclamos_gir5.sr_idb_monto AS c12_sr_idb_monto,
		if_sol_reclamos_gir5.sr_cedula AS c13_sr_cedula,
		if_sol_reclamos_gir5.sr_cliente AS c14_sr_cliente,
		if_sol_reclamos_gir5.sr_nombre AS c15_sr_nombre,
		if_sol_reclamos_gir5.sr_tarjeta AS c16_sr_tarjeta,
		if_sol_reclamos_gir5.sr_cuenta AS c17_sr_cuenta,
		if_sol_reclamos_gir5.sr_producto_cta AS c18_sr_producto_cta,
		if_sol_reclamos_gir5.sr_moneda AS c19_sr_moneda,
		if_sol_reclamos_gir5.sr_fecha_dbt AS c20_sr_fecha_dbt,
		if_sol_reclamos_gir5.sr_secuencial_dbt AS c21_sr_secuencial_dbt,
		if_sol_reclamos_gir5.sr_referencia AS c22_sr_referencia,
		if_sol_reclamos_gir5.sr_causa AS c23_sr_causa,
		if_sol_reclamos_gir5.sr_valor_dbt AS c24_sr_valor_dbt,
		if_sol_reclamos_gir5.sr_cajero_dbt AS c25_sr_cajero_dbt,
		if_sol_reclamos_gir5.sr_banco_dbt AS c26_sr_banco_dbt,
		if_sol_reclamos_gir5.sr_tipo_cajero AS c27_sr_tipo_cajero,
		if_sol_reclamos_gir5.sr_oficina AS c28_sr_oficina,
		if_sol_reclamos_gir5.sr_oficina_tran AS c29_sr_oficina_tran,
		if_sol_reclamos_gir5.sr_interes AS c30_sr_interes,
		if_sol_reclamos_gir5.sr_mora AS c31_sr_mora,
		if_sol_reclamos_gir5.sr_producto AS c32_sr_producto,
		if_sol_reclamos_gir5.sr_fecha_corte AS c33_sr_fecha_corte,
		if_sol_reclamos_gir5.sr_comercio AS c34_sr_comercio,
		if_sol_reclamos_gir5.sr_fcha_desde AS c35_sr_fcha_desde,
		if_sol_reclamos_gir5.sr_fcha_hasta AS c36_sr_fcha_hasta,
		if_sol_reclamos_gir5.sr_tasa_aprobada AS c37_sr_tasa_aprobada,
		if_sol_reclamos_gir5.sr_tasa_aplicada AS c38_sr_tasa_aplicada,
		if_sol_reclamos_gir5.sr_intereses AS c39_sr_intereses,
		if_sol_reclamos_gir5.sr_MontoCanc AS c40_sr_MontoCanc,
		if_sol_reclamos_gir5.sr_cant_respuesta AS c41_sr_cant_respuesta,
		if_sol_reclamos_gir5.sr_tipo AS c42_sr_tipo,
		if_sol_reclamos_gir5.sr_modo_valoracion AS c43_sr_modo_valoracion,
		if_sol_reclamos_gir5.sr_movimiento AS c44_sr_movimiento,
		if_sol_reclamos_gir5.sr_fuente AS c45_sr_fuente,
		if_sol_reclamos_gir5.sr_usuario_act AS c46_sr_usuario_act,
		if_sol_reclamos_gir5.sr_fecha_act AS c47_sr_fecha_act,
		if_sol_reclamos_gir5.sr_fecha_int AS c48_sr_fecha_int,
		if_sol_reclamos_gir5.sr_fecha_est AS c49_sr_fecha_est,
		if_sol_reclamos_gir5.sr_fecha_cierre AS c50_sr_fecha_cierre,
		if_sol_reclamos_gir5.sr_fecha_reintegro AS c51_sr_fecha_reintegro,
		if_sol_reclamos_gir5.sr_fecha_notif AS c52_sr_fecha_notif,
		if_sol_reclamos_gir5.sr_usuario_notif AS c53_sr_usuario_notif,
		if_sol_reclamos_gir5.sr_procesada AS c54_sr_procesada,
		if_sol_reclamos_gir5.sr_aviso AS c55_sr_aviso,
		if_sol_reclamos_gir5.sr_int1 AS c56_sr_int1,
		if_sol_reclamos_gir5.sr_int2 AS c57_sr_int2,
		if_sol_reclamos_gir5.sr_int3 AS c58_sr_int3,
		if_sol_reclamos_gir5.sr_segmento AS c59_sr_segmento,
		if_sol_reclamos_gir5.sr_num_tarjeta AS c60_sr_num_tarjeta,
		if_sol_reclamos_gir5.sr_hora AS c61_sr_hora
	FROM bancaribe_core.dbo.if_sol_reclamos_gir5 AS if_sol_reclamos_gir5'''
    
      # conexion a sybase
    sybase_conn = sybase_hook.get_conn()
    sybase_cursor = sybase_conn.cursor()
    sybase_cursor.execute(sql_query_coltxt)

    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = '''INSERT INTO ods.COL_PRF0IF_SOL_RECLAMOS_GIR5_AT (
    c1_sr_secuencial,
	c2_sr_fecha,
	c3_sr_usuario_ing,
	c4_sr_categ,
	c5_sr_cod_area_trami,
	c6_sr_tipo_rcl,
	c7_sr_estatus,
	c8_sr_estado,
	c9_sr_sobre,
	c10_sr_valor_reclamo,
	c11_sr_comision,
	c12_sr_idb_monto,
	c13_sr_cedula,
	c14_sr_cliente,
	c15_sr_nombre,
	c16_sr_tarjeta,
	c17_sr_cuenta,
	c18_sr_producto_cta,
	c19_sr_moneda,
	c20_sr_fecha_dbt,
	c21_sr_secuencial_dbt,
	c22_sr_referencia,
	c23_sr_causa,
	c24_sr_valor_dbt,
	c25_sr_cajero_dbt,
	c26_sr_banco_dbt,
	c27_sr_tipo_cajero,
	c28_sr_oficina,
	c29_sr_oficina_tran,
	c30_sr_interes,
	c31_sr_mora,
	c32_sr_producto,
	c33_sr_fecha_corte,  
	c34_sr_comercio,
	c35_sr_fcha_desde,  
	c36_sr_fcha_hasta, 
	c37_sr_tasa_aprobada,
	c38_sr_tasa_aplicada,
	c39_sr_intereses,
	c40_sr_MontoCanc,
	c41_sr_cant_respuesta,
	c42_sr_tipo,
	c43_sr_modo_valoracion,
	c44_sr_movimiento,
	c45_sr_fuente,
	c46_sr_usuario_act,
	c47_sr_fecha_act,
	c48_sr_fecha_int, 
	c49_sr_fecha_est, 
	c50_sr_fecha_cierre,  
	c51_sr_fecha_reintegro, 
	c52_sr_fecha_notif,
	c53_sr_usuario_notif,
	c54_sr_procesada,
	c55_sr_aviso,
	c56_sr_int1,
	c57_sr_int2,
	c58_sr_int3,
	c59_sr_segmento,
	c60_sr_num_tarjeta,
	c61_sr_hora
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
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ods.if_sol_reclamos_gir5_at (
	sr_secuencial NUMERIC(100),
	sr_fecha DATE,
	sr_usuario_ing VARCHAR(14),
	sr_categ VARCHAR(5),
	sr_cod_area_trami NUMERIC(100),
	sr_tipo_rcl VARCHAR(10),
	sr_estatus NUMERIC(30),
	sr_estado VARCHAR(4),
	sr_sobre VARCHAR(9),
	sr_valor_reclamo NUMERIC(202),
	sr_comision NUMERIC(222),
	sr_idb_monto NUMERIC(200),
	sr_cedula VARCHAR(20),
	sr_cliente NUMERIC(100),
	sr_nombre VARCHAR(100),
	sr_tarjeta VARCHAR(50),
	sr_cuenta VARCHAR(25),
	sr_producto_cta NUMERIC(30),
	sr_moneda NUMERIC(30),
	sr_fecha_dbt DATE,
	sr_secuencial_dbt NUMERIC(100),
	sr_referencia VARCHAR(15),
	sr_causa VARCHAR(5),
	sr_valor_dbt NUMERIC(202),
	sr_cajero_dbt VARCHAR(30),
	sr_banco_dbt VARCHAR(11),
	sr_tipo_cajero VARCHAR(15),
	sr_oficina NUMERIC(50),
	sr_oficina_tran NUMERIC(50),
	sr_interes NUMERIC(202),
	sr_mora NUMERIC(200),
	sr_producto VARCHAR(40),
	sr_fecha_corte DATE,
	sr_comercio VARCHAR(65),
	sr_fcha_desde DATE,
	sr_fcha_hasta DATE,
	sr_tasa_aprobada NUMERIC(30),
	sr_tasa_aplicada NUMERIC(30),
	sr_intereses NUMERIC(202),
	sr_MontoCanc NUMERIC(200),
	sr_cant_respuesta NUMERIC(50),
	sr_tipo VARCHAR(1),
	sr_modo_valoracion NUMERIC(30),
	sr_movimiento VARCHAR(1),
	sr_fuente VARCHAR(20),
	sr_usuario_act VARCHAR(14),
	sr_fecha_act DATE,
	sr_fecha_int DATE,
	sr_fecha_est DATE,
	sr_fecha_cierre DATE,
	sr_fecha_reintegro DATE,
	sr_fecha_notif DATE,
	sr_usuario_notif VARCHAR(14),
	sr_procesada VARCHAR(1),
	sr_aviso VARCHAR(1),
	sr_int1 NUMERIC(100),
	sr_int2 NUMERIC(100),
	sr_int3 NUMERIC(100),
	sr_segmento VARCHAR(30),
	sr_num_tarjeta VARCHAR(24),
	sr_hora TIMESTAMP(9),
	fechacarga VARCHAR(10)
    );'''
    logger.info("Accion a ejecutarse: create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ods.if_sol_reclamos_gir5_at;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ods.if_sol_reclamos_gir5_at (
	sr_secuencial,
	sr_fecha,
	sr_usuario_ing,
	sr_categ,
	sr_cod_area_trami,
	sr_tipo_rcl,
	sr_estatus,
	sr_estado,
	sr_sobre,
	sr_valor_reclamo,
	sr_comision,
	sr_idb_monto,
	sr_cedula,
	sr_cliente,
	sr_nombre,
	sr_tarjeta,
	sr_cuenta,
	sr_producto_cta,
	sr_moneda,
	sr_fecha_dbt,
	sr_secuencial_dbt,
	sr_referencia,
	sr_causa,
	sr_valor_dbt,
	sr_cajero_dbt,
	sr_banco_dbt,
	sr_tipo_cajero,
	sr_oficina,
	sr_oficina_tran,
	sr_interes,
	sr_mora,
	sr_producto,
	sr_fecha_corte,
	sr_comercio,
	sr_fcha_desde,
	sr_fcha_hasta,
	sr_tasa_aprobada,
	sr_tasa_aplicada,
	sr_intereses,
	sr_MontoCanc,
	sr_cant_respuesta,
	sr_tipo,
	sr_modo_valoracion,
	sr_movimiento,
	sr_fuente,
	sr_usuario_act,
	sr_fecha_act,
	sr_fecha_int,
	sr_fecha_est,
	sr_fecha_cierre,
	sr_fecha_reintegro,
	sr_fecha_notif,
	sr_usuario_notif,
	sr_procesada,
	sr_aviso,
	sr_int1,
	sr_int2,
	sr_int3,
	sr_segmento,
	sr_num_tarjeta,
	sr_hora,
	fechacarga
    )
            SELECT DISTINCT 
            c1_sr_secuencial AS sr_secuencial,
			c2_sr_fecha AS sr_fecha,
			c3_sr_usuario_ing AS sr_usuario_ing,
			c4_sr_categ AS sr_categ,
			c5_sr_cod_area_trami AS sr_cod_area_trami,
			c6_sr_tipo_rcl AS sr_tipo_rcl,
			c7_sr_estatus AS sr_estatus,
			c8_sr_estado AS sr_estado,
			c9_sr_sobre AS sr_sobre,
			c10_sr_valor_reclamo AS sr_valor_reclamo,
			c11_sr_comision AS sr_comision,
			c12_sr_idb_monto AS sr_idb_monto,
			c13_sr_cedula AS sr_cedula,
			c14_sr_cliente AS sr_cliente,
			c15_sr_nombre AS sr_nombre,
			c16_sr_tarjeta AS sr_tarjeta,
			c17_sr_cuenta AS sr_cuenta,
			c18_sr_producto_cta AS sr_producto_cta,
			c19_sr_moneda AS sr_moneda,
			c20_sr_fecha_dbt AS sr_fecha_dbt,
			c21_sr_secuencial_dbt AS sr_secuencial_dbt,
			c22_sr_referencia AS sr_referencia,
			c23_sr_causa AS sr_causa,
			c24_sr_valor_dbt AS sr_valor_dbt,
			c25_sr_cajero_dbt AS sr_cajero_dbt,
			c26_sr_banco_dbt AS sr_banco_dbt,
			c27_sr_tipo_cajero AS sr_tipo_cajero,
			c28_sr_oficina AS sr_oficina,
			c29_sr_oficina_tran AS sr_oficina_tran,
			c30_sr_interes AS sr_interes,
			c31_sr_mora AS sr_mora,
			c32_sr_producto AS sr_producto,
			c33_sr_fecha_corte AS sr_fecha_corte,
			c34_sr_comercio AS sr_comercio,
			c35_sr_fcha_desde AS sr_fcha_desde,
			c36_sr_fcha_hasta AS sr_fcha_hasta,
			c37_sr_tasa_aprobada AS sr_tasa_aprobada,
			c38_sr_tasa_aplicada AS sr_tasa_aplicada,
			c39_sr_intereses AS sr_intereses,
			c40_sr_MontoCanc AS sr_MontoCanc,
			c41_sr_cant_respuesta AS sr_cant_respuesta,
			c42_sr_tipo AS sr_tipo,
			c43_sr_modo_valoracion AS sr_modo_valoracion,
			c44_sr_movimiento AS sr_movimiento,
			c45_sr_fuente AS sr_fuente,
			c46_sr_usuario_act AS sr_usuario_act,
			c47_sr_fecha_act AS sr_fecha_act,
			c48_sr_fecha_int AS sr_fecha_int,
			c49_sr_fecha_est AS sr_fecha_est,
			c50_sr_fecha_cierre AS sr_fecha_cierre,
			c51_sr_fecha_reintegro AS sr_fecha_reintegro,
			c52_sr_fecha_notif AS sr_fecha_notif,
			c53_sr_usuario_notif AS sr_usuario_notif,
			c54_sr_procesada AS sr_procesada,
			c55_sr_aviso AS sr_aviso,
			c56_sr_int1 AS sr_int1,
			c57_sr_int2 AS sr_int2,
			c58_sr_int3 AS sr_int3,
			c59_sr_segmento AS sr_segmento,
			c60_sr_num_tarjeta AS sr_num_tarjeta,
			c61_sr_hora AS sr_hora,
			TO_CHAR(TO_DATE('{vSrcFechaCarga}', '{vSrcFormatoFecha}'), 'DDMM') AS fechacarga
        FROM ods.COL_PRF0IF_SOL_RECLAMOS_GIR5_AT
    ;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Drop work table
    sql_query_deftxt = f'''DROP TABLE ods.COL_PRF0IF_SOL_RECLAMOS_GIR5_AT;'''
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

dag = DAG(dag_id='PKG_CON_LOAD_IF_SOL_RECLAMOS_GIR5_AT_ODS',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_IF_SOL_RECLAMOS_GIR5_AT_ODS_task = PythonOperator(
    task_id='IT_IF_SOL_RECLAMOS_GIR5_AT_ODS_task',
    python_callable=IT_IF_SOL_RECLAMOS_GIR5_AT_ODS,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_IF_SOL_RECLAMOS_GIR5_AT_ODS_task