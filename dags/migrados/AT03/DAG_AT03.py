from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
import time
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

def IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')
	
	# Recuperar las variables definidas en las tareas previas
	AT03_Max_Periodo = get_variable('AT03_Max_Periodo')
	AT03_Max_Corte = get_variable('AT03_Max_Corte')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ODS.CB_HIST_SALDO_AT03 (
	HI_EMPRESA    NUMERIC(3,0),
	HI_CUENTA     VARCHAR(20),
	HI_OFICINA    NUMERIC(10,0),
	HI_AREA       NUMERIC(10,0),
	HI_CORTE      NUMERIC(10,0),
	HI_PERIODO    NUMERIC(10,0),
	HI_SALDO      NUMERIC(32,7),
	HI_SALDO_ME   NUMERIC(32,7),
	HI_DEBITO     NUMERIC(32,7),
	HI_CREDITO    NUMERIC(32,7),
	HI_DEBITO_ME  NUMERIC(32,7),
	HI_CREDITO_ME NUMERIC(32,7),
	HI_FECHA_INI  DATE,
	HI_FECHA_FIN  DATE
	);
	'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS 
	sql_query_deftxt = '''TRUNCATE TABLE ODS.CB_HIST_SALDO_AT03;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ods.cb_hist_saldo_at03 (
	hi_empresa,
	hi_cuenta,
	hi_oficina,
	hi_area,
	hi_corte,
	hi_periodo,
	hi_saldo,
	hi_saldo_me,
	hi_debito,
	hi_credito,
	hi_debito_me,
	hi_credito_me,
	hi_fecha_ini,
	hi_fecha_fin
	)
	SELECT
		cb_hist_saldo.hi_empresa AS hi_empresa,
		TRIM(cb_hist_saldo.hi_cuenta) AS hi_cuenta,
		cb_hist_saldo.hi_oficina AS hi_oficina,
		cb_hist_saldo.hi_area AS hi_area,
		cb_hist_saldo.hi_corte AS hi_corte,
		cb_hist_saldo.hi_periodo AS hi_periodo,
		cb_hist_saldo.hi_saldo AS hi_saldo,
		cb_hist_saldo.hi_saldo_me AS hi_saldo_me,
		cb_hist_saldo.hi_debito AS hi_debito,
		cb_hist_saldo.hi_credito AS hi_credito,
		cb_hist_saldo.hi_debito_me AS hi_debito_me,
		cb_hist_saldo.hi_credito_me AS hi_credito_me,
		CURRENT_DATE AS hi_fecha_ini,
		CURRENT_DATE AS hi_fecha_fin
	FROM ods.cb_hist_saldo
	WHERE (cb_hist_saldo.hi_periodo = '125')
		AND (cb_hist_saldo.hi_corte = '150')
		AND (cb_hist_saldo.hi_empresa = 1); '''
	hook.run(sql_query_deftxt)

def AT03_AGRUPACION_CONTABLE_BANCARIBE(**kwargs):
	repodata_hook = PostgresHook(postgres_conn_id='repodataprd')
	ods_hook = PostgresHook(postgres_conn_id='ods')

	# Recuperar las variables definidas en las tareas previas
	AT03_Max_Periodo = get_variable('AT03_Max_Periodo')
	AT03_Max_Corte = get_variable('AT03_Max_Corte')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AGRUPACION_BANCARIBE (
	OFICINA NUMERIC(10),
	CUENTA VARCHAR(12),
	SALDO NUMERIC(32,7),
	EMPRESA NUMERIC(3),
	CATEGORIA VARCHAR(1)
	);'''
	repodata_hook.run(sql_query_deftxt)

	# Vaciamos la tabla (para no duplicar datos)
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AGRUPACION_BANCARIBE;'''
	repodata_hook.run(sql_query_deftxt)

	# Consulta en la BD ods
	select_sql = f''' SELECT DISTINCT
		COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS OFICINA,
		SUBSTRING(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0') FROM 1 FOR 12) AS CUENTA,
		CB_HIST_SALDO_AT03.HI_SALDO AS SALDO,
		CB_HIST_SALDO_AT03.HI_EMPRESA AS EMPRESA,
		CB_CUENTA.CU_CATEGORIA AS CATEGORIA
	FROM (ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03 INNER JOIN ODS.CB_CUENTA AS CB_CUENTA ON (CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA))
		INNER JOIN ODS.CB_RELOFI AS CB_RELOFI ON (CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA)
	WHERE (CB_HIST_SALDO_AT03.HI_PERIODO = '125')
		AND (CB_HIST_SALDO_AT03.HI_CORTE = '150')
		AND (CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700,701,702,703,704,705,708,709,710,711,713,714,715))
		AND (CB_HIST_SALDO_AT03.HI_AREA >= 0)
		AND (CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9')
		AND (CB_CUENTA.CU_CATEGORIA IS NOT NULL); '''
	
	# conexion a bd ods
	ods_conn = ods_hook.get_conn()
	ods_cursor = ods_conn.cursor(name='fetch_cursor')  # Cursor server-side para no cargar todo en RAM
	ods_cursor.execute(select_sql)

	# INSERTAR DATOS EN DESTINO
	insert_sql_query = '''INSERT INTO ATSUDEBAN.AGRUPACION_BANCARIBE (
	OFICINA, 
	CUENTA, 
	SALDO, 
	EMPRESA, 
	CATEGORIA
	) 
	VALUES %s
	'''
	
	# insercion por lotes en la BD repodata
	repodata_conn = repodata_hook.get_conn()
	repodata_cursor = repodata_conn.cursor()

	batch_size = 10000
	total_insertados = 0
	batch_num = 1

	while True:
		rows = ods_cursor.fetchmany(batch_size)
		if not rows:
			break
		execute_values(repodata_cursor, insert_sql_query, rows)
		repodata_conn.commit()

		total_insertados += len(rows)
		logger.info(f"Lote {batch_num}: insertados {len(rows)} registros (Total acumulado: {total_insertados})")

	# cerrar conexiones
	ods_cursor.close()
	ods_conn.close()
	repodata_cursor.close()
	repodata_conn.close()

	logger.info(f"Proceso finalizado: {total_insertados} registros insertados.")

def Actualizar_cuenta_formato_Sudeban(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '717001000000'
	WHERE CUENTA = '717011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '722001000000'
	WHERE CUENTA = '722011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '732001000000'
	WHERE CUENTA = '732011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '743001000000'
	WHERE CUENTA = '743011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '744001000000'
	WHERE CUENTA = '744010000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '753001000000'
	WHERE CUENTA = '753011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '712001000000'
	WHERE CUENTA = '712011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '734001000000'
	WHERE CUENTA = '734011010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '714002000000'
	WHERE CUENTA = '714020000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '722002000000'
	WHERE CUENTA = '722012000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '751002000000'
	WHERE CUENTA = '751012000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '744002000000'
	WHERE CUENTA = '744020000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '714001000000'
	WHERE CUENTA = '714011000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '751001000000'
	WHERE CUENTA = '751011000000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '712002000000'
	WHERE CUENTA = '712012010000';'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
	SET CUENTA = '1335710000'
	WHERE CUENTA = '1335710100';'''
	hook.run(sql_query_deftxt)

def AT03_AGRUPACION_OFICINAS_BANCARIBE(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT03_OFICINA (
	CUENTA VARCHAR(12),
	OFICINA VARCHAR(4),
	SALDO NUMERIC(32,7),
	CONSTANTE VARCHAR(1),
	EMPRESA NUMERIC(3),
	CATEGORIA VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT03_OFICINA;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT03_OFICINA (
	CUENTA, 
	OFICINA, 
	SALDO,
	CONSTANTE, 
	EMPRESA, 
	CATEGORIA
	)
	SELECT 
		AGRUPACION_BANCARIBE.CUENTA AS CUENTA,
		LPAD(AGRUPACION_BANCARIBE.OFICINA::text, 4, '0') AS OFICINA,
		SUM(AGRUPACION_BANCARIBE.SALDO) AS SALDO,
		'1' AS CONSTANTE,
		AGRUPACION_BANCARIBE.EMPRESA AS EMPRESA,
		AGRUPACION_BANCARIBE.CATEGORIA AS CATEGORIA
	FROM ATSUDEBAN.AGRUPACION_BANCARIBE AS AGRUPACION_BANCARIBE INNER JOIN ATSUDEBAN.SIIF_AT03_CUENTACONTABLE AS SIIF_AT03_CUENTACONTABLE
		ON TRIM(SIIF_AT03_CUENTACONTABLE.CUENTACONTABLE) = TRIM(AGRUPACION_BANCARIBE.CUENTA) 
	AND TRIM(AGRUPACION_BANCARIBE.EMPRESA::TEXT) = TRIM(SIIF_AT03_CUENTACONTABLE.IDBANCO::TEXT)
	WHERE AGRUPACION_BANCARIBE.OFICINA NOT IN ('157', '306')
	GROUP BY AGRUPACION_BANCARIBE.CUENTA,
			AGRUPACION_BANCARIBE.OFICINA,
			AGRUPACION_BANCARIBE.EMPRESA,
			AGRUPACION_BANCARIBE.CATEGORIA;'''
	hook.run(sql_query_deftxt)

def AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT03_OFICINA_SOBREG (
	CUENTA VARCHAR(12),
	OFICINA VARCHAR(4),
	SALDO NUMERIC(32,7),
	CONSTANTE VARCHAR(1),
	EMPRESA NUMERIC(3)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT03_OFICINA_SOBREG;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT03_OFICINA_SOBREG (
	CUENTA,
	OFICINA, 
	SALDO, 
	CONSTANTE,
	EMPRESA
	)
	SELECT 
		AT03_OFICINA.CUENTA AS CUENTA,
		'0147' AS OFICINA,
		SUM(AT03_OFICINA.SALDO) AS SALDO,
		'1' AS CONSTANTE,
		AT03_OFICINA.EMPRESA AS EMPRESA
	FROM ATSUDEBAN.AT03_OFICINA AS AT03_OFICINA
	WHERE (AT03_OFICINA.SALDO < 0) AND (AT03_OFICINA.CATEGORIA = 'D')
	GROUP BY AT03_OFICINA.CUENTA, AT03_OFICINA.EMPRESA;'''
	hook.run(sql_query_deftxt)

def AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT03_OFICINA_FINAL (
	CUENTA VARCHAR(12),
	OFICINA VARCHAR(4),
	SALDO NUMERIC(32,7),
	CONSTANTE VARCHAR(1),
	EMPRESA NUMERIC(3)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT03_OFICINA_FINAL;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO atsudeban.at03_oficina_final (
	CUENTA, 
	OFICINA, 
	SALDO, 
	CONSTANTE,
	EMPRESA
	)
	SELECT 
		AT03_OFICINA.CUENTA AS CUENTA,
		AT03_OFICINA.OFICINA AS OFICINA,
		AT03_OFICINA.SALDO AS SALDO,
		'1' AS CONSTANTE,
		AT03_OFICINA.EMPRESA AS EMPRESA
	FROM atsudeban.at03_oficina AS AT03_OFICINA
	WHERE (AT03_OFICINA.SALDO <> 0);'''
	hook.run(sql_query_deftxt)

def ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO(**kwargs):
    hook = PostgresHook(postgres_conn_id='repodataprd')

    # ACTUALIZA_SALDO_712002
    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
    SET SALDO = (SELECT SUM(SALDO) FROM ATSUDEBAN.AGRUPACION_BANCARIBE AS AGRUPACION_BANCARIBE WHERE CUENTA IN ('712032000000','712092000000','712012000000'))
    WHERE CUENTA IN ('712002000000');'''
    hook.run(sql_query)

def AT_AGRUPACION_V001_BANCARIBE(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT03_V001_BANCARIBE (
	OFICINA VARCHAR(4),
	CUENTA VARCHAR(12),
	SALDO NUMERIC(32,7),
	CONSTANTE VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT03_V001_BANCARIBE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT03_V001_BANCARIBE (
	OFICINA, 
	CUENTA, 
	SALDO, 
	CONSTANTE
	)
	SELECT 
		'V001' AS OFICINA,
		CUENTA, 
		SUM(SALDO) AS SALDO,
		'1' AS CONSTANTE
	FROM ATSUDEBAN.AT03_OFICINA_FINAL
	GROUP BY CUENTA;'''
	hook.run(sql_query_deftxt)

def Actualizar_cuenta_formato_Sudeban_II(**kwargs):
    hook = PostgresHook(postgres_conn_id='repodataprd')

    # Actualizar cuenta_7330010000
    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
    SET CUENTA = '7330010000'
    WHERE CUENTA = '733011000000';'''
    hook.run(sql_query)

    # Actualizar cuenta_7340010000
    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
    SET CUENTA = '7340020000'
    WHERE CUENTA = '734012000000';'''
    hook.run(sql_query)

    # Actualizar cuenta_7430020000
    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
    SET CUENTA = '7430020000'
    WHERE CUENTA = '743012000000';'''
    hook.run(sql_query)

    # Actualizar cuenta_7530020000
    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
    SET CUENTA = '7530020000'
    WHERE CUENTA = '753042000000';'''
    hook.run(sql_query)

    # Copy of Actualizar cuenta_733001000
    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
    SET CUENTA = '7330010000'
    WHERE CUENTA = '733011000000';'''
    hook.run(sql_query)

    # Copy of Actualizar cuenta_734001000
    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
    SET CUENTA = '7340020000'
    WHERE CUENTA = '734012000000';'''
    hook.run(sql_query)

    # Copy of Actualizar cuenta_743002000
    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
    SET CUENTA = '7430020000'
    WHERE CUENTA = '743012000000';'''
    hook.run(sql_query)

    # Copy of Actualizar cuenta_753002000
    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
    SET CUENTA = '7530020000'
    WHERE CUENTA = '753042000000';'''
    hook.run(sql_query)

    # Actualizar cuenta_734001
    sql_query = '''UPDATE ATSUDEBAN.AT03_OFICINA_FINAL
    SET CUENTA = '7340010000'
    WHERE CUENTA = '734011000000';'''
    hook.run(sql_query)

    # Copy of Actualizar cuenta_734001
    sql_query = '''UPDATE ATSUDEBAN.AT03_V001_BANCARIBE
    SET CUENTA = '7340010000'
    WHERE CUENTA = '734011000000';'''
    hook.run(sql_query)

def ATS_TT_AT03(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT03 (
	OFICINA VARCHAR(5),
	CODIGO_CONTABLE VARCHAR(10),
	SALDO NUMERIC(32,7),
	CONSECUTIVO VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT03;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO atsudeban.ats_th_at03 (
	OFICINA, 
	CODIGO_CONTABLE, 
	SALDO, 
	CONSECUTIVO
	)
	SELECT 
		AT03_OFICINA_FINAL.OFICINA AS OFICINA,
		SUBSTRING(AT03_OFICINA_FINAL.CUENTA FROM 1 FOR 10) AS CODIGO_CONTABLE,
		AT03_OFICINA_FINAL.SALDO AS SALDO,
		AT03_OFICINA_FINAL.CONSTANTE AS CONSECUTIVO
	FROM atsudeban.at03_oficina_final AS AT03_OFICINA_FINAL
	WHERE AT03_OFICINA_FINAL.SALDO <> 0
	
	UNION
	
	SELECT 
		AT03_V001_BANCARIBE.OFICINA AS OFICINA,
		SUBSTRING(AT03_V001_BANCARIBE.CUENTA FROM 1 FOR 10) AS CODIGO_CONTABLE,
		AT03_V001_BANCARIBE.SALDO AS SALDO,
		AT03_V001_BANCARIBE.CONSTANTE AS CONSECUTIVO
	FROM atsudeban.at03_v001_bancaribe AS AT03_V001_BANCARIBE
	WHERE AT03_V001_BANCARIBE.SALDO <> 0;'''
	hook.run(sql_query_deftxt)

	# Borramos tablas TEMPORALES
	#sql_query = '''DROP TABLE IF EXISTS ATSUDEBAN.AT03_OFICINA;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS ATSUDEBAN.AT03_OFICINA_FINAL;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS ATSUDEBAN.AT03_V001_BANCARIBE;'''
	#hook.run(sql_query)

def ATS_TH_AT03_DATACHECK(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT03_ (
	OFICINA VARCHAR(5),
	CODIGO_CONTABLE VARCHAR(10),
	SALDO NUMERIC(32,7),
	CONSECUTIVO VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# Vaciamos la tabla destino antes de la carga
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT03_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT03_ (
	OFICINA,
	CODIGO_CONTABLE,
	SALDO,
	CONSECUTIVO
	)
	SELECT 	
		OFICINA,
		CODIGO_CONTABLE,
		SALDO,
		CONSECUTIVO
	FROM ATSUDEBAN.ATS_TH_AT03
	;'''
	hook.run(sql_query_deftxt)

# WHERE (
# 		(
# 			SUBSTRING(OFICINA FROM 1 FOR 1) IN ('V','I')
# 		) 
# 		AND (
# 			SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 5) IN ('11901','12125','12225','12325','12425','12625','17249','17349','17449','17549') 
# 			OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 3) IN ('129','139','149','159','169') 
# 			OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 8) IN (
# 				'18101102','18101202','18102102','18102202','18103102','18103202','18105102','18105202','18105104',
# 				'18105204','18105106','18105206','18106102','18106202','18107102','18107202','18902102','18902202','18902101',
# 				'18902201','13136102','13236102','13336102','13436102','13136202','13236202','13336202','13436202','13138102',
# 				'13238102','13338102','13438102','13138202','13238202','13338202','13438202','13140102','13240102','13340102',
# 				'13440102','13140202','13240202','13340202','13440202','13142102','13242102','13342102','13442102','13142202',
# 				'13242202','13342202','13442202','13144102','13244102','13344102','13444102','13144202','13244202','13344202',
# 				'13444202','13146102','13246102','13346102','13446102','13146202','13246202','13346202','13446202','13148102',
# 				'13248102','13348102','13448102','13148202','13248202','13348202','13448202','13150102','13250102','13350102',
# 				'13450102','13150202','13250202','13350202','13450202','13152102','13252102','13352102','13452102','13152202',
# 				'13252202','13352202','13452202','13154102','13254102','13354102','13454102','13154202','13254202','13354202',
# 				'13454202'
# 			) 
# 			OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 6) IN ('189011','189012') 
# 			OR SUBSTRING(CODIGO_CONTABLE FROM 1 FOR 10) IN (
# 				'1810810102','1810820102','1810810202','1810820202','1810810302','1810820302','1810810402','1810820402','1810810502','1810820502','1881610102',
# 				'1881620102','1881610202','1881620202','1881610302','1881620302','1881610402','1881620402','1881610502','1881620502','1881610602',
# 				'1881620602','1881610702','1881620702','1881610802','1881620802'
# 			)
# 		)
# 		AND SALDO <= 0
# 		OR (( SUBSTRING(oficina FROM 1 FOR 1) IN ('V','I') ) AND ( SUBSTRING(codigo_contable FROM 1 FOR 1) = '1' )
# 		AND (
# 			SUBSTRING(codigo_contable FROM 1 FOR 5) NOT IN ('11901','12125','12225','12325','12425','12625','17249','17349','17449','17549') 
# 			OR SUBSTRING(codigo_contable FROM 1 FOR 3) NOT IN ('129','139','149','159','169') 
# 			OR SUBSTRING(codigo_contable FROM 1 FOR 8) NOT IN (
# 				'18101102','18101202','18102102','18102202','18103102','18103202','18105102','18105202','18105104',
# 				'18105204','18105106','18105206','18106102','18106202','18107102','18107202','18902102','18902202','18902101',
# 				'18902201','13136102','13236102','13336102','13436102','13136202','13236202','13336202','13436202','13138102',
# 				'13238102','13338102','13438102','13138202','13238202','13338202','13438202','13140102','13240102','13340102',
# 				'13440102','13140202','13240202','13340202','13440202','13142102','13242102','13342102','13442102','13142202',
# 				'13242202','13342202','13442202','13144102','13244102','13344102','13444102','13144202','13244202','13344202',
# 				'13444202','13146102','13246102','13346102','13446102','13146202','13246202','13346202','13446202','13148102',
# 				'13248102','13348102','13448102','13148202','13248202','13348202','13448202','13150102','13250102','13350102',
# 				'13450102','13150202','13250202','13350202','13450202','13152102','13252102','13352102','13452102','13152202',
# 				'13252202','13352202','13452202','13154102','13254102','13354102','13454102','13154202','13254202','13354202',
# 				'13454202'
# 			) 
# 			OR SUBSTRING(codigo_contable FROM 1 FOR 6) NOT IN ('189011','189012') 
# 			OR SUBSTRING(codigo_contable FROM 1 FOR 10) NOT IN (
# 				'1810810102','1810820102','1810810202','1810820202','1810810302','1810820302','1810810402','1810820402','1810810502','1810820502','1881610102',
# 				'1881620102','1881610202','1881620202','1881610302','1881620302','1881610402','1881620402','1881610502','1881620502','1881610602',
# 				'1881620602','1881610702','1881620702','1881610802','1881620802'
# 			)
# 		)
# 		AND saldo >= 0)
# 	)



###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT03',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task = PythonOperator(
	task_id='IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task',
	python_callable=IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS,
	dag=dag
)

AT03_AGRUPACION_CONTABLE_BANCARIBE_task = PythonOperator(
	task_id='AT03_AGRUPACION_CONTABLE_BANCARIBE_task',
	python_callable=AT03_AGRUPACION_CONTABLE_BANCARIBE,
	dag=dag
)

Actualizar_cuenta_formato_Sudeban_task = PythonOperator(
	task_id='Actualizar_cuenta_formato_Sudeban_task',
	python_callable=Actualizar_cuenta_formato_Sudeban,
	dag=dag
)

AT03_AGRUPACION_OFICINAS_BANCARIBE_task = PythonOperator(
	task_id='AT03_AGRUPACION_OFICINAS_BANCARIBE_task',
	python_callable=AT03_AGRUPACION_OFICINAS_BANCARIBE,
	dag=dag
)

AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task = PythonOperator(
	task_id='AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task',
	python_callable=AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA,
	dag=dag
)

AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task = PythonOperator(
	task_id='AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task',
	python_callable=AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL,
	dag=dag
)

ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task = PythonOperator(
    task_id='ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task',
    python_callable=ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO,
    dag=dag
)

AT_AGRUPACION_V001_BANCARIBE_task = PythonOperator(
	task_id='AT_AGRUPACION_V001_BANCARIBE_task',
	python_callable=AT_AGRUPACION_V001_BANCARIBE,
	dag=dag
)

Actualizar_cuenta_formato_Sudeban_II_task = PythonOperator(
    task_id='Actualizar_cuenta_formato_Sudeban_II_task',
    python_callable=Actualizar_cuenta_formato_Sudeban_II,
    dag=dag
)

ATS_TT_AT03_task = PythonOperator(
	task_id='ATS_TT_AT03_task',
	python_callable=ATS_TT_AT03,
	dag=dag
)

ATS_TH_AT03_DATACHECK_task = PythonOperator(
	task_id='ATS_TH_AT03_DATACHECK_task',
	python_callable=ATS_TH_AT03_DATACHECK,
	dag=dag
)


###### SECUENCIA DE EJECUCION ######
IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task >> AT03_AGRUPACION_CONTABLE_BANCARIBE_task >> Actualizar_cuenta_formato_Sudeban_task >> AT03_AGRUPACION_OFICINAS_BANCARIBE_task >> AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task >> AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task >> ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task >> AT_AGRUPACION_V001_BANCARIBE_task >> Actualizar_cuenta_formato_Sudeban_II_task >> ATS_TT_AT03_task >> ATS_TH_AT03_DATACHECK_task
