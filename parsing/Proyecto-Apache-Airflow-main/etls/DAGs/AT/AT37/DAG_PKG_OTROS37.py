from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
import time

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

def AT37_Otros_cb(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.CB (
	re_ofconta	NUMERIC(8),
	re_ofadmin	NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# truncar 
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.CB;'''
	hook.run(sql_query_deftxt)

	# insertar en destino
	sql_query_deftxt = '''INSERT INTO at_stg.cb (
	RE_OFCONTA, 
	RE_OFADMIN
	)
	SELECT
		cb_relofi.re_ofconta,
		cb_relofi.re_ofadmin
	FROM ods.cb_relofi AS cb_relofi
	WHERE (cb_relofi.re_empresa = 1) AND (cb_relofi.re_filial = 1);'''
	hook.run(sql_query_deftxt)

def AT37_Otros_con(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.cc_con (
	cv_producto NUMERIC(3),
	cv_codigo_cta CHAR(5),
	cc_oficina NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# truncar
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.CC_CON;'''
	hook.run(sql_query_deftxt)

	# insertar en destino
	sql_query_deftxt = '''INSERT INTO at_stg.cc_con (
	CV_PRODUCTO, 
	CV_CODIGO_CTA, 
	CC_OFICINA
	)
	SELECT
		CC_CONVERSION.CV_PRODUCTO,
		CC_CONVERSION.CV_CODIGO_CTA,
		CC_CONVERSION.CV_OFICINA
	FROM ODS.CC_CONVERSION
	WHERE (CC_CONVERSION.CV_PRODUCTO IN (3,4));'''
	hook.run(sql_query_deftxt)

def AT37_Otros_ccentes(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.ccente37_otros (
	en_ced_ruc VARCHAR(40),
	cuenta_cliente CHAR(24),
	cliente VARCHAR(50),
	en_nombre VARCHAR(255),
	en_nomlar VARCHAR(255),
	p_pasaporte VARCHAR(50),
	tipo_cuenta VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.CCENTE37_OTROS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.CCENTE37_OTROS (
	EN_CED_RUC, 
	CUENTA_CLIENTE, 
	CLIENTE, 
	EN_NOMBRE, 
	EN_NOMLAR, 
	P_PASAPORTE, 
	TIPO_CUENTA
	)   
	SELECT
		CL_ENTE.EN_CED_RUC,
		CC_CTACTE_AT.CC_CTA_BANCO,
		CC_CTACTE_AT.CC_CLIENTE,
		CL_ENTE.EN_NOMBRE,
		CL_ENTE.EN_NOMLAR,
		CL_ENTE.P_PASAPORTE,
		'2' AS TIPO_CUENTA
	FROM
		ODS.CL_ENTE AS CL_ENTE INNER JOIN ODS.CC_CTACTE_AT AS CC_CTACTE_AT ON (CL_ENTE.EN_ENTE = CC_CTACTE_AT.CC_CLIENTE)
	WHERE (CC_CTACTE_AT.CC_FECHA_CARGA_AT IN (SELECT MAX(CC_FECHA_CARGA_AT) FROM ODS.CC_CTACTE_AT))
	  
	UNION  

	SELECT
		CL_ENTE.EN_CED_RUC,
		AH_CUENTA_AT.AH_CTA_BANCO,
		AH_CUENTA_AT.AH_CLIENTE,
		CL_ENTE.EN_NOMBRE,
		CL_ENTE.EN_NOMLAR,
		CL_ENTE.P_PASAPORTE,
		'1' AS TIPO_CUENTA
	FROM
		ODS.CL_ENTE AS CL_ENTE INNER JOIN ODS.AH_CUENTA_AT AS AH_CUENTA_AT ON (CL_ENTE.EN_ENTE = AH_CUENTA_AT.AH_CLIENTE)
	WHERE (AH_CUENTA_AT.AH_FECHA_CARGA_AT IN (SELECT MAX(AH_FECHA_CARGA_AT) FROM ODS.AH_CUENTA_AT)) ;'''
	hook.run(sql_query_deftxt)

def AT37_Otros_pre(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')
	at37_montobs = get_variable('AT37_MONTOBS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.PRESENTADOS_HIS (
	PR_IDENT_BENEFICIARIO VARCHAR(12),
	PR_NOMBRE_BENEFICIARIO VARCHAR(35),
	PR_CTA_DESTINO VARCHAR(20),
	PR_MONTO NUMERIC(15,2),
	PR_FECH_COMP DATE,
	PR_SECUENCIAL NUMERIC(10),
	PR_CONCEPTO VARCHAR(80),
	PR_IDENT_EMISOR VARCHAR(12),
	PR_NOMBRE_EMISOR VARCHAR(35),
	PR_CTA_ORIGEN VARCHAR(20),
	CV_OFICINA NUMERIC(5),
	COD_CTA CHAR(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.PRESENTADOS_HIS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.presentados_his (
	PR_IDENT_BENEFICIARIO,
	PR_NOMBRE_BENEFICIARIO,
	PR_CTA_DESTINO,
	PR_MONTO,
	PR_FECH_COMP,
	PR_SECUENCIAL,
	PR_CONCEPTO,
	PR_IDENT_EMISOR,
	PR_NOMBRE_EMISOR,
	PR_CTA_ORIGEN,
	CV_OFICINA,
	COD_CTA
	)
	SELECT DISTINCT
		re_sip_presentados_his.pr_ident_beneficiario,
		re_sip_presentados_his.pr_nombre_beneficiario,
		re_sip_presentados_his.pr_cta_destino,
		re_sip_presentados_his.pr_monto,
		re_sip_presentados_his.pr_fech_comp,
		re_sip_presentados_his.pr_secuencial,
		re_sip_presentados_his.pr_concepto,
		re_sip_presentados_his.pr_ident_emisor,
		re_sip_presentados_his.pr_nombre_emisor,
		re_sip_presentados_his.pr_cta_origen,
		CAST(SUBSTRING(re_sip_presentados_his.pr_cta_destino FROM 5 FOR 4) AS NUMERIC),
		SUBSTRING(re_sip_presentados_his.pr_cta_destino FROM 11 FOR 4)
	FROM ods.re_sip_presentados_his AS re_sip_presentados_his
	WHERE re_sip_presentados_his.pr_tipo_arch = 'CRO' AND re_sip_presentados_his.pr_cod_operacion = 10
		AND re_sip_presentados_his.pr_monto >= CAST({at37_montobs} AS NUMERIC)
		AND re_sip_presentados_his.pr_fech_comp BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY');'''
	hook.run(sql_query_deftxt)

def AT37_Otros_pre_arch(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')
	at37_montobs = get_variable('AT37_MONTOBS')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.PRESENTADOS_HIS_ARCH (
	PR_IDENT_BENEFICIARIO	VARCHAR(12),
	PR_NOMBRE_BENEFICIARIO	VARCHAR(35),
	PR_CTA_DESTINO	VARCHAR(20),
	PR_MONTO	NUMERIC(15,2),
	PR_FECH_COMP	DATE,
	PR_SECUENCIAL	NUMERIC(10),
	PR_CONCEPTO	VARCHAR(80),
	PR_IDENT_EMISOR	VARCHAR(12),
	PR_NOMBRE_EMISOR	VARCHAR(35),
	PR_CTA_ORIGEN	VARCHAR(20),
	CV_OFICINA	NUMERIC(5),
	COD_CTA	CHAR(5)
	);'''
	hook.run(sql_query_deftxt)

	# truncar
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.PRESENTADOS_HIS_ARCH;'''
	hook.run(sql_query_deftxt)

	# insertar en destino
	sql_query_deftxt = f'''INSERT INTO at_stg.presentados_his_arch (
	pr_ident_beneficiario,
	pr_nombre_beneficiario,
	pr_cta_destino,
	pr_monto,
	pr_fech_comp,
	pr_secuencial,
	pr_concepto,
	pr_ident_emisor,
	pr_nombre_emisor,
	pr_cta_origen,
	cv_oficina,
	cod_cta
	)
	SELECT DISTINCT
		RE_SIP_PRESENTADOS_HIS.PR_IDENT_EMISOR,
		RE_SIP_PRESENTADOS_HIS.PR_NOMBRE_EMISOR,
		RE_SIP_PRESENTADOS_HIS.PR_CTA_ORIGEN,
		RE_SIP_PRESENTADOS_HIS.PR_MONTO,
		RE_SIP_PRESENTADOS_HIS.PR_FECH_COMP,
		RE_SIP_PRESENTADOS_HIS.PR_SECUENCIAL,
		RE_SIP_PRESENTADOS_HIS.PR_CONCEPTO,
		RE_SIP_PRESENTADOS_HIS.PR_IDENT_BENEFICIARIO,
		RE_SIP_PRESENTADOS_HIS.PR_NOMBRE_BENEFICIARIO,
		RE_SIP_PRESENTADOS_HIS.PR_CTA_DESTINO,
		CAST(SUBSTRING(RE_SIP_PRESENTADOS_HIS.PR_CTA_ORIGEN FROM 5 FOR 4) AS NUMERIC),
		SUBSTRING(RE_SIP_PRESENTADOS_HIS.PR_CTA_ORIGEN FROM 11 FOR 4)
	FROM ODS.RE_SIP_PRESENTADOS_HIS AS RE_SIP_PRESENTADOS_HIS
	WHERE (RE_SIP_PRESENTADOS_HIS.PR_FECH_COMP BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY'))
	AND (RE_SIP_PRESENTADOS_HIS.PR_COD_OPERACION = 110)
	AND (RE_SIP_PRESENTADOS_HIS.PR_MONTO >= CAST({at37_montobs} AS NUMERIC))
	AND (RE_SIP_PRESENTADOS_HIS.PR_TIPO_ARCH = 'CRO');'''
	hook.run(sql_query_deftxt)

def AT37_Otros_pre_ope(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')
	at37_montobs = get_variable('AT37_MONTOBS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.presentados_his_ope (
	pr_ident_beneficiario	VARCHAR(12),
	pr_nombre_beneficiario	VARCHAR(35),
	pr_cta_destino	VARCHAR(20),
	pr_monto	NUMERIC(15,2),
	pr_fech_comp	DATE,
	pr_secuencial	NUMERIC(10),
	pr_concepto	VARCHAR(80),
	pr_ident_emisor	VARCHAR(12),
	pr_nombre_emisor	VARCHAR(35),
	pr_cta_origen	VARCHAR(20),
	cv_oficina	NUMERIC(5),
	cod_cta	CHAR(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.PRESENTADOS_HIS_OPE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.presentados_his_ope (
	pr_ident_beneficiario,
	pr_nombre_beneficiario,
	pr_cta_destino,
	pr_monto,
	pr_fech_comp,
	pr_secuencial,
	pr_concepto,
	pr_ident_emisor,
	pr_nombre_emisor,
	pr_cta_origen,
	cv_oficina,
	cod_cta
	)
	SELECT DISTINCT
		RE_SIP_PRESENTADOS_HIS.PR_IDENT_EMISOR,
		RE_SIP_PRESENTADOS_HIS.PR_NOMBRE_EMISOR,
		RE_SIP_PRESENTADOS_HIS.PR_CTA_ORIGEN,
		RE_SIP_PRESENTADOS_HIS.PR_MONTO,
		RE_SIP_PRESENTADOS_HIS.PR_FECH_COMP,
		RE_SIP_PRESENTADOS_HIS.PR_SECUENCIAL,
		RE_SIP_PRESENTADOS_HIS.PR_CONCEPTO,
		RE_SIP_PRESENTADOS_HIS.PR_IDENT_BENEFICIARIO,
		RE_SIP_PRESENTADOS_HIS.PR_NOMBRE_BENEFICIARIO,
		RE_SIP_PRESENTADOS_HIS.PR_CTA_DESTINO,
		CAST(SUBSTRING(RE_SIP_PRESENTADOS_HIS.PR_CTA_ORIGEN FROM 5 FOR 4) AS NUMERIC),
		SUBSTRING(RE_SIP_PRESENTADOS_HIS.PR_CTA_ORIGEN FROM 11 FOR 4)
	FROM ODS.RE_SIP_PRESENTADOS_HIS AS RE_SIP_PRESENTADOS_HIS
	WHERE (RE_SIP_PRESENTADOS_HIS.PR_FECH_COMP BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY'))
	AND (RE_SIP_PRESENTADOS_HIS.PR_COD_OPERACION = 10)
	AND (RE_SIP_PRESENTADOS_HIS.PR_MONTO >= CAST({at37_montobs} AS NUMERIC))
	AND (RE_SIP_PRESENTADOS_HIS.PR_TIPO_ARCH = 'LOT');;'''
	hook.run(sql_query_deftxt)

def AT37_Otros_Union(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.PRESENTADOS_UNION (
	PR_IDENT_BENEFICIARIO VARCHAR(12),
	PR_NOMBRE_BENEFICIARIO VARCHAR(35),
	PR_CTA_DESTINO VARCHAR(20),
	PR_MONTO NUMERIC(15,2),
	PR_FECH_COMP DATE,
	PR_SECUENCIAL NUMERIC(10),
	PR_CONCEPTO VARCHAR(80),
	PR_IDENT_EMISOR VARCHAR(12),
	PR_NOMBRE_EMISOR VARCHAR(35),
	PR_CTA_ORIGEN VARCHAR(20),
	CV_OFICINA NUMERIC(5),
	COD_CTA CHAR(5),
	TIPO_HIS VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.PRESENTADOS_UNION;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.presentados_union (
	pr_ident_beneficiario,
	pr_nombre_beneficiario,
	pr_cta_destino,
	pr_monto,
	pr_fech_comp,
	pr_secuencial,
	pr_concepto,
	pr_ident_emisor,
	pr_nombre_emisor,
	pr_cta_origen,
	cv_oficina,
	cod_cta,
	tipo_his
	)
	SELECT
		pr_ident_beneficiario,
		pr_nombre_beneficiario,
		pr_cta_destino,
		pr_monto,
		pr_fech_comp,
		pr_secuencial,
		pr_concepto,
		pr_ident_emisor,
		pr_nombre_emisor,
		pr_cta_origen,
		cv_oficina,
		cod_cta,
		'1' AS tipo_his
	FROM at_stg.presentados_his;'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''There is no Oracle SQL code provided to convert.'''
	hook.run(sql_query_deftxt)

def AT37_OTROS_UNION_(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.PRESENTADOS_UNION_ (
	PR_IDENT_BENEFICIARIO	VARCHAR(12),
	PR_NOMBRE_BENEFICIARIO	VARCHAR(35),
	PR_CTA_DESTINO	VARCHAR(20),
	PR_MONTO	NUMERIC(15,2),
	PR_FECH_COMP	DATE,
	PR_SECUENCIAL	NUMERIC(10),
	PR_CONCEPTO	VARCHAR(80),
	PR_IDENT_EMISOR	VARCHAR(12),
	PR_NOMBRE_EMISOR	VARCHAR(35),
	PR_CTA_ORIGEN	VARCHAR(20),
	CV_OFICINA	NUMERIC(5),
	COD_CTA	CHAR(5),
	TIPO_HIS	VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.PRESENTADOS_UNION_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.presentados_union_ (
	pr_ident_beneficiario,
	pr_nombre_beneficiario,
	pr_cta_destino,
	pr_monto,
	pr_fech_comp,
	pr_secuencial,
	pr_concepto,
	pr_ident_emisor,
	pr_nombre_emisor,
	pr_cta_origen,
	cv_oficina,
	cod_cta,
	tipo_his
	)
	SELECT 
		PRESENTADOS_HIS_ARCH.PR_IDENT_EMISOR AS PR_IDENT_BENEFICIARIO,
		PRESENTADOS_HIS_ARCH.PR_NOMBRE_EMISOR AS PR_NOMBRE_BENEFICIARIO,
		PRESENTADOS_HIS_ARCH.PR_CTA_ORIGEN AS PR_CTA_DESTINO,
		PRESENTADOS_HIS_ARCH.PR_MONTO AS PR_MONTO,
		PRESENTADOS_HIS_ARCH.PR_FECH_COMP AS PR_FECH_COMP,
		PRESENTADOS_HIS_ARCH.PR_SECUENCIAL AS PR_SECUENCIAL,
		PRESENTADOS_HIS_ARCH.PR_CONCEPTO AS PR_CONCEPTO,
		PRESENTADOS_HIS_ARCH.PR_IDENT_BENEFICIARIO AS PR_IDENT_EMISOR,
		PRESENTADOS_HIS_ARCH.PR_NOMBRE_BENEFICIARIO AS PR_NOMBRE_EMISOR,
		PRESENTADOS_HIS_ARCH.PR_CTA_DESTINO AS PR_CTA_ORIGEN,
		PRESENTADOS_HIS_ARCH.CV_OFICINA AS CV_OFICINA,
		PRESENTADOS_HIS_ARCH.COD_CTA AS COD_CTA,
		'3' AS TIPO_HIS
	FROM at_stg.presentados_his_arch AS presentados_his_arch

	UNION

	SELECT 
		PRESENTADOS_HIS_OPE.PR_IDENT_EMISOR AS PR_IDENT_BENEFICIARIO,
		PRESENTADOS_HIS_OPE.PR_NOMBRE_EMISOR AS PR_NOMBRE_BENEFICIARIO,
		PRESENTADOS_HIS_OPE.PR_CTA_ORIGEN AS PR_CTA_DESTINO,
		PRESENTADOS_HIS_OPE.PR_MONTO AS PR_MONTO,
		PRESENTADOS_HIS_OPE.PR_FECH_COMP AS PR_FECH_COMP,
		PRESENTADOS_HIS_OPE.PR_SECUENCIAL AS PR_SECUENCIAL,
		PRESENTADOS_HIS_OPE.PR_CONCEPTO AS PR_CONCEPTO,
		PRESENTADOS_HIS_OPE.PR_IDENT_BENEFICIARIO AS PR_IDENT_EMISOR,
		PRESENTADOS_HIS_OPE.PR_NOMBRE_BENEFICIARIO AS PR_NOMBRE_EMISOR,
		PRESENTADOS_HIS_OPE.PR_CTA_DESTINO AS PR_CTA_ORIGEN,
		PRESENTADOS_HIS_OPE.CV_OFICINA AS CV_OFICINA,
		PRESENTADOS_HIS_OPE.COD_CTA AS COD_CTA,
		'2' AS TIPO_HIS
	FROM at_stg.presentados_his_ope;'''
	hook.run(sql_query_deftxt)

def AT37_CONVERSION(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT37_CONVER (
	CV_OFICINA NUMERIC(5),
	CV_CODIGO_CTA VARCHAR(5),
	CV_PRODUCTO NUMERIC(3)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_CONVER;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT37_CONVER (
	CV_OFICINA, 
	CV_CODIGO_CTA, 
	CV_PRODUCTO
	)  
	SELECT
		CC_CONVERSION.CV_OFICINA,
		CC_CONVERSION.CV_CODIGO_CTA,
		CC_CONVERSION.CV_PRODUCTO
	FROM ODS.CC_CONVERSION AS CC_CONVERSION
	WHERE (CC_CONVERSION.CV_PRODUCTO IN (3,4));'''
	hook.run(sql_query_deftxt)

def AT37_OTRO_PRE_CC(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	Moneda_Vzla = get_variable('Moneda_Vzla')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.HIS_ENTE_ (
	TIPOTRANSFERENCIA	NUMERIC(1),
	CLASIFICACION	NUMERIC(1),
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	NUMERIC(1),
	CUENTACLIENTE	VARCHAR(20),
	MONEDA	VARCHAR(3),
	MONTO	NUMERIC(24,2),
	FECHA	VARCHAR(8),
	REFERENCIA	VARCHAR(20),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(20),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	NUMERIC(1),
	CUENTACONTRAPARTE	VARCHAR(20),
	ENTE NUMERIC(5),
	CANAL NUMERIC(5),
	ENTE_D NUMERIC(5),
	OFICINA	NUMERIC(8)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.HIS_ENTE_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.HIS_ENTE_ (
	TIPOTRANSFERENCIA,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA,
	CLASIFICACION,
	MONEDA,
	TIPOINSTRUMENTOCONTRAPARTE
	DIRECCIONIP,
	ENTE,
	CANAL,
	ENTE_D
	) 
	SELECT
		CASE WHEN PRESENTADOS_UNION.TIPO_HIS = '1' THEN '2'
			 WHEN PRESENTADOS_UNION.TIPO_HIS = '2' THEN '1'
			 WHEN PRESENTADOS_UNION.TIPO_HIS = '3' THEN '3'
		END AS TIPOTRANSFERENCIA,
		CASE
			WHEN SUBSTRING(PRESENTADOS_UNION.PR_IDENT_BENEFICIARIO FROM 1 FOR 1) IS NULL THEN 'P'
			ELSE SUBSTRING(PRESENTADOS_UNION.PR_IDENT_BENEFICIARIO FROM 1 FOR)
		END AS TIPOCLIENTE,
		CASE
			WHEN TRIM(PRESENTADOS_UNION.PR_IDENT_BENEFICIARIO) IS NOT NULL THEN SUBSTRING(PRESENTADOS_UNION.PR_IDENT_BENEFICIARIO FROM 2 FOR LENGTH(PRESENTADOS_UNION.PR_IDENT_BENEFICIARIO))
		END AS IDENTIFICACIONCLIENTE,
		UPPER(PRESENTADOS_UNION.PR_NOMBRE_BENEFICIARIO) AS NOMBRECLIENTE,
		CASE
			WHEN AT37_CONVER.CV_PRODUCTO = 3 THEN 2
			WHEN AT37_CONVER.CV_PRODUCTO = 4 THEN 1
		END AS TIPOINSTRUMENTO,
		PRESENTADOS_UNION.PR_CTA_DESTINO AS CUENTACLIENTE,
		PRESENTADOS_UNION.PR_MONTO AS MONTO,
		CONCAT('20', TO_CHAR(TO_DATE(PRESENTADOS_UNION.PR_FECH_COMP, 'DD/MM/YYYY'), 'YYMMDD')) AS FECHA,
		CAST(PRESENTADOS_UNION.PR_SECUENCIAL AS TEXT) AS REFERENCIA,
		CASE
			WHEN SUBSTRING(PRESENTADOS_UNION.PR_IDENT_BENEFICIARIO FROM 1 FOR 1) IN ('F', 'M') THEN 1
			ELSE 0
		END AS EMPRESAFORMACION,
		CASE
			WHEN TRIM(PRESENTADOS_UNION.PR_CONCEPTO) IS NOT NULL THEN TRIM(PRESENTADOS_UNION.PR_CONCEPTO)
			ELSE 'NO DISPONIBLE'
		END AS MOTIVO,
		CASE
			WHEN PRESENTADOS_UNION.PR_IDENT_EMISOR IS NULL THEN 'NO DISPONIBLE'
			ELSE SUBSTRING(PRESENTADOS_UNION.PR_IDENT_EMISOR FROM 1 FOR 1)
		END AS TIPOCONTRAPARTE,
		CASE
			WHEN PRESENTADOS_UNION.PR_IDENT_EMISOR IS NULL THEN 'NO DISPONIBLE'
			ELSE SUBSTRING(PRESENTADOS_UNION.PR_IDENT_EMISOR FROM 2 FOR LENGTH(PRESENTADOS_UNION.PR_IDENT_EMISOR))
		END AS IDENTIFICACIONCONTRAPARTE,
		UPPER(PRESENTADOS_UNION.PR_NOMBRE_EMISOR) AS NOMBRECONTRAPARTE,
		PRESENTADOS_UNION.PR_CTA_ORIGEN AS CUENTACONTRAPARTE,
		PRESENTADOS_UNION.CV_OFICINA AS OFICINA,
		2 AS CLASIFICACION,
		'{Moneda_Vzla}' AS MONEDA,
		0 AS TIPOINSTRUMENTOCONTRAPARTE,
		NULL AS DIRECCIONIP,
		NULL AS ENTE,
		NULL AS CANAL,
		NULL AS ENTE_D
	FROM AT_STG.AT37_CONVER AS AT37_CONVER INNER JOIN AT_STG.PRESENTADOS_UNION AS PRESENTADOS_UNION
		ON CAST(SUBSTRING(PRESENTADOS_UNION.PR_CTA_DESTINO FROM 5 FOR 4) AS NUMERIC) = AT37_CONVER.CV_OFICINA
		AND TRIM(SUBSTRING(PRESENTADOS_UNION.PR_CTA_DESTINO FROM 11 FOR 4)) = TRIM(AT37_CONVER.CV_CODIGO_CTA);'''
	hook.run(sql_query_deftxt)

def AT37_OTRO_PRE_CC_D(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	Moneda_Vzla = get_variable('Moneda_Vzla')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.HIS_ENTE_HD (
	TIPOTRANSFERENCIA	NUMERIC(1),
	CLASIFICACION	NUMERIC(1),
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	NUMERIC(1),
	CUENTACLIENTE	VARCHAR(20),
	MONEDA	VARCHAR(3),
	MONTO	NUMERIC(24,2),
	FECHA	VARCHAR(8),
	REFERENCIA	VARCHAR(20),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(20),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	NUMERIC(1),
	CUENTACONTRAPARTE	VARCHAR(20),
	ENTE NUMERIC(5),
	CANAL NUMERIC(5),
	ENTE_D NUMERIC(5),
	OFICINA	NUMERIC(8)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.HIS_ENTE_HD;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.his_ente_hd (
	TIPOTRANSFERENCIA,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA,
	CLASIFICACION,
	MONEDA,
	TIPOINSTRUMENTOCONTRAPARTE
	DIRECCIONIP,
	ENTE,
	CANAL,
	ENTE_D
	)
	SELECT
		CASE 
			WHEN presentados_union.tipo_his = '1' THEN '2'
			WHEN presentados_union.tipo_his = '2' THEN '1'
			WHEN presentados_union.tipo_his = '3' THEN '3'
		END AS tipotransferencia,
		CASE 
			WHEN SUBSTRING(presentados_union.pr_ident_emisor FROM 1 FOR 1) IS NULL THEN 'P'
			ELSE SUBSTRING(presentados_union.pr_ident_emisor FROM 1 FOR 1)
		END AS tipocliente,
		CASE 
			WHEN TRIM(presentados_union.pr_ident_emisor) IS NOT NULL THEN SUBSTRING(presentados_union.pr_ident_emisor FROM 2 FOR LENGTH(presentados_union.pr_ident_emisor))
		END AS identificacioncliente,
		UPPER(presentados_union.pr_nombre_emisor) AS nombrecliente,
		CASE 
			WHEN at37_conver.cv_producto = 3 THEN 2
			WHEN at37_conver.cv_producto = 4 THEN 1
		END AS tipoinstrumento,
		presentados_union.pr_cta_origen AS cuentacliente,
		presentados_union.pr_monto AS monto,
		CONCAT('20', TO_CHAR(TO_DATE(presentados_union.pr_fech_comp, 'DD/MM/YYYY'), 'YYMMDD')) AS fecha,
		CAST(presentados_union.pr_secuencial AS TEXT) AS referencia,
		CASE 
			WHEN SUBSTRING(presentados_union.pr_ident_beneficiario FROM 1 FOR 1) IN ('F', 'M') THEN 1
			ELSE 0
		END AS empresaformacion,
		CASE 
			WHEN TRIM(presentados_union.pr_concepto) IS NOT NULL THEN TRIM(presentados_union.pr_concepto)
			ELSE 'NO DISPONIBLE'
		END AS motivo,
		CASE 
			WHEN presentados_union.pr_ident_beneficiario IS NULL THEN 'NO DISPONIBLE'
			ELSE SUBSTRING(presentados_union.pr_ident_beneficiario FROM 1 FOR 1)
		END AS tipocontraparte,
		CASE 
			WHEN presentados_union.pr_ident_beneficiario IS NULL THEN 'NO DISPONIBLE'
			ELSE SUBSTRING(presentados_union.pr_ident_beneficiario FROM 2 FOR LENGTH(presentados_union.pr_ident_beneficiario))
		END AS identificacioncontraparte,
		UPPER(presentados_union.pr_nombre_beneficiario) AS nombrecontraparte,
		presentados_union.pr_cta_destino AS cuentacontraparte,
		presentados_union.cv_oficina AS oficina,
		2 AS CLASIFICACION,
		'{Moneda_Vzla}' AS MONEDA,
		0 AS TIPOINSTRUMENTOCONTRAPARTE,
		NULL AS DIRECCIONIP,
		NULL AS ENTE,
		NULL AS CANAL,
		NULL AS ENTE_D
	FROM at_stg.at37_conver AS at37_conver INNER JOIN ls_at_stg.presentados_union_ AS presentados_union
		ON CAST(SUBSTRING(presentados_union.pr_cta_origen FROM 5 FOR 4) AS NUMERIC) = at37_conver.cv_oficina
		AND TRIM(SUBSTRING(presentados_union.pr_cta_origen FROM 11 FOR 4)) = TRIM(at37_conver.cv_codigo_cta);'''
	hook.run(sql_query_deftxt)

def AT37_HIS_ENTE_HD_(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_his_ente_hd_ (
	TIPOTRANSFERENCIA	NUMERIC(1),
	CLASIFICACION	NUMERIC(1),
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	NUMERIC(1),
	CUENTACLIENTE	VARCHAR(20),
	MONEDA	VARCHAR(3),
	MONTO	NUMERIC(24,2),
	FECHA	VARCHAR(8),
	REFERENCIA	VARCHAR(20),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(20),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	NUMERIC(1),
	CUENTACONTRAPARTE	VARCHAR(20),
	ENTE NUMERIC(5),
	CANAL NUMERIC(5),
	ENTE_D NUMERIC(5),
	OFICINA	NUMERIC(8)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_HIS_ENTE_HD_'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at37_his_ente_hd_ (
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	ENTE,
	CANAL,
	ENTE_D,
	OFICINA
	)
	SELECT DISTINCT
		TIPOTRANSFERENCIA,
		CLASIFICACION,
		TIPOCLIENTE,
		IDENTIFICACIONCLIENTE,
		NOMBRECLIENTE,
		TIPOINSTRUMENTO,
		CUENTACLIENTE,
		MONEDA,
		MONTO,
		FECHA,
		REFERENCIA,
		EMPRESAFORMACION,
		MOTIVO,
		NULL AS DIRECCIONIP,
		TIPOCONTRAPARTE,
		IDENTIFICACIONCONTRAPARTE,
		NOMBRECONTRAPARTE,
		TIPOINSTRUMENTOCONTRAPARTE,
		CUENTACONTRAPARTE,
		NULL AS ENTE,
		NULL AS CANAL,
		NULL AS ENTE_D,
		OFICINA
	FROM AT_STG.HIS_ENTE_
	
	UNION

	SELECT DISTINCT
		TIPOTRANSFERENCIA,
		CLASIFICACION,
		TIPOCLIENTE,
		IDENTIFICACIONCLIENTE,
		NOMBRECLIENTE,
		TIPOINSTRUMENTO,
		CUENTACLIENTE,
		MONEDA,
		MONTO,
		FECHA,
		REFERENCIA,
		EMPRESAFORMACION,
		MOTIVO,
		NULL AS DIRECCIONIP,
		TIPOCONTRAPARTE,
		IDENTIFICACIONCONTRAPARTE,
		NOMBRECONTRAPARTE,
		TIPOINSTRUMENTOCONTRAPARTE,
		CUENTACONTRAPARTE,
		NULL AS ENTE,
		NULL AS CANAL,
		NULL AS ENTE_D,
		OFICINA
	FROM AT_STG.HIS_ENTE_HD;'''
	hook.run(sql_query_deftxt)

def AT37_OTROS_SOURCE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_otros_source1 (
	TIPOTRANSFERENCIA	NUMERIC(1),
	CLASIFICACION	NUMERIC(1),
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	NUMERIC(1),
	CUENTACLIENTE	VARCHAR(20),
	MONEDA	VARCHAR(3),
	MONTO	NUMERIC(24,2),
	FECHA	VARCHAR(8),
	REFERENCIA	VARCHAR(20),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(20),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	NUMERIC(1),
	CUENTACONTRAPARTE	VARCHAR(20),
	ENTE NUMERIC(5),
	CANAL NUMERIC(5),
	ENTE_D NUMERIC(5),
	OFICINA	NUMERIC(8)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_OTROS_SOURCE1;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at37_otros_source1 (
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	ENTE,
	CANAL,
	ENTE_D,
	OFICINA
	)
	SELECT DISTINCT
		HIS_ENTE_HD.TIPOTRANSFERENCIA,
		HIS_ENTE_HD.CLASIFICACION,
		HIS_ENTE_HD.TIPOCLIENTE,
		LTRIM(HIS_ENTE_HD.IDENTIFICACIONCLIENTE, '0'),
		HIS_ENTE_HD.NOMBRECLIENTE,
		HIS_ENTE_HD.TIPOINSTRUMENTO,
		HIS_ENTE_HD.CUENTACLIENTE,
		HIS_ENTE_HD.MONEDA,
		HIS_ENTE_HD.MONTO,
		HIS_ENTE_HD.FECHA,
		HIS_ENTE_HD.REFERENCIA,
		HIS_ENTE_HD.EMPRESAFORMACION,
		HIS_ENTE_HD.MOTIVO,
		NULL AS DIRECCIONIP,
		HIS_ENTE_HD.TIPOCONTRAPARTE,
		HIS_ENTE_HD.IDENTIFICACIONCONTRAPARTE,
		HIS_ENTE_HD.NOMBRECONTRAPARTE,
		HIS_ENTE_HD.TIPOINSTRUMENTOCONTRAPARTE,
		HIS_ENTE_HD.CUENTACONTRAPARTE,
		NULL AS ENTE,
		NULL AS CANAL,
		NULL AS ENTE_D,
		CB.RE_OFCONTA
	FROM at_stg.at37_his_ente_hd_ AS HIS_ENTE_HD INNER JOIN at_stg.cb AS CB ON HIS_ENTE_HD.OFICINA = CB.RE_OFADMIN;'''
	hook.run(sql_query_deftxt)

def AT37_OTROS_SOURCE_STG_IP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_otrosbancos (
	tipotransferencia	NUMERIC,
	clasificacion	NUMERIC,
	tipocliente	VARCHAR(1),
	identificacioncliente	VARCHAR(15),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	NUMERIC,
	cuentacliente	CHAR(20),
	moneda	CHAR(3),
	monto	NUMERIC(15,2),
	fecha	CHAR(8),
	referencia	VARCHAR(20),
	empresaformacion	CHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	tipocontraparte	CHAR(3),
	identificacioncontraparte	VARCHAR(30),
	nombrecontraparte	VARCHAR(100),
	tipoinstrumentocontraparte	NUMERIC,
	cuentacontraparte	VARCHAR(20),
	ente	NUMERIC,
	canal	NUMERIC,
	ente_d	NUMERIC,
	oficina	NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_OTROSBANCOS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at37_otrosbancos (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	moneda,
	monto,
	fecha,
	referencia,
	empresaformacion,
	motivo,
	direccionip,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	ente,
	canal,
	ente_d,
	oficina
	)
	SELECT DISTINCT
		at37_otros_source1.tipotransferencia,
		at37_otros_source1.clasificacion,
		at37_otros_source1.tipocliente,
		at37_otros_source1.identificacioncliente,
		at37_otros_source1.nombrecliente,
		at37_otros_source1.tipoinstrumento,
		at37_otros_source1.cuentacliente,
		at37_otros_source1.moneda,
		at37_otros_source1.monto,
		at37_otros_source1.fecha,
		at37_otros_source1.referencia,
		at37_otros_source1.empresaformacion,
		at37_otros_source1.motivo,
		CASE
			WHEN at37_otros_source1.tipocliente IN ('J','R') THEN '192.168.11.30'
			WHEN (at37_otros_source1.tipotransferencia = 1 AND ip.ip IS NULL) THEN '192.168.11.30'
			WHEN (at37_otros_source1.tipocliente NOT IN ('J','R') AND ip.ip IS NOT NULL) THEN ip.ip
			ELSE '0'
		END AS direccionip,
		at37_otros_source1.tipocontraparte,
		at37_otros_source1.identificacioncontraparte,
		at37_otros_source1.nombrecontraparte,
		at37_otros_source1.tipoinstrumentocontraparte,
		at37_otros_source1.cuentacontraparte,
		'' AS ente,
		'' AS canal,
		'' AS ente_d,
		CASE
			WHEN (at37_otros_source1.oficina IS NULL AND SUBSTRING(at37_otros_source1.cuentacliente FROM 1 FOR 4) = '0114') THEN 150
			WHEN (at37_otros_source1.oficina >= 700 AND SUBSTRING(at37_otros_source1.cuentacliente FROM 1 FOR 4) = '0114') THEN 150
			WHEN (at37_otros_source1.oficina = 148 AND SUBSTRING(at37_otros_source1.cuentacliente FROM 1 FOR 4) = '0114') THEN 150
			ELSE at37_otros_source1.oficina
		END AS oficina 
	FROM at_stg.at37_otros_source1 AS at37_otros_source1 LEFT OUTER JOIN at_stg.ip AS ip
	ON (TRIM(ip.identificacioncliente) = TRIM(at37_otros_source1.identificacioncliente) AND TRIM(at37_otros_source1.fecha) = CONCAT('20', TO_CHAR(TO_DATE(ip.fecha, 'DD/MM/YYYY'), 'YYMMDD')));'''
	hook.run(sql_query_deftxt)

def AT37_TOTAL_IP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_otrosbancos_ (
	tipotransferencia	NUMERIC(1),
	clasificacion	NUMERIC(1),
	tipocliente	VARCHAR(1),
	identificacioncliente	VARCHAR(15),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	NUMERIC(1),
	cuentacliente	VARCHAR(20),
	moneda	VARCHAR(3),
	monto	NUMERIC(24,2),
	fecha	VARCHAR(8),
	referencia	VARCHAR(20),
	empresaformacion	VARCHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	tipocontraparte	VARCHAR(3),
	identificacioncontraparte	VARCHAR(20),
	nombrecontraparte	VARCHAR(100),
	tipoinstrumentocontraparte	NUMERIC(1),
	cuentacontraparte	VARCHAR(20),
	ente	NUMERIC(5),
	canal	NUMERIC(5),
	ente_d	NUMERIC(5),
	oficina	NUMERIC(8)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_OTROSBANCOS_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at37_otrosbancos_(
	tipotransferencia, 
	clasificacion, 
	tipocliente, 
	identificacioncliente, 
	nombrecliente, 
	tipoinstrumento, 
	cuentacliente, 
	moneda, 
	monto, 
	fecha, 
	referencia, 
	empresaformacion, 
	motivo, 
	direccionip, 
	tipocontraparte, 
	identificacioncontraparte, 
	nombrecontraparte, 
	tipoinstrumentocontraparte, 
	cuentacontraparte, 
	oficina, 
	ente, 
	canal, 
	ente_d
	) 
	SELECT 
	tipotransferencia, 
	clasificacion, 
	tipocliente, 
	identificacioncliente, 
	nombrecliente, 
	tipoinstrumento, 
	cuentacliente, 
	moneda, 
	monto, 
	fecha, 
	referencia, 
	empresaformacion, 
	motivo, 
	CASE 
		WHEN (tipotransferencia = 1 AND direccionip IS NULL AND identificacioncliente IN ('J','R')) THEN '192.168.11.30' 
		WHEN tipotransferencia = 2 THEN '0' 
		WHEN tipotransferencia = 3 AND direccionip IS NULL THEN '0' 
		ELSE direccionip 
	END, 
	tipocontraparte, 
	identificacioncontraparte, 
	nombrecontraparte, 
	tipoinstrumentocontraparte, 
	cuentacontraparte, 
	oficina, 
	'', 
	'', 
	'' 
	FROM at_stg.at37_otrosbancos;'''
	hook.run(sql_query_deftxt)

def AT37_OTROSBANCOS_ATSUDEBAN(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS atsudeban.at37_otrosbancos_ (
	fecha_ejecucion DATE,
	tipotransferencia NUMERIC,
	clasificacion NUMERIC,
	tipocliente VARCHAR(3),
	identificacioncliente VARCHAR(15),
	nombrecliente VARCHAR(250),
	tipoinstrumento VARCHAR(5),
	cuentacliente CHAR(20),
	moneda CHAR(3),
	monto NUMERIC(15,2),
	fecha CHAR(8),
	referencia VARCHAR(20),
	empresaformacion CHAR(3),
	motivo VARCHAR(100),
	direccionip VARCHAR(50),
	tipocontraparte CHAR(3),
	identificacioncontraparte VARCHAR(30),
	nombrecontraparte VARCHAR(100),
	tipoinstrumentocontraparte VARCHAR(5),
	cuentacontraparte VARCHAR(20),
	oficina NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_OTROSBANCOS_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_OTROSBANCOS_ (
	FECHA_EJECUCION,
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA
	)
	SELECT DISTINCT
		NULL AS FECHA_EJECUCION,
		AT37_OTROSBANCOS_.TIPOTRANSFERENCIA,
		AT37_OTROSBANCOS_.CLASIFICACION,
		AT37_OTROSBANCOS_.TIPOCLIENTE,
		AT37_OTROSBANCOS_.IDENTIFICACIONCLIENTE,
		AT37_OTROSBANCOS_.NOMBRECLIENTE,
		AT37_OTROSBANCOS_.TIPOINSTRUMENTO,
		AT37_OTROSBANCOS_.CUENTACLIENTE,
		AT37_OTROSBANCOS_.MONEDA,
		AT37_OTROSBANCOS_.MONTO,
		AT37_OTROSBANCOS_.FECHA,
		AT37_OTROSBANCOS_.REFERENCIA,
		AT37_OTROSBANCOS_.EMPRESAFORMACION,
		AT37_OTROSBANCOS_.MOTIVO,
		AT37_OTROSBANCOS_.DIRECCIONIP,
		AT37_OTROSBANCOS_.TIPOCONTRAPARTE,
		AT37_OTROSBANCOS_.IDENTIFICACIONCONTRAPARTE,
		AT37_OTROSBANCOS_.NOMBRECONTRAPARTE,
		AT37_OTROSBANCOS_.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_OTROSBANCOS_.CUENTACONTRAPARTE,
		AT37_OTROSBANCOS_.OFICINA
	FROM AT_STG.AT37_OTROSBANCOS_ AS AT37_OTROSBANCOS_;'''
	hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_OTROS37',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

AT37_Otros_cb_task = PythonOperator(
	task_id='AT37_Otros_cb_task',
	python_callable=AT37_Otros_cb,
	dag=dag
)

AT37_Otros_con_task = PythonOperator(
	task_id='AT37_Otros_con_task',
	python_callable=AT37_Otros_con,
	dag=dag
)

AT37_Otros_ccentes_task = PythonOperator(
	task_id='AT37_Otros_ccentes_task',
	python_callable=AT37_Otros_ccentes,
	dag=dag
)

AT37_Otros_pre_task = PythonOperator(
	task_id='AT37_Otros_pre_task',
	python_callable=AT37_Otros_pre,
	dag=dag
)

AT37_Otros_pre_arch_task = PythonOperator(
	task_id='AT37_Otros_pre_arch_task',
	python_callable=AT37_Otros_pre_arch,
	dag=dag
)

AT37_Otros_pre_ope_task = PythonOperator(
	task_id='AT37_Otros_pre_ope_task',
	python_callable=AT37_Otros_pre_ope,
	dag=dag
)

AT37_Otros_Union_task = PythonOperator(
	task_id='AT37_Otros_Union_task',
	python_callable=AT37_Otros_Union,
	dag=dag
)

AT37_OTROS_UNION__task = PythonOperator(
	task_id='AT37_OTROS_UNION__task',
	python_callable=AT37_OTROS_UNION_,
	dag=dag
)

AT37_CONVERSION_task = PythonOperator(
	task_id='AT37_CONVERSION_task',
	python_callable=AT37_CONVERSION,
	dag=dag
)

AT37_OTRO_PRE_CC_task = PythonOperator(
	task_id='AT37_OTRO_PRE_CC_task',
	python_callable=AT37_OTRO_PRE_CC,
	dag=dag
)

AT37_OTRO_PRE_CC_D_task = PythonOperator(
	task_id='AT37_OTRO_PRE_CC_D_task',
	python_callable=AT37_OTRO_PRE_CC_D,
	dag=dag
)

AT37_HIS_ENTE_HD__task = PythonOperator(
	task_id='AT37_HIS_ENTE_HD__task',
	python_callable=AT37_HIS_ENTE_HD_,
	dag=dag
)

AT37_OTROS_SOURCE_task = PythonOperator(
	task_id='AT37_OTROS_SOURCE_task',
	python_callable=AT37_OTROS_SOURCE,
	dag=dag
)

AT37_OTROS_SOURCE_STG_IP_task = PythonOperator(
	task_id='AT37_OTROS_SOURCE_STG_IP_task',
	python_callable=AT37_OTROS_SOURCE_STG_IP,
	dag=dag
)

AT37_TOTAL_IP_task = PythonOperator(
	task_id='AT37_TOTAL_IP_task',
	python_callable=AT37_TOTAL_IP,
	dag=dag
)

AT37_OTROSBANCOS_ATSUDEBAN_task = PythonOperator(
	task_id='AT37_OTROSBANCOS_ATSUDEBAN_task',
	python_callable=AT37_OTROSBANCOS_ATSUDEBAN,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_Otros_cb_task >> AT37_Otros_con_task >> AT37_Otros_ccentes_task >> AT37_Otros_pre_task >> AT37_Otros_pre_arch_task >> AT37_Otros_pre_ope_task >> AT37_Otros_Union_task >> AT37_OTROS_UNION__task >> AT37_CONVERSION_task >> AT37_OTRO_PRE_CC_task >> AT37_OTRO_PRE_CC_D_task >> AT37_HIS_ENTE_HD__task >> AT37_OTROS_SOURCE_task >> AT37_OTROS_SOURCE_STG_IP_task >> AT37_TOTAL_IP_task >> AT37_OTROSBANCOS_ATSUDEBAN_task
