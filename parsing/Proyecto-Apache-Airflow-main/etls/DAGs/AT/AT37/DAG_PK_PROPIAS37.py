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

def AT37_Propias_bvente(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.bv_ente (
	en_ente NUMERIC(10),
	en_oficina NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.BV_ENTE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.bv_ente (
	en_ente,
	en_oficina
	)
	SELECT 
		bv_ente.en_ente,
		bv_ente.en_oficina
	FROM ods.bv_ente
	WHERE (bv_ente.en_oficina <> 148);'''
	hook.run(sql_query_deftxt)

def AT37_Propias_bvlog(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')
	at37_montobs = get_variable('AT37_MONTOBS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.bv_log (
	lo_producto	NUMERIC(3),
	lo_cuenta	VARCHAR(32),
	lo_monto	NUMERIC(15,2),
	lo_fecha	DATE,
	lo_secuencial	NUMERIC(10),
	lo_producto_dest	NUMERIC(3),
	lo_cuenta_dest	VARCHAR(32),
	lo_servicio	NUMERIC(3),
	lo_ente	NUMERIC(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.BV_LOG;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.bv_log (
	lo_producto,
	lo_cuenta,
	lo_monto,
	lo_fecha,
	lo_secuencial,
	lo_producto_dest,
	lo_cuenta_dest,
	lo_servicio,
	lo_ente
	)
	SELECT DISTINCT
		BV_HIS_LOG.HL_PRODUCTO,
		BV_HIS_LOG.HL_CUENTA,
		BV_HIS_LOG.HL_MONTO,
		BV_HIS_LOG.HL_FECHA,
		BV_HIS_LOG.HL_SECUENCIAL,
		BV_HIS_LOG.HL_PRODUCTO_DEST,
		BV_HIS_LOG.HL_CUENTA_DEST,
		BV_HIS_LOG.HL_SERVICIO,
		BV_HIS_LOG.HL_ENTE
	FROM ODS.BV_HIS_LOG AS BV_HIS_LOG
	WHERE BV_HIS_LOG.HL_SERVICIO IN (1,2,9)
	AND BV_HIS_LOG.HL_PRODUCTO IN (3,4)
	AND BV_HIS_LOG.HL_STATUS = 'EJ'
	AND BV_HIS_LOG.HL_TRANSACCION IN ('18056','18075','18024')
	AND BV_HIS_LOG.HL_MONTO >= CAST({at37_montobs} AS NUMERIC)
	AND BV_HIS_LOG.HL_FECHA BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY')
	AND SUBSTRING(TRIM(BV_HIS_LOG.HL_CUENTA) FROM 1 FOR 4) = SUBSTRING(TRIM(BV_HIS_LOG.HL_CUENTA_DEST) FROM 1 FOR 4);'''
	hook.run(sql_query_deftxt)

def AT37_Propias_cb(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.r_ofconta (
	re_ofconta NUMERIC(8),
	re_ofadmin NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.R_OFCONTA;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.r_ofconta (
	re_ofconta, 
	re_ofadmin
	)
	SELECT
		cb_relofi.re_ofconta,
		cb_relofi.re_ofadmin
	FROM ods.cb_relofi AS cb_relofi
	WHERE (cb_relofi.re_ofadmin <> 148) AND (cb_relofi.re_ofconta <> 148) AND (cb_relofi.re_empresa = 1) AND (cb_relofi.re_filial = 1);'''
	hook.run(sql_query_deftxt)

def AT37_Propias_Retran(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.re_tran (
	tm_cta_banco	CHAR(24),
	tm_cta_banco_d	CHAR(24),
	tm_valor	NUMERIC(15,2),
	tm_fecha	DATE,
	tm_concepto	VARCHAR(128),
	tm_tipo_tran	NUMERIC(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.RE_TRAN;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.RE_TRAN (
	TM_CTA_BANCO, 
	TM_CTA_BANCO_D, 
	TM_VALOR, 
	TM_FECHA, 
	TM_CONCEPTO, 
	TM_TIPO_TRAN
	)
	SELECT
		TRIM(RE_TRAN_MONET.TM_CTA_BANCO),
		TRIM(RE_TRAN_MONET.TM_CTA_BANCO_D),
		RE_TRAN_MONET.TM_VALOR,
		RE_TRAN_MONET.TM_FECHA,
		RE_TRAN_MONET.TM_CONCEPTO,
		RE_TRAN_MONET.TM_TIPO_TRAN
	FROM ODS.RE_TRAN_MONET AS RE_TRAN_MONET
	WHERE (RE_TRAN_MONET.TM_PRODUCTO_D IN (3,4)) AND (RE_TRAN_MONET.TM_TIPO_TRAN IN ('18056','18075','18024')) AND (RE_TRAN_MONET.TM_ESTADO_EJECUCION = 'EJ')
	AND (RE_TRAN_MONET.TM_CANAL = 1) AND (TRIM(RE_TRAN_MONET.TM_CONCEPTO) IS NOT NULL)
	AND (RE_TRAN_MONET.TM_FECHA BETWEEN TO_DATE('{FechainicioS}','MM/DD/YY') AND TO_DATE('{FechafinS}','MM/DD/YY'));'''
	hook.run(sql_query_deftxt)

def AT37_Propias_ccentes(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.ccente37_propias (
	en_ced_ruc	VARCHAR(40),
	cuenta_cliente	CHAR(24),
	cliente	VARCHAR(50),
	en_nombre	VARCHAR(255),
	en_nomlar	VARCHAR(255),
	p_pasaporte	VARCHAR(50),
	tipo_cuenta	VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.CCENTE37_PROPIAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.ccente37_propias (
	en_ced_ruc,
	cuenta_cliente,
	cliente,
	en_nombre,
	en_nomlar,
	p_pasaporte,
	tipo_cuenta
	)
	SELECT
		CL_ENTE.EN_CED_RUC,
		CC_CTACTE_AT.CC_CTA_BANCO,
		CC_CTACTE_AT.CC_CLIENTE,
		CL_ENTE.EN_NOMBRE,
		CL_ENTE.EN_NOMLAR,
		CL_ENTE.P_PASAPORTE,
		'2' AS tipo_cuenta
	FROM ODS.CL_ENTE AS CL_ENTE INNER JOIN ODS.CC_CTACTE_AT AS CC_CTACTE_AT ON (CL_ENTE.EN_ENTE = CC_CTACTE_AT.CC_CLIENTE)
	WHERE (CC_CTACTE_AT.CC_FECHA_CARGA_AT IN (SELECT MAX(CC_FECHA_CARGA_AT) FROM ODS.CC_CTACTE_AT))

	UNION

	SELECT
		CL_ENTE.EN_CED_RUC,
		AH_CUENTA_AT.AH_CTA_BANCO,
		AH_CUENTA_AT.AH_CLIENTE,
		CL_ENTE.EN_NOMBRE,
		CL_ENTE.EN_NOMLA,
		CL_ENTE.P_PASAPORTE,
		'1' AS tipo_cuenta
	FROM ODS.CL_ENTE AS CL_ENTE INNER JOIN ODS.AH_CUENTA_AT AS AH_CUENTA_AT ON (CL_ENTE.EN_ENTE = AH_CUENTA_AT.AH_CLIENTE)
	WHERE (AH_CUENTA_AT.AH_FECHA_CARGA_AT IN (SELECT MAX(AH_FECHA_CARGA_AT) FROM ODS.AH_CUENTA_AT));'''
	hook.run(sql_query_deftxt)

def AT37_Propias_ccbv_d(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.bv_ccente_d (
	lo_cuenta	VARCHAR(32),
	lo_cuenta_dest	VARCHAR(32),
	en_ced_ruc	VARCHAR(40),
	en_nombre	VARCHAR(255),
	cliente	VARCHAR(50),
	cuenta	VARCHAR(24),
	tipo_cuenta	VARCHAR(1),
	p_pasaporte	VARCHAR(50)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.BV_CCENTE_D;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.bv_ccente_d (
	LO_CUENTA,
	LO_CUENTA_DEST,
	EN_CED_RUC,
	EN_NOMBRE,
	CLIENTE,
	CUENTA,
	TIPO_CUENTA,
	P_PASAPORTE
	)
	SELECT DISTINCT
	BV_LOG.LO_CUENTA AS LO_CUENTA,
	BV_LOG.LO_CUENTA_DEST AS LO_CUENTA_DEST,
	CCENTE37_PROPIAS.EN_CED_RUC AS EN_CED_RUC,
	CCENTE37_PROPIAS.EN_NOMLAR AS EN_NOMBRE,
	CCENTE37_PROPIAS.CLIENTE AS CLIENTE,
	CCENTE37_PROPIAS.CUENTA_CLIENTE AS CUENTA,
	CCENTE37_PROPIAS.TIPO_CUENTA AS TIPO_CUENTA,
	CCENTE37_PROPIAS.P_PASAPORTE AS P_PASAPORTE
	FROM at_stg.bv_log AS BV_LOG INNER JOIN at_stg.ccente37_propias AS CCENTE37_PROPIAS ON (TRIM(BV_LOG.LO_CUENTA_DEST) = TRIM(CCENTE37_PROPIAS.CUENTA_CLIENTE));'''
	hook.run(sql_query_deftxt)

def AT37_Propias_ccbv(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.bv_ccente (
	lo_producto	NUMERIC(4),
	lo_cuenta	VARCHAR(32),
	lo_monto	NUMERIC(16,2),
	lo_fecha	DATE,
	lo_secuencial	NUMERIC(11),
	lo_producto_dest	NUMERIC(4),
	lo_cuenta_dest	VARCHAR(32),
	lo_servicio	NUMERIC(4),
	en_ced_ruc	VARCHAR(40),
	en_nombre	VARCHAR(255),
	cliente	VARCHAR(50),
	cuenta	VARCHAR(24),
	tipo_cuenta	VARCHAR(1),
	lo_ente	NUMERIC(10),
	p_pasaporte	VARCHAR(50)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.BV_CCENTE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.bv_ccente (
	lo_producto,
	lo_cuenta,
	lo_monto,
	lo_fecha,
	lo_secuencial,
	lo_producto_dest,
	lo_cuenta_dest,
	lo_servicio,
	en_ced_ruc,
	en_nombre,
	cliente,
	cuenta,
	tipo_cuenta,
	lo_ente,
	p_pasaporte
	)
	SELECT 
		bv_log.lo_producto,
		bv_log.lo_cuenta,
		bv_log.lo_monto,
		bv_log.lo_fecha,
		bv_log.lo_secuencial,
		bv_log.lo_producto_dest,
		bv_log.lo_cuenta_dest,
		bv_log.lo_servicio,
		ccente37_propias.en_ced_ruc,
		ccente37_propias.en_nomlar,
		ccente37_propias.cliente,
		ccente37_propias.cuenta_cliente,
		ccente37_propias.tipo_cuenta,
		bv_log.lo_ente,
		ccente37_propias.p_pasaporte
	FROM at_stg.bv_log AS bv_log INNER JOIN at_stg.ccente37_propias AS ccente37_propias ON TRIM(bv_log.lo_cuenta) = TRIM(ccente37_propias.cuenta_cliente);'''
	hook.run(sql_query_deftxt)

def AT37_Propias_stg1(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	Moneda_Vzla = get_variable('Moneda_Vzla')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.PROPIAS_STG1 (
	TIPOTRANSFERENCIA	NUMERIC(1),
	CLASIFICACION	NUMERIC(1),
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	NUMERIC(1),
	CUENTACLIENTE	CHAR(20),
	MODEDA	CHAR(3),
	MONTO	NUMERIC(20,2),
	FECHA	CHAR(8),
	REFERENCIA	VARCHAR(20),
	EMPRESAFORMACION	CHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP VARCHAR(50)
	TIPOCONTRAPARTE	CHAR(1),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(30),
	NOMBRECONTRAPARTE	CHARACTER VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	NUMERIC(5),
	CUENTACONTRAPARTE	VARCHAR(24),
	ENTE	NUMERIC(5),
	CANAL	NUMERIC(5),
	ENTE_D	NUMERIC(5),
	OFICINA	NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.PROPIAS_STG1;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.propias_stg1 (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	modeda,
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
	SELECT
		1 AS tipotransferencia,
		1 AS clasificacion,
		CASE
			WHEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1) IN ('M', 'F') THEN 'V'
			WHEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1) IN ('E', 'G', 'I', 'J', 'P', 'R', 'V', 'C') THEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1)
			ELSE 'P'
		END AS tipocliente,
		CASE
			WHEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1) IN ('M') THEN SUBSTRING(bv_ccente.en_ced_ruc FROM 3 FOR LENGHT(bv_ccente.en_ced_ruc))
			WHEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1) IN ('E', 'G', 'I', 'J', 'R', 'V', 'F', 'P', 'C') THEN SUBSTRING(bv_ccente.en_ced_ruc FROM 2 FOR LENGHT(bv_ccente.en_ced_ruc))
			ELSE bv_ccente.p_pasaporte
		END AS identificacioncliente,
		bv_ccente.en_nombre AS nombrecliente,
		CASE
			WHEN bv_ccente.lo_producto = 3 THEN 2
			WHEN bv_ccente.lo_producto = 4 THEN 1
		END AS tipoinstrumento,
		TRIM(bv_ccente.lo_cuenta) AS cuentacliente,
		'{Moneda_Vzla}' AS modeda,
		bv_ccente.lo_monto AS monto,
		'20' || TO_CHAR(TO_DATE(bv_ccente.lo_fecha, 'DD/MM/YYYY'), 'YYMMDD') AS fecha,
		bv_ccente.lo_secuencial AS referencia,
		CASE
			WHEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1) IN ('R', 'F') THEN '1'
			WHEN SUBSTRING(bv_ccente.en_ced_ruc FROM 1 FOR 1) = 'M' THEN '2'
			ELSE '0'
		END AS empresaformacion,
		CASE
			WHEN RE_TRAN.TM_CONCEPTO IS NULL THEN 'NO DISPONIBLE'
			ELSE RE_TRAN.TM_CONCEPTO
		END AS motivo,
		NULL AS direccionip,
		CASE
			WHEN SUBSTRING(bv_ccente_d.en_ced_ruc FROM 1 FOR 1) = 'M' THEN 'V'
			WHEN bv_ccente_d.en_ced_ruc IS NULL THEN 'P'
			ELSE SUBSTRING(bv_ccente_d.en_ced_ruc FROM 1 FOR 1)
		END AS tipocontraparte,
		CASE
			WHEN SUBSTRING(bv_ccente_d.en_ced_ruc, FROM 1 FOR 1) = 'M' THEN SUBSTRING(bv_ccente_d.en_ced_ruc FROM 3 FOR length(bv_ccente_d.en_ced_ruc) - 1)
			WHEN substring(bv_ccente_d.en_ced_ruc FROM 2 FOR length(bv_ccente_d.en_ced_ruc) - 1) IS NULL THEN bv_ccente_d.p_pasaporte
			ELSE SUBSTRING(bv_ccente_d.en_ced_ruc FROM 2 FOR length(bv_ccente_d.en_ced_ruc) - 1)
		END AS identificacioncontraparte,
		bv_ccente_d.en_nombre AS nombrecontraparte,
		bv_ccente_d.tipo_cuenta AS tipoinstrumentocontraparte,
		bv_ccente_d.cuenta AS cuentacontraparte,
		0 AS ente,
		0 AS canal,
		0 AS ente_d,
		CAST(SUBSTRING(bv_ccente.lo_cuenta FROM 5 FOR 4) AS NUMERIC) AS oficina
	FROM at_stg.bv_ccente AS bv_ccente INNER JOIN at_stg.bv_ente AS bv_ente ON bv_ccente.lo_ente = bv_ente.en_ente
		INNER JOIN at_stg.bv_ccente_d AS bv_ccente_d ON bv_ccente.lo_cuenta_dest = bv_ccente_d.lo_cuenta_dest
		LEFT JOIN at_stg.re_tran AS re_tran ON bv_ccente.lo_cuenta = re_tran.tm_cta_banco AND bv_ccente.lo_cuenta_dest = re_tran.tm_cta_banco_d
		AND bv_ccente.lo_fecha = re_tran.tm_fecha AND bv_ccente.lo_monto = re_tran.tm_valor;'''
	hook.run(sql_query_deftxt)

def AT37_PROPIAS_STG_IP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_propias (
	tipotransferencia	NUMERIC,
	clasificacion	NUMERIC,
	tipocliente	VARCHAR(1),
	identificacioncliente	VARCHAR(15),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	NUMERIC,
	cuentacliente	CHAR(24),
	moneda	CHAR(3),
	monto	NUMERIC,
	fecha	CHAR(8),
	referencia	VARCHAR(20),
	empresaformacion	CHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	tipocontraparte	CHAR(3),
	identificacioncontraparte	VARCHAR(30),
	nombrecontraparte	VARCHAR(100),
	tipoinstrumentocontraparte	NUMERIC,
	cuentacontraparte	VARCHAR(24),
	ente	NUMERIC,
	canal	NUMERIC,
	ente_d	NUMERIC,
	oficina	NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_PROPIAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at37_propias (
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
		propias_stg1.tipotransferencia,
		propias_stg1.clasificacion,
		propias_stg1.tipocliente,
		propias_stg1.identificacioncliente,
		propias_stg1.nombrecliente,
		propias_stg1.tipoinstrumento,
		propias_stg1.cuentacliente,
		propias_stg1.modeda,
		propias_stg1.monto,
		propias_stg1.fecha,
		propias_stg1.referencia,
		propias_stg1.empresaformacion,
		propias_stg1.motivo,
		CASE
			WHEN SUBSTRING(propias_stg1.tipocliente FROM 1 FOR 1) IN ('J','R','G') THEN '192.168.11.30'
			ELSE ip.ip
		END,
		propias_stg1.tipocontraparte,
		propias_stg1.identificacioncontraparte,
		propias_stg1.nombrecontraparte,
		propias_stg1.tipoinstrumentocontraparte,
		TRIM(propias_stg1.cuentacontraparte),
		propias_stg1.ente,
		propias_stg1.canal,
		propias_stg1.ente_d,
		CASE
			WHEN (propias_stg1.oficina IS NULL AND SUBSTRING(propias_stg1.cuentacliente FROM 1 FOR 4) = '0114') THEN 150
			WHEN (propias_stg1.oficina >= 700 AND SUBSTRING(propias_stg1.cuentacliente FROM 1 FOR 4) = '0114') THEN 150
			WHEN (propias_stg1.oficina = 148 AND SUBSTRING(propias_stg1.cuentacliente FROM 1 FOR 4) = '0114') THEN 150
			ELSE r_ofconta.re_ofconta
		END
	FROM at_stg.propias_stg1 AS propias_stg1 LEFT JOIN at_stg.ip AS ip
		ON (TRIM(propias_stg1.identificacioncliente)=TRIM(ip.identificacioncliente) AND TRIM(propias_stg1.fecha)=CONCAT('20', to_char(to_date(ip.fecha, 'DD/MM/YYYY'),'YYMMDD')))
	INNER JOIN at_stg.r_ofconta AS r_ofconta ON (propias_stg1.oficina=r_ofconta.re_ofadmin);'''
	hook.run(sql_query_deftxt)

def AT37_TOTALP_IP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT37_PROPIASF (
	TIPOTRANSFERENCIA	NUMERIC,
	CLASIFICACION	NUMERIC,
	TIPOCLIENTE	VARCHAR(1),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	NUMERIC,
	CUENTACLIENTE	VARCHAR(24),
	MONEDA	VARCHAR(3),
	MONTO	NUMERIC(20,2),
	FECHA	VARCHAR(8),
	REFERENCIA	VARCHAR(20),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP	VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(30),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	NUMERIC,
	CUENTACONTRAPARTE	VARCHAR(24),
	ENTE	NUMERIC,
	CANAL	NUMERIC,
	ENTE_D	NUMERIC,
	OFICINA	NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_PROPIASF;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at37_propiasf (
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
		at37_propias.tipotransferencia,
		at37_propias.clasificacion,
		at37_propias.tipocliente,
		at37_propias.identificacioncliente,
		at37_propias.nombrecliente,
		at37_propias.tipoinstrumento,
		at37_propias.cuentacliente,
		at37_propias.moneda,
		at37_propias.monto,
		at37_propias.fecha,
		at37_propias.referencia,
		at37_propias.empresaformacion,
		at37_propias.motivo,
		CASE
			WHEN at37_propias.tipotransferencia = 1 AND at37_propias.direccionip IS NULL THEN '192.168.11.30'
			WHEN at37_propias.tipotransferencia = 2 AND at37_propias.direccionip IS NULL THEN '0'
			ELSE at37_propias.direccionip
		END,
		at37_propias.tipocontraparte,
		at37_propias.identificacioncontraparte,
		at37_propias.nombrecontraparte,
		at37_propias.tipoinstrumentocontraparte,
		at37_propias.cuentacontraparte,
		at37_propias.ente,
		at37_propias.canal,
		at37_propias.ente_d,
		at37_propias.oficina
	FROM at_stg.at37_propias AS at37_propias;'''
	hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PK_PROPIAS37',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

AT37_Propias_bvente_task = PythonOperator(
	task_id='AT37_Propias_bvente_task',
	python_callable=AT37_Propias_bvente,
	dag=dag
)

AT37_Propias_bvlog_task = PythonOperator(
	task_id='AT37_Propias_bvlog_task',
	python_callable=AT37_Propias_bvlog,
	dag=dag
)

AT37_Propias_cb_task = PythonOperator(
	task_id='AT37_Propias_cb_task',
	python_callable=AT37_Propias_cb,
	dag=dag
)

AT37_Propias_Retran_task = PythonOperator(
	task_id='AT37_Propias_Retran_task',
	python_callable=AT37_Propias_Retran,
	dag=dag
)

AT37_Propias_ccentes_task = PythonOperator(
	task_id='AT37_Propias_ccentes_task',
	python_callable=AT37_Propias_ccentes,
	dag=dag
)

AT37_Propias_ccbv_d_task = PythonOperator(
	task_id='AT37_Propias_ccbv_d_task',
	python_callable=AT37_Propias_ccbv_d,
	dag=dag
)

AT37_Propias_ccbv_task = PythonOperator(
	task_id='AT37_Propias_ccbv_task',
	python_callable=AT37_Propias_ccbv,
	dag=dag
)

AT37_Propias_stg1_task = PythonOperator(
	task_id='AT37_Propias_stg1_task',
	python_callable=AT37_Propias_stg1,
	dag=dag
)

AT37_PROPIAS_STG_IP_task = PythonOperator(
	task_id='AT37_PROPIAS_STG_IP_task',
	python_callable=AT37_PROPIAS_STG_IP,
	dag=dag
)

AT37_TOTALP_IP_task = PythonOperator(
	task_id='AT37_TOTALP_IP_task',
	python_callable=AT37_TOTALP_IP,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_Propias_bvente_task >> AT37_Propias_bvlog_task >> AT37_Propias_cb_task >> AT37_Propias_Retran_task >> AT37_Propias_ccentes_task >> AT37_Propias_ccbv_d_task >> AT37_Propias_ccbv_task >> AT37_Propias_stg1_task >> AT37_PROPIAS_STG_IP_task >> AT37_TOTALP_IP_task
