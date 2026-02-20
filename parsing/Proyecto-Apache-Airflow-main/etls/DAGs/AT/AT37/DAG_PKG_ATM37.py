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

def AT37_bvente(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.bv_servicio (
	ep_servicio NUMERIC(3),
	ep_cuenta VARCHAR(30)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.BV_SERVICIO;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.bv_servicio (
	EP_SERVICIO, 
	EP_CUENTA
	)
	SELECT
		BV_ENTE_SERVICIO_PRODUCTO.EP_SERVICIO,
		BV_ENTE_SERVICIO_PRODUCTO.EP_CUENTA
	FROM ODS.BV_ENTE_SERVICIO_PRODUCTO AS BV_ENTE_SERVICIO_PRODUCTO
	WHERE (BV_ENTE_SERVICIO_PRODUCTO.EP_SERVICIO = 6);'''
	hook.run(sql_query_deftxt)

def AT37_ccente(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.CCENTE37_ATM (
	EN_CED_RUC VARCHAR(40),
	CUENTA_CLIENTE CHAR(24),
	CLIENTE VARCHAR(50),
	EN_NOMBRE VARCHAR(255),
	EN_NOMLAR VARCHAR(255),
	P_PASAPORTE VARCHAR(50),
	EN_ACTIVIDAD VARCHAR(50),
	TIPO_CUENTA VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.CCENTE37_ATM;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.CCENTE37_ATM (
	EN_CED_RUC, 
	CUENTA_CLIENTE, 
	CLIENTE, 
	EN_NOMBRE, 
	EN_NOMLAR, 
	P_PASAPORTE, 
	EN_ACTIVIDAD, 
	TIPO_CUENTA
	)  
	SELECT
		CL_ENTE.EN_CED_RUC,
		CC_CTACTE_AT.CC_CTA_BANCO,
		CC_CTACTE_AT.CC_CLIENTE,
		CL_ENTE.EN_NOMBRE,
		CL_ENTE.EN_NOMLAR,
		CL_ENTE.P_PASAPORTE,
		CL_ENTE.EN_ACTIVIDAD,
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
		CL_ENTE.EN_ACTIVIDAD,
		'1' AS TIPO_CUENTA
	FROM
		ODS.CL_ENTE AS CL_ENTE INNER JOIN ODS.AH_CUENTA_AT AS AH_CUENTA_AT ON (CL_ENTE.EN_ENTE = AH_CUENTA_AT.AH_CLIENTE)
	WHERE (AH_CUENTA_AT.AH_FECHA_CARGA_AT IN (SELECT MAX(AH_FECHA_CARGA_AT) FROM ODS.AH_CUENTA_AT));'''
	hook.run(sql_query_deftxt)

def AT37_retran(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechainicioS = get_variable('FechainicioS')
	FechafinS = get_variable('FechafinS')
	Montobs = get_variable('AT37_MONTOBS')

	# CREAMOS TABAL DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.retran_his (
	th_ssn_local	NUMERIC(10),
	th_cta_banco	CHAR(24),
	th_cta_banco_d	VARCHAR(24),
	th_fecha	VARCHAR(8),
	th_valor	NUMERIC(15),
	th_oficina	NUMERIC(5),
	th_tipo_tran	NUMERIC(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.RETRAN_HIS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.retran_his (
	th_ssn_local,
	th_cta_banco,
	th_cta_banco_d,
	th_fecha,
	th_valor,
	th_oficina,
	th_tipo_tran
	)
	SELECT
		RE_TRAN_MONET_HIS.TH_SSN_LOCAL,
		RE_TRAN_MONET_HIS.TH_CTA_BANCO,
		RE_TRAN_MONET_HIS.TH_CTA_BANCO_D,
		TO_CHAR(RE_TRAN_MONET_HIS.TH_FECHA, 'YYYYMMDD'),
		RE_TRAN_MONET_HIS.TH_VALOR,
		RE_TRAN_MONET_HIS.TH_OFICINA,
		RE_TRAN_MONET_HIS.TH_TIPO_TRAN
	FROM ODS.RE_TRAN_MONET_HIS AS RE_TRAN_MONET_HIS
	WHERE (RE_TRAN_MONET_HIS.TH_VALOR >= CAST('{Montobs}' AS NUMERIC))
		AND (RE_TRAN_MONET_HIS.TH_FECHA BETWEEN TO_DATE('{FechainicioS}','MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY'))
		AND (RE_TRAN_MONET_HIS.TH_PRODUCTO IN ('3', '4'))
		AND (LENGTH(TRIM(RE_TRAN_MONET_HIS.TH_CTA_BANCO)) > 1)
		AND (RE_TRAN_MONET_HIS.TH_ESTADO_EJECUCION IN ('EJ', 'EE'));'''
	hook.run(sql_query_deftxt)

def AT37_trx(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.TT_TRX (
	TT_TRN_COBIS VARCHAR(8),
	TT_TRN_NOMBRE VARCHAR(64),
	TT_TIPO_TRN CHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE LS_AT_STG.TT_TRX;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.tt_trx (
	tt_trn_cobis, 
	tt_trn_nombre, 
	tt_tipo_trn
	)
	SELECT
		TM_TTRANSACCION.TT_TRN_COBIS,
		TM_TTRANSACCION.TT_TRN_NOMBRE,
		TM_TTRANSACCION.TT_TIPO_TRN
	FROM ODS.TM_TTRANSACCION AS TM_TTRANSACCION
	WHERE (TM_TTRANSACCION.TT_TIPO_TRN = 'T');'''
	hook.run(sql_query_deftxt)

def AT37_ATM_SOURCE_STG(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.tt_ttran_monet (
	tt_trn_nombre	VARCHAR(64),
	th_ssn_local	INTEGER,
	th_cta_banco	CHAR(24),
	th_cta_banco_d	VARCHAR(24),
	th_fecha	VARCHAR(8),
	th_valor	NUMERIC,
	th_oficina	INTEGER,
	th_tipo_tran	INTEGER
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.TT_TTRAN_MONET;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.tt_ttran_monet (
	tt_trn_nombre,
	th_ssn_local,
	th_cta_banco,
	th_cta_banco_d,
	th_fecha,
	th_valor,
	th_oficina,
	th_tipo_tran
	)
	SELECT DISTINCT
		tt_trx.tt_trn_nombre,
		retran_his.th_ssn_local,
		retran_his.th_cta_banco,
		retran_his.th_cta_banco_d,
		retran_his.th_fecha,
		retran_his.th_valor,
		retran_his.th_oficina,
		retran_his.th_tipo_tran
	FROM at_stg.tt_trx AS tt_trx INNER JOIN at_stg.retran_his AS retran_his ON trim(tt_trx.tt_trn_cobis) = trim(retran_his.th_tipo_tran)
		INNER JOIN at_stg.bv_servicio AS bv_servicio ON trim(retran_his.th_cta_banco) = trim(bv_servicio.ep_cuenta);'''
	hook.run(sql_query_deftxt)

def AT37_ATM_temp_Client_Contra(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.ATS (
	CLIENTE VARCHAR(50),
	TH_SSN_LOCAL INTEGER,
	TH_CTA_BANCO CHAR(24),
	TH_CTA_BANCO_D VARCHAR(32),
	TH_FECHA VARCHAR(8),
	TH_VALOR DECIMAL(15),
	TH_OFICINA INTEGER,
	TH_TIPO_TRAN INTEGER,
	TT_TRN_NOMBRE VARCHAR(64),
	EN_CED_RUC VARCHAR(40),
	EN_NOMLAR VARCHAR(255),
	P_PASAPORTE VARCHAR(50),
	CUENTA_CLIENTE VARCHAR(50),
	TIPO_CUENTA VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.ATS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.ATS (
	CLIENTE, 
	TH_SSN_LOCAL, 
	TH_CTA_BANCO, 
	TH_CTA_BANCO_D, 
	TH_FECHA, 
	TH_VALOR, 
	TH_OFICINA, 
	TH_TIPO_TRAN, 
	TT_TRN_NOMBRE, 
	EN_CED_RUC, 
	EN_NOMLAR, 
	P_PASAPORTE, 
	CUENTA_CLIENTE, 
	TIPO_CUENTA
	) 
	SELECT DISTINCT 
		CCENTE37_ATM.CLIENTE AS CLIENTE,
		TT_TTRAN_MONET.TH_SSN_LOCAL AS TH_SSN_LOCAL,
		TT_TTRAN_MONET.TH_CTA_BANCO AS TH_CTA_BANCO,
		TT_TTRAN_MONET.TH_CTA_BANCO_D AS TH_CTA_BANCO_D,
		TT_TTRAN_MONET.TH_FECHA AS TH_FECHA,
		TT_TTRAN_MONET.TH_VALOR AS TH_VALOR,
		TT_TTRAN_MONET.TH_OFICINA AS TH_OFICINA,
		TT_TTRAN_MONET.TH_TIPO_TRAN AS TH_TIPO_TRAN,
		TT_TTRAN_MONET.TT_TRN_NOMBRE AS TT_TRN_NOMBRE,
		CCENTE37_ATM.EN_CED_RUC AS EN_CED_RUC,
		CCENTE37_ATM.EN_NOMLAR AS EN_NOMLAR,
		CCENTE37_ATM.P_PASAPORTE AS P_PASAPORTE,
		CCENTE37_ATM.CUENTA_CLIENTE AS CUENTA_CLIENTE,
		CCENTE37_ATM.TIPO_CUENTA AS TIPO_CUENTA
	FROM at_stg.tt_ttran_monet AS TT_TTRAN_MONET INNER JOIN at_stg.ccente37_atm AS CCENTE37_ATM
		ON (TRIM(TT_TTRAN_MONET.TH_CTA_BANCO) = TRIM(CCENTE37_ATM.CUENTA_CLIENTE));'''
	hook.run(sql_query_deftxt)

def AT37_ATM_temp_Client_Contra_d(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.ATS_D (
	EN_CED_RUC	VARCHAR(40),
	EN_NOMLAR	VARCHAR(255),
	P_PASAPORTE	VARCHAR(50),
	CLIENTE	VARCHAR(50),
	CUENTA_CLIENTE	VARCHAR(50),
	TH_CTA_BANCO_D	VARCHAR(50),
	TIPO_CUENTA	VARCHAR(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.ATS_D;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.ats_d (
	en_ced_ruc,
	en_nomlar,
	p_pasaporte,
	cliente,
	cuenta_cliente,
	th_cta_banco_d,
	tipo_cuenta
	)
	SELECT DISTINCT
		ccente37_atm.en_ced_ruc,
		ccente37_atm.en_nomlar,
		ccente37_atm.p_pasaporte,
		ccente37_atm.cliente,
		ccente37_atm.cuenta_cliente,
		tt_ttran_monet.th_cta_banco_d,
		ccente37_atm.tipo_cuenta
	FROM at_stg.tt_ttran_monet AS tt_ttran_monet INNER JOIN at_stg.ccente37_atm AS ccente37_atm
		ON TRIM(tt_ttran_monet.th_cta_banco_d) = TRIM(ccente37_atm.cuenta_cliente);'''
	hook.run(sql_query_deftxt)

def AT_37_ATM(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	Moneda_Vzla = get_variable('Moneda_Vzla')
	IpATM = get_variable('AT37_IPATM')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT37_ATM_ATS (
	TIPOTRANSFERENCIA NUMERIC,
	CLASIFICACION NUMERIC,
	TIPOCLIENTE VARCHAR(1),
	IDENTIFICACIONCLIENTE VARCHAR(15),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO NUMERIC,
	CUENTACLIENTE CHAR(20),
	MONEDA CHAR(3),
	MONTO NUMERIC,
	FECHA CHAR(8),
	REFERENCIA VARCHAR(20),
	EMPRESAFORMACION CHAR(3),
	MOTIVO VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE CHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE NUMERIC,
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_ATM_ATS;'''
	hook.run(sql_query_deftxt)

	# INSERATR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at37_atm_ats (
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
		6 AS TIPOTRANSFERENCIA,
		1 AS CLASIFICACION,
		CASE 
			WHEN SUBSTRING(ATS.EN_CED_RUC FROM 1 FOR 1) IS NULL THEN 'P'
			ELSE SUBSTRING(ATS.EN_CED_RUC FROM 1 FOR 1)
		END AS TIPOCLIENTE,
		CASE 
			WHEN TRIM(ATS.EN_CED_RUC) IS NULL THEN ATS.P_PASAPORTE
			ELSE SUBSTRING(ATS.EN_CED_RUC FROM 2 FOR LENGTH(ATS.EN_CED_RUC))
		END AS IDENTIFICACIONCLIENTE,
		TRIM(ATS.EN_NOMLAR) AS NOMBRECLIENTE,
		ATS.TIPO_CUENTA AS TIPOINSTRUMENTO,
		TRIM(ATS.TH_CTA_BANCO) AS CUENTACLIENTE,
		'{Moneda_Vzla}' AS MONEDA,
		TRIM(ATS.TH_VALOR) AS MONTO,
		ATS.TH_FECHA AS FECHA,
		TRIM(ATS.TH_SSN_LOCAL) AS REFERENCIA,
		CASE 
			WHEN SUBSTRING(ATS.EN_CED_RUC FROM 1 FOR 1) IN ('F', 'R') THEN '1'
			WHEN SUBSTRING(ATS.EN_CED_RUC FROM 1 FOR 1) = 'M' THEN '2'
			ELSE '0'
		END AS EMPRESAFORMACION,
		CASE 
			WHEN CONCAT(CAST(ATS.TH_TIPO_TRAN AS TEXT), ATS.TT_TRN_NOMBRE) IS NULL THEN 'No Disponible'
			ELSE CONCAT(CAST(ATS.TH_TIPO_TRAN AS TEXT), ATS.TT_TRN_NOMBRE)
		END AS MOTIVO,
		'{IpATM}' AS DIRECCIONIP,
		SUBSTRING(ATS_D.EN_CED_RUC FROM 1 FOR 1) AS TIPOCONTRAPARTE,
		CASE 
			WHEN TRIM(ATS_D.EN_CED_RUC) IS NULL THEN ATS_D.P_PASAPORTE
			ELSE SUBSTRING(ATS_D.EN_CED_RUC FROM 2 FOR LENGTH(ATS_D.EN_CED_RUC))
		END AS IDENTIFICACIONCONTRAPARTE,
		TRIM(ATS_D.EN_NOMLAR) AS NOMBRECONTRAPARTE,
		ATS_D.TIPO_CUENTA AS TIPOINSTRUMENTOCONTRAPARTE,
		TRIM(ATS.TH_CTA_BANCO_D) AS CUENTACONTRAPARTE,
		R_OFCONTA.RE_OFCONTA AS OFICINA
	FROM at_stg.r_ofconta AS R_OFCONTA INNER JOIN at_stg.ats AS ATS ON ATS.TH_OFICINA = R_OFCONTA.RE_OFCONTA
		INNER JOIN at_stg.ats_d AS ATS_D ON TRIM(ATS.TH_CTA_BANCO_D) = TRIM(ATS_D.TH_CTA_BANCO_D);'''
	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_ATM37',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

AT37_bvente_task = PythonOperator(
	task_id='AT37_bvente_task',
	python_callable=AT37_bvente,
	dag=dag
)

AT37_ccente_task = PythonOperator(
	task_id='AT37_ccente_task',
	python_callable=AT37_ccente,
	dag=dag
)

AT37_retran_task = PythonOperator(
	task_id='AT37_retran_task',
	python_callable=AT37_retran,
	dag=dag
)

AT37_trx_task = PythonOperator(
	task_id='AT37_trx_task',
	python_callable=AT37_trx,
	dag=dag
)

AT37_ATM_SOURCE_STG_task = PythonOperator(
	task_id='AT37_ATM_SOURCE_STG_task',
	python_callable=AT37_ATM_SOURCE_STG,
	dag=dag
)

AT37_ATM_temp_Client_Contra_task = PythonOperator(
	task_id='AT37_ATM_temp_Client_Contra_task',
	python_callable=AT37_ATM_temp_Client_Contra,
	dag=dag
)

AT37_ATM_temp_Client_Contra_d_task = PythonOperator(
	task_id='AT37_ATM_temp_Client_Contra_d_task',
	python_callable=AT37_ATM_temp_Client_Contra_d,
	dag=dag
)

AT_37_ATM_task = PythonOperator(
	task_id='AT_37_ATM_task',
	python_callable=AT_37_ATM,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT37_bvente_task >> AT37_ccente_task >> AT37_retran_task >> AT37_trx_task >> AT37_ATM_SOURCE_STG_task >> AT37_ATM_temp_Client_Contra_task >> AT37_ATM_temp_Client_Contra_d_task >> AT_37_ATM_task
