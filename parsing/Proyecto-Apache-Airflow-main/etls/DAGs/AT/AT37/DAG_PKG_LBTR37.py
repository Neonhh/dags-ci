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

def IT_RECIBIDAS_TEMPORAL_BC(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.TEMPORAL_RECIBIDAS_BC (
	HR_ORGNLENDTOENDID VARCHAR(30),
	HR_TIPO_SERVICIO VARCHAR(20),
	HR_HORA DATE,
	HR_PURP VARCHAR(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.TEMPORAL_RECIBIDAS_BC;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ATSUDEBAN.TEMPORAL_RECIBIDAS_BC (
	hr_orgnlendtoendid, 
	hr_tipo_servicio, 
	hr_hora, 
	hr_purp
	) 
	SELECT
		HRE.hr_OrgnlEndToEndId,
		HRE.hr_tipo_servicio,
		HRE.hr_fecha,
		HRE.hr_purp
	FROM SOURCE_COB_REMESAS_HIS.hre_transf_recibidas_control AS HRE
	WHERE HRE.hr_purp NOT IN ('221', '224') AND HRE.hr_fecha >= '{FechainicioS}'::DATE AND HRE.hr_fecha <= '{FechafinS}'::DATE;'''
	hook.run(sql_query_deftxt)

def IT_ENVIADAS_TEMPORAL_BC(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS atsudeban.temporal_enviadas_ref (
	ht_ssn NUMERIC(18),
	ht_cod_end_end VARCHAR(64),
	ht_tipo_servicio VARCHAR(10),
	ht_hora DATE,
	ht_purp VARCHAR(3)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE atsudeban.temporal_enviadas_ref;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ATSUDEBAN.TEMPORAL_ENVIADAS_REF (
	HT_SSN, 
	HT_COD_END_END, 
	HT_TIPO_SERVICIO, 
	HT_HORA, 
	HT_PURP
	) 
	SELECT
		HRE.ht_ssn,
		HRE.ht_cod_end_end,
		HRE.ht_tipo_servicio,
		HRE.ht_fecha,
		HRE.ht_purp
	FROM SOURCE_COB_REMESAS_HIS.hre_transf_control AS HRE
	WHERE HRE.hr_purp NOT IN ('221', '224') AND HRE.hr_fecha >= '{FechainicioS}'::DATE AND HRE.hr_fecha <= '{FechafinS}'::DATE;'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_NEW(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechafinS = get_variable('FechafinS')
	FechainicioS = get_variable('FechainicioS')
	at37_montobs = get_variable('AT37_MONTOBS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT37_LBTR_A (
	TIPOTRANSFERENCIA NUMERIC(15),
	CLASIFICACION NUMERIC(1),
	TIPOCLIENTE VARCHAR(3),
	IDENTIFICACIONCLIENTE VARCHAR(30),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO VARCHAR(5),
	CUENTACLIENTE VARCHAR(20),
	MONEDA VARCHAR(3),
	MONTO NUMERIC(15,2),
	FECHA VARCHAR(8),
	REFERENCIA VARCHAR(30),
	EMPRESAFORMACION VARCHAR(3),
	MOTIVO VARCHAR(150),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(250),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_LBTR_A;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DDESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.AT37_LBTR_A (
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
	SELECT
		AT37DATA_HIST.TIPO_TRANF,
		AT37DATA_HIST.CLASI_TRANF,
		AT37DATA_HIST.TIPO_CLIENTE,
		AT37DATA_HIST.ID_CLIENTE,
		AT37DATA_HIST.NOMBRE_CLIENTE,
		AT37DATA_HIST.TIPO_INSTR_CLI,
		AT37DATA_HIST.CUENTA_CLIENTE,
		AT37DATA_HIST.MONEDA,
		AT37DATA_HIST.MONTO,
		TO_CHAR(AT37DATA_HIST.FECHA_VALOR, 'YYYYMMDD'),
		AT37DATA_HIST.CODIGO_REFEREN,
		AT37DATA_HIST.MENOR_O_EMPFORMA,
		AT37DATA_HIST.MOTIVO_TRANSACCI,
		AT37DATA_HIST.DIRECCION_IP,
		AT37DATA_HIST.TIPO_CLI_CONTRAP,
		AT37DATA_HIST.ID_CLI_CONTRAP,
		AT37DATA_HIST.NOMBRE_CLI_CONTRAP,
		AT37DATA_HIST.TIPO_INSTR_CLI_CONTRAP,
		AT37DATA_HIST.CUENTA_CLIENTE_CONTRAP,
		AT37DATA_HIST.CODIGO_AGENCIA
	FROM ODS.AT37DATA_HIST AS AT37DATA_HIST
	WHERE (AT37DATA_HIST.MONTO >= CAST({at37_montobs} AS NUMERIC))
		AND (AT37DATA_HIST.FECHA_VALOR BETWEEN TO_DATE('{FechainicioS}','MM/DD/YY') AND TO_DATE('{FechafinS}','MM/DD/YY'));'''
	hook.run(sql_query_deftxt)

def PROVEEDORES_RECIBIDAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechaFin_Sem = get_variable('FechaFin_Sem')
	FechaInicio_Sem = get_variable('FechaInicio_Sem')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.PROVEEDORES_RECIBIDAS (
	HR_ORGNLENDTOENDID VARCHAR(30),
	HR_TIPO_SERVICIO VARCHAR(20),
	HR_HORA DATE,
	HR_PURP VARCHAR(5),
	TIPO_REFERENCIA NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.PROVEEDORES_RECIBIDAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ATSUDEBAN.PROVEEDORES_RECIBIDAS (
	HR_ORGNLENDTOENDID,
	HR_TIPO_SERVICIO,
	HR_HORA,
	HR_PURP,
	TIPO_REFERENCIA
	)
	SELECT 
		TEMPORAL_RECIBIDAS_BC.HR_ORGNLENDTOENDID,
		TEMPORAL_RECIBIDAS_BC.HR_TIPO_SERVICIO,
		TEMPORAL_RECIBIDAS_BC.HR_HORA,
		TEMPORAL_RECIBIDAS_BC.HR_PURP,
		5 AS TIPO_REFERENCIA
	FROM ATSUDEBAN.TEMPORAL_RECIBIDAS_BC AS TEMPORAL_RECIBIDAS_BC
	WHERE (TEMPORAL_RECIBIDAS_BC.HR_HORA >= TO_DATE('{FechaInicio_Sem}','MM/DD/YYYY') AND TEMPORAL_RECIBIDAS_BC.HR_HORA <= TO_DATE('{FechaFin_Sem}','MM/DD/YYYY'))
		AND (TEMPORAL_RECIBIDAS_BC.HR_PURP = '222');'''
	hook.run(sql_query_deftxt)
	
def PROVEEDORES_ENVIADAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechaFin_Sem = get_variable('FechaFin_Sem')
	FechaInicio_Sem = get_variable('FechaInicio_Sem')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.PROVEEDORES_ENVIADAS (
	HT_SSN NUMERIC(18),
	HT_COD_END_END VARCHAR(64),
	HT_TIPO_SERVICIO VARCHAR(10),
	HT_HORA DATE,
	HT_PURP VARCHAR(3),
	TIPO_REFERENCIA NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.PROVEEDORES_ENVIADAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ATSUDEBAN.PROVEEDORES_ENVIADAS (
	ht_ssn,
	ht_cod_end_end,
	ht_tipo_servicio,
	ht_hora,
	ht_purp,
	tipo_referencia
	)
	SELECT 
		temporal_enviadas_ref.ht_ssn,
		temporal_enviadas_ref.ht_cod_end_end,
		temporal_enviadas_ref.ht_tipo_servicio,
		temporal_enviadas_ref.ht_hora,
		temporal_enviadas_ref.ht_purp,
		4 AS tipo_referencia
	FROM atsudeban.temporal_enviadas_ref AS temporal_enviadas_ref
	WHERE (temporal_enviadas_ref.ht_hora >= TO_DATE('{FechaInicio_Sem}','MM/DD/YYYY') AND temporal_enviadas_ref.ht_hora <= TO_DATE('{FechaFin_Sem}','MM/DD/YYYY')
		AND (temporal_enviadas_ref.ht_purp IN ('222'));'''
	hook.run(sql_query_deftxt)

def IT_VOLTEAR_PARTE_CONTRAPARTE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_contraparte_voltear (
	tipotransferencia NUMERIC(15),
	clasificacion NUMERIC(1),
	tipocliente VARCHAR(3),
	identificacioncliente VARCHAR(30),
	nombrecliente VARCHAR(250),
	tipoinstrumento VARCHAR(5),
	cuentacliente VARCHAR(20),
	referencia VARCHAR(50),
	tipocontraparte VARCHAR(3),
	identificacioncontraparte VARCHAR(30),
	nombrecontraparte VARCHAR(250),
	tipoinstrumentocontraparte VARCHAR(5),
	cuentacontraparte VARCHAR(20),
	moneda VARCHAR(3),
	monto NUMERIC(15,2),
	fecha VARCHAR(8),
	empresaformacion VARCHAR(3),
	motivo VARCHAR(150),
	direccionip VARCHAR(50),
	oficina NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_CONTRAPARTE_VOLTEAR;'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''INSERT INTO at_stg.at37_contraparte_voltear (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	referencia,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	moneda,
	monto,
	fecha,
	empresformacion,
	motivo,
	direccionip,
	oficina
	)
	SELECT
		at37_lbtr_a.tipotransferencia AS tipotransferencia,
		at37_lbtr_a.clasificacion AS clasificacion,
		at37_lbtr_a.tipocontraparte AS tipocliente,
		at37_lbtr_a.identificacioncontraparte AS identificacioncliente,
		at37_lbtr_a.nombrecontraparte AS nombrecliente,
		at37_lbtr_a.tipoinstrumentocontraparte AS tipoinstrumento,
		at37_lbtr_a.cuentacontraparte AS cuentacliente,
		at37_lbtr_a.referencia AS referencia,
		at37_lbtr_a.tipocliente AS tipocontraparte,
		at37_lbtr_a.identificacioncliente AS identificacioncontraparte,
		at37_lbtr_a.nombrecliente AS nombrecontraparte,
		at37_lbtr_a.tipoinstrumento AS tipoinstrumentocontraparte,
		at37_lbtr_a.cuentacliente AS cuentacontraparte,
		at37_lbtr_a.moneda AS moneda,
		at37_lbtr_a.monto AS monto,
		at37_lbtr_a.fecha AS fecha,
		at37_lbtr_a.empresformacion AS empresformacion,
		at37_lbtr_a.motivo AS motivo,
		at37_lbtr_a.direccionip AS direccionip,
		at37_lbtr_a.oficina AS oficina
	FROM at_stg.at37_lbtr_a as at37_lbtr_a
	WHERE (at37_lbtr_a.cuentacontraparte LIKE '0146%') AND (at37_lbtr_a.tipotransferencia = '2');'''
	hook.run(sql_query_deftxt)

def ACTUALIZA_OFICINAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# 1
	sql_query_deftxt = '''DELETE FROM at_stg.at37_lbtr_a
	WHERE tipotransferencia = '2'AND cuentacontraparte LIKE '0146%';'''
	hook.run(sql_query_deftxt)

	# 2
	sql_query_deftxt = '''INSERT INTO at_stg.at37_lbtr_a (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	referencia,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	moneda,
	monto,
	fecha,
	empresaformacion,
	motivo,
	direccionip,
	oficina
	) 
	SELECT
		tipotransferencia,
		clasificacion,
		tipocliente,
		identificacioncliente,
		nombrecliente,
		tipoinstrumento,
		cuentacliente,
		referencia,
		tipocontraparte,
		identificacioncontraparte,
		nombrecontraparte,
		tipoinstrumentocontraparte,
		cuentacontraparte,
		moneda,
		monto,
		fecha,
		empresaformacion,
		motivo,
		direccionip,
		oficina
	FROM at_stg.at37_contraparte_voltear;'''
	hook.run(sql_query_deftxt)

	# 3
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET NOMBRECONTRAPARTE='NO DISPONIBLE'
	WHERE NOMBRECONTRAPARTE IN (' ', '', '0');'''
	hook.run(sql_query_deftxt)

	# 4
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET NOMBRECLIENTE='NO DISPONIBLE'
	WHERE NOMBRECLIENTE IN (' ', '', '0');'''
	hook.run(sql_query_deftxt)

	# 5
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET OFICINA=SUBSTRING(AT37_LBTR_A.CUENTACLIENTE FROM 6 FOR 3)
	WHERE SUBSTRING(AT37_LBTR_A.CUENTACLIENTE FROM 1 FOR 4) = '0146' AND OFICINA IN ('800','805') AND TIPOTRANSFERENCIA IN ('1','2','3');'''
	hook.run(sql_query_deftxt)

	# 6
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET OFICINA=SUBSTRING(CUENTACONTRAPARTE FROM 6 FOR 3)
	WHERE SUBSTRING(CUENTACONTRAPARTE FROM 1 FOR 4) = '0146' AND OFICINA IN ('800','805') AND TIPOTRANSFERENCIA IN ('2','1','3');'''
	hook.run(sql_query_deftxt)

	# 7
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET OFICINA=SUBSTRING(AT37_LBTR_A.CUENTACLIENTE FROM 6 FOR 3)
	WHERE SUBSTRING(AT37_LBTR_A.CUENTACLIENTE FROM 1 FOR 4) = '0114' AND OFICINA IN ('800', '805') AND TIPOTRANSFERENCIA IN ('1', '3', '2');'''
	hook.run(sql_query_deftxt)

	# 8
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET OFICINA=SUBSTRING(CUENTACONTRAPARTE FROM 6 FOR 3)
	WHERE SUBSTRING(CUENTACONTRAPARTE FROM 1 FOR 4) = '0114' AND OFICINA IN ('800','805') AND TIPOTRANSFERENCIA IN ('1','2','3');'''
	hook.run(sql_query_deftxt)

def TEMP__ACTUALIZA_IP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# 1
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET DIRECCIONIP = '200.109.237.180'
	WHERE CUENTACLIENTE LIKE '0114%' AND TIPOCLIENTE IN ('V', 'E', 'P') AND DIRECCIONIP = '0' AND TIPOTRANSFERENCIA = '1';'''
	hook.run(sql_query_deftxt)

	# 2
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET DIRECCIONIP = '200.74.211.46'
	WHERE CUENTACLIENTE LIKE '0146%' AND TIPOCLIENTE IN ('V', 'E', 'P') AND DIRECCIONIP = '0' AND TIPOTRANSFERENCIA = '1';'''
	hook.run(sql_query_deftxt)

	# 3
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET DIRECCIONIP = '200.109.237.182'
	WHERE CUENTACLIENTE LIKE '0114%' AND TIPOCLIENTE IN ('J', 'G', 'R') AND DIRECCIONIP = '0' AND TIPOTRANSFERENCIA = '1';'''
	hook.run(sql_query_deftxt)

	# 4
	sql_query_deftxt = '''UPDATE AT_STG.AT37_LBTR_A
	SET DIRECCIONIP='200.74.211.47'
	WHERE CUENTACLIENTE LIKE '0146%' AND TIPOCLIENTE IN ('J','G','R') AND DIRECCIONIP='0' AND TIPOTRANSFERENCIA='1';'''
	hook.run(sql_query_deftxt)

def AT37_LTBR_OFIC_CONTABLE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_lbtr_ofccontable (
	tipotransferencia NUMERIC(15),
	clasificacion NUMERIC(1),
	tipocliente VARCHAR(5),
	identificacioncliente VARCHAR(30),
	nombrecliente VARCHAR(250),
	tipoinstrumento VARCHAR(5),
	cuentacliente VARCHAR(20),
	moneda VARCHAR(3),
	monto NUMERIC(15,2),
	fecha VARCHAR(8),
	referencia VARCHAR(30),
	empresaformacion VARCHAR(3),
	motivo VARCHAR(150),
	direccionip VARCHAR(50),
	tipocontraparte VARCHAR(3),
	identificacioncontraparte VARCHAR(30),
	nombrecontraparte VARCHAR(250),
	tipoinstrumentocontraparte VARCHAR(5),
	cuentacontraparte VARCHAR(20),
	oficina NUMERIC(5)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_LBTR_OFCCONTABLE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT37_LBTR_OFCCONTABLE(
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
	SELECT 
		AT37_LBTR_A.TIPOTRANSFERENCIA,
		AT37_LBTR_A.CLASIFICACION,
		AT37_LBTR_A.TIPOCLIENTE,
		AT37_LBTR_A.IDENTIFICACIONCLIENTE,
		AT37_LBTR_A.NOMBRECLIENTE,
		AT37_LBTR_A.TIPOINSTRUMENTO,
		AT37_LBTR_A.CUENTACLIENTE,
		AT37_LBTR_A.MONEDA,
		AT37_LBTR_A.MONTO,
		AT37_LBTR_A.FECHA,
		TRIM(AT37_LBTR_A.REFERENCIA),
		AT37_LBTR_A.EMPRESAFORMACION,
		'TRANSFRENCIA QUE NO CONSIGO' AS MOTIVO,
		AT37_LBTR_A.DIRECCIONIP,
		AT37_LBTR_A.TIPOCONTRAPARTE,
		AT37_LBTR_A.IDENTIFICACIONCONTRAPARTE,
		AT37_LBTR_A.NOMBRECONTRAPARTE,
		AT37_LBTR_A.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_LBTR_A.CUENTACONTRAPARTE,
		R_OFCONTA.RE_OFCONTA
	FROM AT_STG.AT37_LBTR_A AS AT37_LBTR_A INNER JOIN AT_STG.R_OFCONTA AS R_OFCONTA ON (TRIM(AT37_LBTR_A.OFICINA) = TRIM(R_OFCONTA.RE_OFADMIN));'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_PARTE1(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechainicioS = get_variable('FechainicioS')
	FechafinS = get_variable('FechafinS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_temp_update_enviadas (
	cuentacliente	VARCHAR(20),
	cuentacontraparte	VARCHAR(20),
	direccionip	VARCHAR(50),
	empresaformacion	VARCHAR(3),
	fecha	VARCHAR(8),
	identificacioncliente	VARCHAR(30),
	identificacioncontraparte	VARCHAR(30),
	moneda	VARCHAR(3),
	monto	NUMERIC(15,2),
	motivo	VARCHAR(150),
	nombrecliente	VARCHAR(250),
	nombrecontraparte	VARCHAR(250),
	oficina	NUMERIC(5),
	referencia	VARCHAR(30),
	tipocliente	VARCHAR(1),
	clasificacion	NUMERIC(1),
	tipocontraparte	VARCHAR(3),
	tipoinstrumento	VARCHAR(5),
	tipoinstrumentocontraparte	VARCHAR(5),
	tipotransferencia	NUMERIC(15)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_TEMP_UPDATE_ENVIADAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.AT37_TEMP_UPDATE_ENVIADAS (
	CUENTACLIENTE,
	CUENTACONTRAPARTE,
	DIRECCIONIP,
	EMPRESAFORMACION,
	FECHA,
	IDENTIFICACIONCLIENTE,
	IDENTIFICACIONCONTRAPARTE,
	MONEDA,
	MONTO,
	MOTIVO,
	NOMBRECLIENTE,
	NOMBRECONTRAPARTE,
	OFICINA,
	REFERENCIA,
	TIPOCLIENTE,
	CLASIFICACION,
	TIPOCONTRAPARTE,
	TIPOINSTRUMENTO,
	TIPOINSTRUMENTOCONTRAPARTE,
	TIPOTRANSFERENCIA
	)
	SELECT 
		AT37_LBTR_OFCCONTABLE.CUENTACLIENTE,
		AT37_LBTR_OFCCONTABLE.CUENTACONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.DIRECCIONIP,
		AT37_LBTR_OFCCONTABLE.EMPRESAFORMACION,
		AT37_LBTR_OFCCONTABLE.FECHA,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCLIENTE,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.MONEDA,
		AT37_LBTR_OFCCONTABLE.MONTO,
		'TRANSFERENCIA CREDITO INMEDIATO ENVIADA' AS MOTIVO,
		AT37_LBTR_OFCCONTABLE.NOMBRECLIENTE,
		AT37_LBTR_OFCCONTABLE.NOMBRECONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.OFICINA,
		AT37_LBTR_OFCCONTABLE.REFERENCIA,
		AT37_LBTR_OFCCONTABLE.TIPOCLIENTE,
		AT37_LBTR_OFCCONTABLE.CLASIFICACION,
		AT37_LBTR_OFCCONTABLE.TIPOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTO,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOTRANSFERENCIA
	FROM at_stg.AT37_LBTR_OFCCONTABLE AS AT37_LBTR_OFCCONTABLE INNER JOIN ATSUDEBAN.TEMPORAL_ENVIADAS_REF AS TEMPORAL_ENVIADAS_REF
		ON TRIM(AT37_LBTR_OFCCONTABLE.REFERENCIA) = TRIM(TEMPORAL_ENVIADAS_REF.REFERENCIA)
	WHERE TEMPORAL_ENVIADAS_REF.HT_TIPO_SERVICIO = 'CRIN'
  		AND TEMPORAL_ENVIADAS_REF.HT_HORA BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY');'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_PARTE2(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechainicioS = get_variable('FechainicioS')
	FechafinS = get_variable('FechafinS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_temp_update_recibidas (
	cuentacliente	VARCHAR(20),
	cuentacontraparte	VARCHAR(20),
	direccionip	VARCHAR(50),
	empresaformacion	VARCHAR(3),
	fecha	VARCHAR(8),
	identificacioncliente	VARCHAR(30),
	identificacioncontraparte	VARCHAR(30),
	moneda	VARCHAR(3),
	monto	NUMERIC(15,2),
	motivo	VARCHAR(150),
	nombrecliente	VARCHAR(250),
	nombrecontraparte	VARCHAR(250),
	oficina	NUMERIC(5),
	referencia	VARCHAR(30),
	tipocliente	VARCHAR(3),
	clasificacion	NUMERIC(1),
	tipocontraparte	VARCHAR(3),
	tipoinstrumento	VARCHAR(5),
	tipoinstrumentocontraparte	VARCHAR(5),
	tipotransferencia	NUMERIC(15)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE at_stg.at37_temp_update_recibidas;'''
	hook.run(sql_query_deftxt)
	
	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at37_temp_update_recibidas (
	CUENTACLIENTE,
	CUENTACONTRAPARTE,
	DIRECCIONIP,
	EMPRESAFORMACION,
	FECHA,
	IDENTIFICACIONCLIENTE,
	IDENTIFICACIONCONTRAPARTE,
	MONEDA,
	MONTO,
	MOTIVO,
	NOMBRECLIENTE,
	NOMBRECONTRAPARTE,
	OFICINA,
	REFERENCIA,
	TIPOCLIENTE,
	CLASIFICACION,
	TIPOCONTRAPARTE,
	TIPOINSTRUMENTO,
	TIPOINSTRUMENTOCONTRAPARTE,
	TIPOTRANSFERENCIA
	)
	SELECT 
		AT37_LBTR_OFCCONTABLE.CUENTACLIENTE,
		AT37_LBTR_OFCCONTABLE.CUENTACONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.DIRECCIONIP,
		AT37_LBTR_OFCCONTABLE.EMPRESAFORMACION,
		AT37_LBTR_OFCCONTABLE.FECHA,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCLIENTE,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.MONEDA,
		AT37_LBTR_OFCCONTABLE.MONTO,
		'TRANSFERENCIA CREDITO INMEDIATO RECIBIDA' AS MOTIVO,
		AT37_LBTR_OFCCONTABLE.NOMBRECLIENTE,
		AT37_LBTR_OFCCONTABLE.NOMBRECONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.OFICINA,
		AT37_LBTR_OFCCONTABLE.REFERENCIA,
		AT37_LBTR_OFCCONTABLE.TIPOCLIENTE,
		AT37_LBTR_OFCCONTABLE.CLASIFICACION,
		AT37_LBTR_OFCCONTABLE.TIPOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTO,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOTRANSFERENCIA
	FROM AT_STG.AT37_LBTR_OFCCONTABLE AS AT37_LBTR_OFCCONTABLE INNER JOIN ATSUDEBAN.TEMPORAL_RECIBIDAS_BC AS TEMPORAL_RECIBIDAS_BC
		ON TRIM(AT37_LBTR_OFCCONTABLE.REFERENCIA) = TRIM(TEMPORAL_RECIBIDAS_BC.HR_ORGNLENDTOENDID)
	WHERE TEMPORAL_RECIBIDAS_BC.HR_TIPO_SERVICIO = 'CRIN'
		AND TEMPORAL_RECIBIDAS_BC.HR_HORA BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY')
	;'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_PARTE3(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechainicioS = get_variable('FechainicioS')
	FechafinS = get_variable('FechafinS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_temp_update_rec_lbtr (
	cuentacliente	VARCHAR(20),
	cuentacontraparte	VARCHAR(20),
	direccionip	VARCHAR(50),
	empresaformacion	VARCHAR(3),
	fecha	VARCHAR(8),
	identificacioncliente	VARCHAR(30),
	identificacioncontraparte	VARCHAR(30),
	moneda	VARCHAR(3),
	monto	NUMERIC(15,2),
	motivo	VARCHAR(150),
	nombrecliente	VARCHAR(250),
	nombrecontraparte	VARCHAR(250),
	oficina numeric(5),
	referencia	VARCHAR(30),
	tipocliente	VARCHAR(3),
	tipocontraparte	VARCHAR(3),
	tipoinstrumento	VARCHAR(5),
	tipoinstrumentocontraparte	VARCHAR(5),
	tipotransferencia	NUMERIC(15),
	clasificacion	NUMERIC(1)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_TEMP_UPDATE_REC_LBTR;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at37_temp_update_rec_lbtr (
	CUENTACLIENTE,
	CUENTACONTRAPARTE,
	DIRECCIONIP,
	EMPRESAFORMACION,
	FECHA,
	IDENTIFICACIONCLIENTE,
	IDENTIFICACIONCONTRAPARTE,
	MONEDA,
	MONTO,
	MOTIVO,
	NOMBRECLIENTE,
	NOMBRECONTRAPARTE,
	OFICINA,
	REFERENCIA,
	TIPOCLIENTE,
	TIPOCONTRAPARTE,
	TIPOINSTRUMENTO,
	TIPOINSTRUMENTOCONTRAPARTE,
	TIPOTRANSFERENCIA,
	CLASIFICACION
	)
	SELECT 
		AT37_LBTR_OFCCONTABLE.CUENTACLIENTE,
		AT37_LBTR_OFCCONTABLE.CUENTACONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.DIRECCIONIP,
		AT37_LBTR_OFCCONTABLE.EMPRESAFORMACION,
		AT37_LBTR_OFCCONTABLE.FECHA,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCLIENTE,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.MONEDA,
		AT37_LBTR_OFCCONTABLE.MONTO,
		'TRANSFERENCIA LBTR RECIBIDA' AS MOTIVO,
		AT37_LBTR_OFCCONTABLE.NOMBRECLIENTE,
		AT37_LBTR_OFCCONTABLE.NOMBRECONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.OFICINA,
		AT37_LBTR_OFCCONTABLE.REFERENCIA,
		AT37_LBTR_OFCCONTABLE.TIPOCLIENTE,
		AT37_LBTR_OFCCONTABLE.TIPOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTO,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOTRANSFERENCIA,
		AT37_LBTR_OFCCONTABLE.CLASIFICACION
	FROM AT_STG.AT37_LBTR_OFCCONTABLE AS AT37_LBTR_OFCCONTABLE INNER JOIN ATSUDEBAN.TEMPORAL_RECIBIDAS_BC AS ATSUDEBAN.TEMPORAL_RECIBIDAS_BC
		ON (TRIM(AT37_LBTR_OFCCONTABLE.REFERENCIA) = TRIM(TEMPORAL_RECIBIDAS_BC.HR_ORGNLENDTOENDID))
	WHERE TEMPORAL_RECIBIDAS_BC.HR_TIPO_SERVICIO = 'LBTR'
  		AND TEMPORAL_RECIBIDAS_BC.HR_HORA BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY')
		;'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_PARTE4(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	FechainicioS = get_variable('FechainicioS')
	FechafinS = get_variable('FechafinS')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at37_temp_update_env_lbtr (
	cuentacliente	VARCHAR(20),
	cuentacontraparte	VARCHAR(20),
	direccionip	VARCHAR(50),
	empresaformacion	VARCHAR(3),
	fecha	VARCHAR(8),
	identificacioncliente	VARCHAR(30),
	identificacioncontraparte	VARCHAR(30),
	moneda	VARCHAR(3),
	monto	NUMERIC(15,2),
	motivo	VARCHAR(150),
	nombrecliente	VARCHAR(250),
	nombrecontraparte	VARCHAR(250),
	oficina	NUMERIC(5),
	referencia	VARCHAR(30),
	tipocliente	VARCHAR(3),
	clasificacion	NUMERIC(1),
	tipocontraparte	VARCHAR(3),
	tipoinstrumento	VARCHAR(5),
	tipoinstrumentocontraparte	VARCHAR(5),
	tipotransferencia	NUMERIC(15)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT37_TEMP_UPDATE_ENV_LBTR;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at37_temp_update_env_lbtr (
	CUENTACLIENTE,
	CUENTACONTRAPARTE,
	DIRECCIONIP,
	EMPRESAFORMACION,
	FECHA,
	IDENTIFICACIONCLIENTE,
	IDENTIFICACIONCONTRAPARTE,
	MONEDA,
	MONTO,
	MOTIVO,
	NOMBRECLIENTE,
	NOMBRECONTRAPARTE,
	OFICINA,
	REFERENCIA,
	TIPOCLIENTE,
	CLASIFICACION,
	TIPOCONTRAPARTE,
	TIPOINSTRUMENTO,
	TIPOINSTRUMENTOCONTRAPARTE,
	TIPOTRANSFERENCIA
	)
	SELECT 
		AT37_LBTR_OFCCONTABLE.CUENTACLIENTE,
		AT37_LBTR_OFCCONTABLE.CUENTACONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.DIRECCIONIP,
		AT37_LBTR_OFCCONTABLE.EMPRESAFORMACION,
		AT37_LBTR_OFCCONTABLE.FECHA,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCLIENTE,
		AT37_LBTR_OFCCONTABLE.IDENTIFICACIONCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.MONEDA,
		AT37_LBTR_OFCCONTABLE.MONTO,
		'TRANSFERENCIA LBTR ENVIADA' ,
		AT37_LBTR_OFCCONTABLE.NOMBRECLIENTE,
		AT37_LBTR_OFCCONTABLE.NOMBRECONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.OFICINA,
		AT37_LBTR_OFCCONTABLE.REFERENCIA,
		AT37_LBTR_OFCCONTABLE.TIPOCLIENTE,
		AT37_LBTR_OFCCONTABLE.CLASIFICACION,
		AT37_LBTR_OFCCONTABLE.TIPOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTO,
		AT37_LBTR_OFCCONTABLE.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_LBTR_OFCCONTABLE.TIPOTRANSFERENCIA
	FROM AT_STG.AT37_LBTR_OFCCONTABLE AS AT37_LBTR_OFCCONTABLE INNER JOIN ATSUDEBAN.TEMPORAL_ENVIADAS_REF AS TEMPORAL_ENVIADAS_REF
		ON (AT37_LBTR_OFCCONTABLE.REFERENCIA = TEMPORAL_ENVIADAS_REF.HT_COD_END_END)
	WHERE (TRIM(TEMPORAL_ENVIADAS_REF.HT_TIPO_SERVICIO) = 'LBTR')
  		AND (TEMPORAL_ENVIADAS_REF.HT_HORA BETWEEN TO_DATE('{FechainicioS}', 'MM/DD/YY') AND TO_DATE('{FechafinS}', 'MM/DD/YY'));'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_PARTE5_UNION_ALL(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_PREVIO_TODOS (
	FECHA VARCHAR(8),
	CLASIFICACION NUMERIC(1),
	CUENTACLIENTE VARCHAR(20),
	CUENTACONTRAPARTE VARCHAR(20),
	DIRECCIONIP VARCHAR(50),
	EMPRESAFORMACION VARCHAR(3),
	IDENTIFICACIONCLIENTE VARCHAR(30),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	MONEDA VARCHAR(3),
	MONTO NUMERIC(15,2),
	MOTIVO VARCHAR(150),
	NOMBRECLIENTE VARCHAR(250),
	NOMBRECONTRAPARTE VARCHAR(250),
	OFICINA NUMERIC(5),
	REFERENCIA VARCHAR(30),
	TIPOCLIENTE VARCHAR(3),
	TIPOCONTRAPARTE VARCHAR(3),
	TIPOINSTRUMENTO VARCHAR(5),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	TIPOTRANSFERENCIA NUMERIC(15)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_PREVIO_TODOS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_PREVIO_TODOS (
	FECHA,
	CLASIFICACION,
	CUENTACLIENTE,
	CUENTACONTRAPARTE,
	DIRECCIONIP,
	EMPRESAFORMACION,
	IDENTIFICACIONCLIENTE,
	IDENTIFICACIONCONTRAPARTE,
	MONEDA,
	MONTO,
	MOTIVO,
	NOMBRECLIENTE,
	NOMBRECONTRAPARTE,
	OFICINA,
	REFERENCIA,
	TIPOCLIENTE,
	TIPOCONTRAPARTE,
	TIPOINSTRUMENTO,
	TIPOINSTRUMENTOCONTRAPARTE,
	TIPOTRANSFERENCIA
	)
	SELECT
		AT37_TEMP_UPDATE_ENVIADAS.FECHA,
		AT37_TEMP_UPDATE_ENVIADAS.CLASIFICACION,
		AT37_TEMP_UPDATE_ENVIADAS.CUENTACLIENTE,
		AT37_TEMP_UPDATE_ENVIADAS.CUENTACONTRAPARTE,
		AT37_TEMP_UPDATE_ENVIADAS.DIRECCIONIP,
		AT37_TEMP_UPDATE_ENVIADAS.EMPRESAFORMACION,
		AT37_TEMP_UPDATE_ENVIADAS.IDENTIFICACIONCLIENTE,
		AT37_TEMP_UPDATE_ENVIADAS.IDENTIFICACIONCONTRAPARTE,
		AT37_TEMP_UPDATE_ENVIADAS.MONEDA,
		AT37_TEMP_UPDATE_ENVIADAS.MONTO,
		AT37_TEMP_UPDATE_ENVIADAS.MOTIVO,
		AT37_TEMP_UPDATE_ENVIADAS.NOMBRECLIENTE,
		AT37_TEMP_UPDATE_ENVIADAS.NOMBRECONTRAPARTE,
		AT37_TEMP_UPDATE_ENVIADAS.OFICINA,
		AT37_TEMP_UPDATE_ENVIADAS.REFERENCIA,
		AT37_TEMP_UPDATE_ENVIADAS.TIPOCLIENTE,
		AT37_TEMP_UPDATE_ENVIADAS.TIPOCONTRAPARTE,
		AT37_TEMP_UPDATE_ENVIADAS.TIPOINSTRUMENTO,
		AT37_TEMP_UPDATE_ENVIADAS.TIPOINSTRUMENTOCONTRAPARTE,
		CASE 
			WHEN AT37_TEMP_UPDATE_ENVIADAS.TIPOTRANSFERENCIA = 1 THEN 6
		END AS TIPOTRANSFERENCIA
	FROM AT_STG.AT37_TEMP_UPDATE_ENVIADAS

	UNION

	SELECT
		AT37_TEMP_UPDATE_RECIBIDAS.FECHA,
		AT37_TEMP_UPDATE_RECIBIDAS.CLASIFICACION,
		AT37_TEMP_UPDATE_RECIBIDAS.CUENTACLIENTE,
		AT37_TEMP_UPDATE_RECIBIDAS.CUENTACONTRAPARTE,
		AT37_TEMP_UPDATE_RECIBIDAS.DIRECCIONIP,
		AT37_TEMP_UPDATE_RECIBIDAS.EMPRESAFORMACION,
		AT37_TEMP_UPDATE_RECIBIDAS.IDENTIFICACIONCLIENTE,
		AT37_TEMP_UPDATE_RECIBIDAS.IDENTIFICACIONCONTRAPARTE,
		AT37_TEMP_UPDATE_RECIBIDAS.MONEDA,
		AT37_TEMP_UPDATE_RECIBIDAS.MONTO,
		AT37_TEMP_UPDATE_RECIBIDAS.MOTIVO,
		AT37_TEMP_UPDATE_RECIBIDAS.NOMBRECLIENTE,
		AT37_TEMP_UPDATE_RECIBIDAS.NOMBRECONTRAPARTE,
		AT37_TEMP_UPDATE_RECIBIDAS.OFICINA,
		AT37_TEMP_UPDATE_RECIBIDAS.REFERENCIA,
		AT37_TEMP_UPDATE_RECIBIDAS.TIPOCLIENTE,
		AT37_TEMP_UPDATE_RECIBIDAS.TIPOCONTRAPARTE,
		AT37_TEMP_UPDATE_RECIBIDAS.TIPOINSTRUMENTO,
		AT37_TEMP_UPDATE_RECIBIDAS.TIPOINSTRUMENTOCONTRAPARTE,
		CASE 
			WHEN AT37_TEMP_UPDATE_RECIBIDAS.TIPOTRANSFERENCIA = 2 THEN 7
		END AS TIPOTRANSFERENCIA
	FROM AT_STG.AT37_TEMP_UPDATE_RECIBIDAS

	UNION

	SELECT
		AT37_TEMP_UPDATE_REC_LBTR.FECHA,
		AT37_TEMP_UPDATE_REC_LBTR.CLASIFICACION,
		AT37_TEMP_UPDATE_REC_LBTR.CUENTACLIENTE,
		AT37_TEMP_UPDATE_REC_LBTR.CUENTACONTRAPARTE,
		AT37_TEMP_UPDATE_REC_LBTR.DIRECCIONIP,
		AT37_TEMP_UPDATE_REC_LBTR.EMPRESAFORMACION,
		AT37_TEMP_UPDATE_REC_LBTR.IDENTIFICACIONCLIENTE,
		AT37_TEMP_UPDATE_REC_LBTR.IDENTIFICACIONCONTRAPARTE,
		AT37_TEMP_UPDATE_REC_LBTR.MONEDA,
		AT37_TEMP_UPDATE_REC_LBTR.MONTO,
		AT37_TEMP_UPDATE_REC_LBTR.MOTIVO,
		AT37_TEMP_UPDATE_REC_LBTR.NOMBRECLIENTE,
		AT37_TEMP_UPDATE_REC_LBTR.NOMBRECONTRAPARTE,
		AT37_TEMP_UPDATE_REC_LBTR.OFICINA,
		AT37_TEMP_UPDATE_REC_LBTR.REFERENCIA,
		AT37_TEMP_UPDATE_REC_LBTR.TIPOCLIENTE,
		AT37_TEMP_UPDATE_REC_LBTR.TIPOCONTRAPARTE,
		AT37_TEMP_UPDATE_REC_LBTR.TIPOINSTRUMENTO,
		AT37_TEMP_UPDATE_REC_LBTR.TIPOINSTRUMENTOCONTRAPARTE,
		CASE 
			WHEN AT37_TEMP_UPDATE_REC_LBTR.TIPOTRANSFERENCIA = 2 THEN 7
		END AS TIPOTRANSFERENCIA
	FROM AT_STG.AT37_TEMP_UPDATE_REC_LBTR

	UNION

	SELECT
		AT37_TEMP_UPDATE_ENV_LBTR.FECHA,
		AT37_TEMP_UPDATE_ENV_LBTR.CLASIFICACION,
		AT37_TEMP_UPDATE_ENV_LBTR.CUENTACLIENTE,
		AT37_TEMP_UPDATE_ENV_LBTR.CUENTACONTRAPARTE,
		AT37_TEMP_UPDATE_ENV_LBTR.DIRECCIONIP,
		AT37_TEMP_UPDATE_ENV_LBTR.EMPRESAFORMACION,
		AT37_TEMP_UPDATE_ENV_LBTR.IDENTIFICACIONCLIENTE,
		AT37_TEMP_UPDATE_ENV_LBTR.IDENTIFICACIONCONTRAPARTE,
		AT37_TEMP_UPDATE_ENV_LBTR.MONEDA,
		AT37_TEMP_UPDATE_ENV_LBTR.MONTO,
		AT37_TEMP_UPDATE_ENV_LBTR.MOTIVO,
		AT37_TEMP_UPDATE_ENV_LBTR.NOMBRECLIENTE,
		AT37_TEMP_UPDATE_ENV_LBTR.NOMBRECONTRAPARTE,
		AT37_TEMP_UPDATE_ENV_LBTR.OFICINA,
		AT37_TEMP_UPDATE_ENV_LBTR.REFERENCIA,
		AT37_TEMP_UPDATE_ENV_LBTR.TIPOCLIENTE,
		AT37_TEMP_UPDATE_ENV_LBTR.TIPOCONTRAPARTE,
		AT37_TEMP_UPDATE_ENV_LBTR.TIPOINSTRUMENTO,
		AT37_TEMP_UPDATE_ENV_LBTR.TIPOINSTRUMENTOCONTRAPARTE,
		CASE 
			WHEN AT37_TEMP_UPDATE_ENV_LBTR.TIPOTRANSFERENCIA = 1 THEN 6
		END AS TIPOTRANSFERENCIA
	FROM AT_STG.AT37_TEMP_UPDATE_ENV_LBTR
	;'''
	hook.run(sql_query_deftxt)

def ACTUALIZAR_PROVEEDORES(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AT37_PREVIO_TODOS AS a
	SET a.TIPOTRANSFERENCIA = (
		SELECT b.TIPO_REFERENCIA
		FROM ATSUDEBAN.PROVEEDORES_ENVIADAS AS b
		WHERE a.REFERENCIA = b.HT_COD_END_END
	)
	WHERE EXISTS (
		SELECT '1'
		FROM ATSUDEBAN.PROVEEDORES_ENVIADAS AS b
		WHERE a.REFERENCIA = b.HT_COD_END_END
	);'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''UPDATE ATSUDEBAN.AT37_PREVIO_TODOS a
	SET a.TIPOTRANSFERENCIA = (
		SELECT b.TIPO_REFERENCIA 
		FROM ATSUDEBAN.PROVEEDORES_RECIBIDAS b 
		WHERE a.REFERENCIA = b.HR_ORGNLENDTOENDID
	)
	WHERE EXISTS (
		SELECT '1' 
		FROM ATSUDEBAN.PROVEEDORES_RECIBIDAS b 
		WHERE a.REFERENCIA = b.HR_ORGNLENDTOENDID
	);'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_ATSUDEBAN(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_LBTRS (
	FECHA_EJECUCION DATE,
	TIPOTRANSFERENCIA NUMERIC,
	CLASIFICACION NUMERIC,
	TIPOCLIENTE VARCHAR(3),
	IDENTIFICACIONCLIENTE VARCHAR(15),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO VARCHAR(5),
	CUENTACLIENTE CHAR(20),
	MONEDA CHAR(3),
	MONTO NUMERIC,
	FECHA CHAR(8),
	REFERENCIA VARCHAR(30),
	EMPRESAFORMACION VARCHAR(3),
	MOTIVO VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA NUMERIC
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_LBTRS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO atsudeban.at37_lbtrs (
	fecha_ejecucion,
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
	oficina
	)
	SELECT DISTINCT
		NULL AS fecha_ejecucion,
		tipotransferencia,
		clasificacion,
		tipocliente,
		ltrim(identificacioncliente, '0'),
		regexp_replace(nombrecliente,'(^[[:space:]]+|[[:space:]]+$)','','g'),
		tipoinstrumento,
		cuentacliente,
		moneda,
		monto,
		fecha,
		referencia,
		substring(trim(empresaformacion) from 1 for 1),
		motivo,
		direccionip,
		substring(trim(tipocontraparte) from 1 for 1),
		identificacioncontraparte,
		regexp_replace(nombrecontraparte,'(^[[:space:]]+|[[:space:]]+$)','','g'),
		tipoinstrumentocontraparte,
		cuentacontraparte,
		oficina
	FROM atsudeban.at37_previo_todos
	WHERE (identificacioncliente IS NOT NULL AND identificacioncontraparte IS NOT NULL);'''
	hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_LBTR37',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

IT_RECIBIDAS_TEMPORAL_BC_task = PythonOperator(
	task_id='IT_RECIBIDAS_TEMPORAL_BC_task',
	python_callable=IT_RECIBIDAS_TEMPORAL_BC,
	dag=dag
)

IT_ENVIADAS_TEMPORAL_BC_task = PythonOperator(
	task_id='IT_ENVIADAS_TEMPORAL_BC_task',
	python_callable=IT_ENVIADAS_TEMPORAL_BC,
	dag=dag
)

AT37_LBTR_NEW_task = PythonOperator(
	task_id='AT37_LBTR_NEW_task',
	python_callable=AT37_LBTR_NEW,
	dag=dag
)

PROVEEDORES_RECIBIDAS_task = PythonOperator(
	task_id='PROVEEDORES_RECIBIDAS_task',
	python_callable=PROVEEDORES_RECIBIDAS,
	dag=dag
)

PROVEEDORES_ENVIADAS_task = PythonOperator(
	task_id='PROVEEDORES_ENVIADAS_task',
	python_callable=PROVEEDORES_ENVIADAS,
	dag=dag
)

IT_VOLTEAR_PARTE_CONTRAPARTE_task = PythonOperator(
	task_id='IT_VOLTEAR_PARTE_CONTRAPARTE_task',
	python_callable=IT_VOLTEAR_PARTE_CONTRAPARTE,
	dag=dag
)

ACTUALIZA_OFICINAS_task = PythonOperator(
	task_id='ACTUALIZA_OFICINAS_task',
	python_callable=ACTUALIZA_OFICINAS,
	dag=dag
)

TEMP__ACTUALIZA_IP_task = PythonOperator(
	task_id='TEMP__ACTUALIZA_IP_task',
	python_callable=TEMP__ACTUALIZA_IP,
	dag=dag
)

AT37_LTBR_OFIC_CONTABLE_task = PythonOperator(
	task_id='AT37_LTBR_OFIC_CONTABLE_task',
	python_callable=AT37_LTBR_OFIC_CONTABLE,
	dag=dag
)

AT37_LBTR_PARTE1_task = PythonOperator(
	task_id='AT37_LBTR_PARTE1_task',
	python_callable=AT37_LBTR_PARTE1,
	dag=dag
)

AT37_LBTR_PARTE2_task = PythonOperator(
	task_id='AT37_LBTR_PARTE2_task',
	python_callable=AT37_LBTR_PARTE2,
	dag=dag
)

AT37_LBTR_PARTE3_task = PythonOperator(
	task_id='AT37_LBTR_PARTE3_task',
	python_callable=AT37_LBTR_PARTE3,
	dag=dag
)

AT37_LBTR_PARTE4_task = PythonOperator(
	task_id='AT37_LBTR_PARTE4_task',
	python_callable=AT37_LBTR_PARTE4,
	dag=dag
)

AT37_LBTR_PARTE5_UNION_ALL_task = PythonOperator(
	task_id='AT37_LBTR_PARTE5_UNION_ALL_task',
	python_callable=AT37_LBTR_PARTE5_UNION_ALL,
	dag=dag
)

ACTUALIZAR_PROVEEDORES_task = PythonOperator(
	task_id='ACTUALIZAR_PROVEEDORES_task',
	python_callable=ACTUALIZAR_PROVEEDORES,
	dag=dag
)

AT37_LBTR_ATSUDEBAN_task = PythonOperator(
	task_id='AT37_LBTR_ATSUDEBAN_task',
	python_callable=AT37_LBTR_ATSUDEBAN,
	dag=dag
)

Execution_of_the_Scenario_PKG_EXCLUIDAS_version_001_task = TriggerDagRunOperator(
	task_id='Execution_of_the_Scenario_PKG_EXCLUIDAS_version_001_task',
	trigger_dag_id='PKG_EXCLUIDAS',
	wait_for_completion=True,
	dag=dag
)


###### SECUENCIA DE EJECUCION ######
IT_RECIBIDAS_TEMPORAL_BC_task >> IT_ENVIADAS_TEMPORAL_BC_task >> AT37_LBTR_NEW_task >> PROVEEDORES_RECIBIDAS_task >> PROVEEDORES_ENVIADAS_task >> IT_VOLTEAR_PARTE_CONTRAPARTE_task >> ACTUALIZA_OFICINAS_task >> TEMP__ACTUALIZA_IP_task >> AT37_LTBR_OFIC_CONTABLE_task >> AT37_LBTR_PARTE1_task >> AT37_LBTR_PARTE2_task >> AT37_LBTR_PARTE3_task >> AT37_LBTR_PARTE4_task >> AT37_LBTR_PARTE5_UNION_ALL_task >> ACTUALIZAR_PROVEEDORES_task >> AT37_LBTR_ATSUDEBAN_task >> Execution_of_the_Scenario_PKG_EXCLUIDAS_version_001_task
