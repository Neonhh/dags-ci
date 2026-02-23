from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
import time
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import tempfile
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
def AT13_Catalogo(**kwargs):

	hook = PostgresHook(postgres_conn_id='ods')

	# CREAR VISTA TEMPORAL DESTINO
	sql_query_deftxt = '''
	CREATE TEMP VIEW AT13_CATALOGO AS
	SELECT TA_CODIGO
	FROM ODS.IF_TABLA_CATALOGO
	WHERE TA_TABLA = 'if_estado_reclamo';
	'''
	hook.run(sql_query_deftxt)

def AT13_Catalogo_det(**kwargs):

	hook = PostgresHook(postgres_conn_id='ods')

	# CREAR VISTA TEMPORAL DESTINO
	sql_query_deftxt = ''' 
	CREATE TEMP VIEW AT13_CATALOGO_DET AS
	SELECT 
		CA_CODIGO,
		CA_VALOR,
		CA_TABLA
	FROM ODS.IF_CATALOGO_DETALLE;
	'''
	hook.run(sql_query_deftxt)

def AT13_Categoria(**kwargs):

	hook = PostgresHook(postgres_conn_id='ods')

	# CREAR VISTA TEMPORAL DESTINO
	sql_query_deftxt = ''' 
	CREATE TEMP VIEW AT13_CATEGORIA AS
	SELECT 
		TC_CODIGO,
		TC_CATEGORIA
	FROM ODS.IF_TIPO_CATEGORIA;
	'''
	hook.run(sql_query_deftxt)

def AT13_Categoria_recl(**kwargs):

	hook = PostgresHook(postgres_conn_id='ods')

	# CREAR VISTA TEMPORAL DESTINO
	sql_query_deftxt = ''' 
	CREATE TEMP VIEW AT13_CATEGORIA_RECL AS
	SELECT 
		TC_SECUENCIAL,
		TC_TIPO_RECL
	FROM ODS.IF_CATEGORIA_RECL_GIR5;
	'''
	hook.run(sql_query_deftxt)

def AT13_DetalleRecl(**kwargs):
	repodata_hook = PostgresHook(postgres_conn_id='repodataprd')
	ods_hook = PostgresHook(postgres_conn_id='ods')

	# CREAR TABLA DESTINO
	sql_query_deftxt = ''' 
	CREATE TABLE IF NOT EXISTS AT_STG.AT13_DETRECL(
		SR_SECUENCIAL	NUMERIC(10),
		SR_FECHACARGA	DATE
	)'''
	repodata_hook.run(sql_query_deftxt)

	# Vaciamos la tabla (para no duplicar datos)
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_DETRECL;'''
	repodata_hook.run(sql_query_deftxt)

	# Consulta en la BD ods
	select_sql = ''' SELECT DISTINCT
		SR_SECUENCIAL,
		MAX(TO_DATE(CONCAT(CONCAT(SUBSTRING(FECHACARGA, 1, 2), '/'), CONCAT(SUBSTRING(FECHACARGA, 3, 2), '/22')), 'DD/MM/YY')) AS SR_FECHACARGA
	FROM ODS.IF_SOL_RECLAMOS_GIR5_AT
	WHERE SR_FECHA >= TO_DATE('01/01/2018', 'MM/DD/YYYY')
	GROUP BY SR_SECUENCIAL; '''

	# conexion a bd ods
	ods_conn = ods_hook.get_conn()
	ods_cursor = ods_conn.cursor(name='fetch_cursor')  # Cursor server-side para no cargar todo en RAM
	ods_cursor.execute(select_sql)

	# INSERTAMOS DATOS
	sql_query_deftxt = '''
	INSERT INTO AT_STG.AT13_DETRECL(
		SR_SECUENCIAL,
		SR_FECHACARGA
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
		execute_values(repodata_cursor, sql_query_deftxt, rows)
		repodata_conn.commit()

		total_insertados += len(rows)
		logger.info(f"Lote {batch_num}: insertados {len(rows)} registros (Total acumulado: {total_insertados})")

	# cerrar conexiones
	ods_cursor.close()
	ods_conn.close()
	repodata_cursor.close()
	repodata_conn.close()

	logger.info(f"Proceso finalizado: {total_insertados} registros insertados.")

def AT13_IF_RECLAMOS(**kwargs):
	repodata_hook = PostgresHook(postgres_conn_id='repodataprd')
	ods_hook = PostgresHook(postgres_conn_id='ods')

	# CREAR TABLA DESTINO
	sql_query_deftxt = ''' 
	CREATE TABLE IF NOT EXISTS AT_STG.AT13_IF_RECLAMOS(
		SR_SECUENCIAL      NUMERIC(10),
		SR_TIPO_RCL        VARCHAR(10),
		SR_INT1            NUMERIC(10),
		SR_CEDULA          VARCHAR(20),
		SR_NOMBRE          VARCHAR(100),
		SR_FECHA           DATE,
		SR_VALOR_RECLAMO   NUMERIC(15,2),
		SR_CUENTA          VARCHAR(30),
		SR_TARJETA         VARCHAR(30),
		SR_BANCO_DBT       VARCHAR(10),
		SR_FECHA_CIERRE    DATE,
		SR_VALOR_DBT       NUMERIC(15),
		SR_FECHA_NOTIF     VARCHAR(8),
		SR_INT3            NUMERIC(10),
		SR_CAJERO_DBT      VARCHAR(30),
		SR_OFICINA         NUMERIC(5),
		SR_CATEG           VARCHAR(5),
		SR_ESTADO          VARCHAR(4),
		SR_FECHACARGA      VARCHAR(50),
		SR_ESTATUS         NUMERIC(3),
		SR_COMISION        NUMERIC(15,2),
		SR_INTERES         NUMERIC(15,2),
		SR_COMERCIO        VARCHAR(150),
		SR_PRODUCTO		   VARCHAR(26)
	);'''
	repodata_hook.run(sql_query_deftxt)

	# Vaciamos la tabla (para no duplicar datos)
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_IF_RECLAMOS;'''
	repodata_hook.run(sql_query_deftxt)

	# Consulta en la BD ods
	select_sql = ''' SELECT DISTINCT
		SR_SECUENCIAL,
		SR_TIPO_RCL,
		SR_INT1,
		SR_CEDULA,
		SR_NOMBRE,
		SR_FECHA,
		SR_VALOR_RECLAMO,
		SR_CUENTA,
		SR_TARJETA,
		SR_BANCO_DBT,
		SR_FECHA_CIERRE,
		SR_VALOR_DBT,
		TO_CHAR(SR_FECHA_NOTIF, 'YYYYMMDD') AS SR_FECHA_NOTIF,
		SR_INT3,
		SR_CAJERO_DBT,
		SR_OFICINA,
		SR_CATEG,
		SR_ESTADO,
		FECHACARGA AS SR_FECHACARGA,
		SR_ESTATUS,
		SR_COMISION,
		SR_INTERES,
		SR_COMERCIO,
		SR_PRODUCTO
	FROM ODS.IF_SOL_RECLAMOS_GIR5_AT
	WHERE SR_FECHA >= TO_DATE('01/01/2018','MM/DD/YYYY') AND SR_ESTATUS NOT IN (54, 60); '''

	# conexion a bd ods
	ods_conn = ods_hook.get_conn()
	ods_cursor = ods_conn.cursor(name='fetch_cursor')  # Cursor server-side para no cargar todo en RAM
	ods_cursor.execute(select_sql)

	# INSERTAMOS DATOS
	sql_query_deftxt = '''
	INSERT INTO AT_STG.AT13_IF_RECLAMOS(
		SR_SECUENCIAL,
		SR_TIPO_RCL,
		SR_INT1,
		SR_CEDULA,
		SR_NOMBRE,
		SR_FECHA,
		SR_VALOR_RECLAMO,
		SR_CUENTA,
		SR_TARJETA,
		SR_BANCO_DBT,
		SR_FECHA_CIERRE,
		SR_VALOR_DBT,
		SR_FECHA_NOTIF,
		SR_INT3,
		SR_CAJERO_DBT,
		SR_OFICINA,
		SR_CATEG,
		SR_ESTADO,
		SR_FECHACARGA,
		SR_ESTATUS,
		SR_COMISION,
		SR_INTERES,
		SR_COMERCIO,
		SR_PRODUCTO
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
		execute_values(repodata_cursor, sql_query_deftxt, rows)
		repodata_conn.commit()

		total_insertados += len(rows)
		logger.info(f"Lote {batch_num}: insertados {len(rows)} registros (Total acumulado: {total_insertados})")

	# cerrar conexiones
	ods_cursor.close()
	ods_conn.close()
	repodata_cursor.close()
	repodata_conn.close()

	logger.info(f"Proceso finalizado: {total_insertados} registros insertados.")


def AT13_FASE_REC(**kwargs):

	hook = PostgresHook(postgres_conn_id='repodataprd')

	# Recuperar las variables definidas en las tareas previas
	FechaInicio = get_variable('FechaInicio')
	FechaFin = get_variable('FechaFin')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = ''' 
	CREATE TABLE IF NOT EXISTS AT_STG.AT13_FASE_REC(
		HI_SECUENCIAL      NUMERIC(10),
		HI_TIPO_RCL        VARCHAR(10),
		HI_INT1            NUMERIC(10),
		HI_CEDULA          VARCHAR(20),
		HI_NOMBRE          VARCHAR(100),
		HI_FECHA           DATE,
		HI_VALOR_RECLAMO   NUMERIC(15,2),
		HI_CUENTA          VARCHAR(30),
		HI_TARJETA         VARCHAR(30),
		HI_BANCO_DBT       VARCHAR(10),
		HI_FECHA_CIERRE    DATE,
		HI_VALOR_DBT       NUMERIC(15),
		HI_FECHA_NOTIF     VARCHAR(8),
		HI_INT3            NUMERIC(10),
		HI_CAJERO_DBT      VARCHAR(30),
		HI_OFICINA         NUMERIC(5),
		HI_CATEG           VARCHAR(5),
		HI_ESTADO          VARCHAR(4),
		HI_ESTATUS         NUMERIC(3),
		HI_FECHACARGA      VARCHAR(50),
		SR_COMISION        NUMERIC(15,2),
		SR_INTERES         NUMERIC(15,2),
		SR_COMERCIO        VARCHAR(150)
	);'''
	hook.run(sql_query_deftxt)

	# Vaciamos la tabla (para no duplicar datos)
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_FASE_REC;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f''' 
	INSERT INTO AT_STG.AT13_FASE_REC(
		HI_SECUENCIAL,
		HI_TIPO_RCL,
		HI_INT1,
		HI_CEDULA,
		HI_NOMBRE,
		HI_FECHA,
		HI_VALOR_RECLAMO,
		HI_CUENTA,
		HI_TARJETA,
		HI_BANCO_DBT,
		HI_FECHA_CIERRE,
		HI_VALOR_DBT,
		HI_FECHA_NOTIF,
		HI_INT3,
		HI_CAJERO_DBT,
		HI_OFICINA,
		HI_CATEG,
		HI_ESTADO,
		HI_ESTATUS,
		HI_FECHACARGA,
		SR_COMISION,
		SR_INTERES,
		SR_COMERCIO 
	)
	SELECT
		AT13_DETRECL.SR_SECUENCIAL AS HI_SECUENCIAL,
		AT13_IF_RECLAMOS.SR_TIPO_RCL AS HI_TIPO_RCL,
		AT13_IF_RECLAMOS.SR_INT1 AS HI_INT1,
		AT13_IF_RECLAMOS.SR_CEDULA AS HI_CEDULA,
		AT13_IF_RECLAMOS.SR_NOMBRE AS HI_NOMBRE,
		AT13_IF_RECLAMOS.SR_FECHA AS HI_FECHA,
		AT13_IF_RECLAMOS.SR_VALOR_RECLAMO AS HI_VALOR_RECLAMO,
		AT13_IF_RECLAMOS.SR_CUENTA AS HI_CUENTA,
		AT13_IF_RECLAMOS.SR_TARJETA AS HI_TARJETA,
		AT13_IF_RECLAMOS.SR_BANCO_DBT AS HI_BANCO_DBT,
		AT13_IF_RECLAMOS.SR_FECHA_CIERRE AS HI_FECHA_CIERRE,
		AT13_IF_RECLAMOS.SR_VALOR_DBT AS HI_VALOR_DBT,
		AT13_IF_RECLAMOS.SR_FECHA_NOTIF AS HI_FECHA_NOTIF,
		AT13_IF_RECLAMOS.SR_INT3 AS HI_INT3,
		AT13_IF_RECLAMOS.SR_CAJERO_DBT AS HI_CAJERO_DBT,
		AT13_IF_RECLAMOS.SR_OFICINA AS HI_OFICINA,
		AT13_IF_RECLAMOS.SR_CATEG AS HI_CATEG,
		AT13_IF_RECLAMOS.SR_ESTADO AS HI_ESTADO,
		AT13_IF_RECLAMOS.SR_ESTATUS AS HI_ESTATUS,
		AT13_IF_RECLAMOS.SR_FECHACARGA AS HI_FECHACARGA,
		AT13_IF_RECLAMOS.SR_COMISION AS SR_COMISION,
		AT13_IF_RECLAMOS.SR_INTERES AS SR_INTERES,
		AT13_IF_RECLAMOS.SR_COMERCIO AS SR_COMERCIO
	FROM AT_STG.AT13_DETRECL AS AT13_DETRECL INNER JOIN AT_STG.AT13_IF_RECLAMOS AS AT13_IF_RECLAMOS
  		ON (AT13_DETRECL.SR_SECUENCIAL = AT13_IF_RECLAMOS.SR_SECUENCIAL
		AND CONCAT(SUBSTRING(TO_CHAR(AT13_DETRECL.SR_FECHACARGA, 'DDMMYY'), 1, 2),SUBSTRING(TO_CHAR(AT13_DETRECL.SR_FECHACARGA, 'DDMMYY'), 3, 2)) = AT13_IF_RECLAMOS.SR_FECHACARGA)
	WHERE 
		CAST(AT13_IF_RECLAMOS.SR_FECHA AS DATE) BETWEEN CAST('{FechaInicio}' AS DATE) AND CAST('{FechaFin}' AS DATE);
	'''
	hook.run(sql_query_deftxt)

def AT13_FASE_RECC(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# Recuperar las variables definidas en las tareas previas
	FechaInicio = get_variable('FechaInicio')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = ''' 
	CREATE TABLE IF NOT EXISTS AT_STG.AT13_FASE_RECC(
		HI_SECUENCIAL      NUMERIC(10),
		HI_TIPO_RCL        VARCHAR(10),
		HI_INT1            NUMERIC(10),
		HI_CEDULA          VARCHAR(20),
		HI_NOMBRE          VARCHAR(100),
		HI_FECHA           DATE,
		HI_VALOR_RECLAMO   NUMERIC(15,2),
		HI_CUENTA          VARCHAR(30),
		HI_TARJETA         VARCHAR(30),
		HI_BANCO_DBT       VARCHAR(10),
		HI_FECHA_CIERRE    DATE,
		HI_VALOR_DBT       NUMERIC(15),
		HI_FECHA_NOTIF     VARCHAR(8),
		HI_INT3            NUMERIC(10),
		HI_CAJERO_DBT      VARCHAR(30),
		HI_OFICINA         NUMERIC(5),
		HI_CATEG           VARCHAR(5),
		HI_ESTADO          VARCHAR(4),
		HI_ESTATUS         NUMERIC(3),
		HI_FECHACARGA      VARCHAR(50),
		SR_COMISION        NUMERIC(15,2),
		SR_INTERES         NUMERIC(15,2),
		SR_COMERCIO        VARCHAR(150)
	);'''
	hook.run(sql_query_deftxt)

	# Vaciamos la tabla (para no duplicar datos)
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_FASE_RECC;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.AT13_FASE_RECC (
	HI_SECUENCIAL,
	HI_TIPO_RCL,
	HI_INT1,
	HI_CEDULA,
	HI_NOMBRE,
	HI_FECHA,
	HI_VALOR_RECLAMO,
	HI_CUENTA,
	HI_TARJETA,
	HI_BANCO_DBT,
	HI_FECHA_CIERRE,
	HI_VALOR_DBT,
	HI_FECHA_NOTIF,
	HI_INT3,
	HI_CAJERO_DBT,
	HI_OFICINA,
	HI_CATEG,
	HI_ESTADO,
	HI_ESTATUS,
	HI_FECHACARGA,
	SR_COMISION,
	SR_INTERES,
	SR_COMERCIO
	)
	SELECT
	AT13_DETRECL.SR_SECUENCIAL AS HI_SECUENCIAL,
	AT13_IF_RECLAMOS.SR_TIPO_RCL AS HI_TIPO_RCL,
	AT13_IF_RECLAMOS.SR_INT1 AS HI_INT1,
	AT13_IF_RECLAMOS.SR_CEDULA AS HI_CEDULA,
	AT13_IF_RECLAMOS.SR_NOMBRE AS HI_NOMBRE,
	AT13_IF_RECLAMOS.SR_FECHA AS HI_FECHA,
	AT13_IF_RECLAMOS.SR_VALOR_RECLAMO AS HI_VALOR_RECLAMO,
	AT13_IF_RECLAMOS.SR_CUENTA AS HI_CUENTA,
	AT13_IF_RECLAMOS.SR_TARJETA AS HI_TARJETA,
	AT13_IF_RECLAMOS.SR_BANCO_DBT AS HI_BANCO_DBT,
	AT13_IF_RECLAMOS.SR_FECHA_CIERRE AS HI_FECHA_CIERRE,
	AT13_IF_RECLAMOS.SR_VALOR_DBT AS HI_VALOR_DBT,
	AT13_IF_RECLAMOS.SR_FECHA_NOTIF AS HI_FECHA_NOTIF,
	AT13_IF_RECLAMOS.SR_INT3 AS HI_INT3,
	AT13_IF_RECLAMOS.SR_CAJERO_DBT AS HI_CAJERO_DBT,
	AT13_IF_RECLAMOS.SR_OFICINA AS HI_OFICINA,
	AT13_IF_RECLAMOS.SR_CATEG AS HI_CATEG,
	AT13_IF_RECLAMOS.SR_ESTADO AS HI_ESTADO,
	AT13_IF_RECLAMOS.SR_ESTATUS AS HI_ESTATUS,
	AT13_IF_RECLAMOS.SR_FECHACARGA AS HI_FECHACARGA,
	AT13_IF_RECLAMOS.SR_COMISION AS SR_COMISION,
	AT13_IF_RECLAMOS.SR_INTERES AS SR_INTERES,
	AT13_IF_RECLAMOS.SR_COMERCIO AS SR_COMERCIO
	FROM AT_STG.AT13_DETRECL AS AT13_DETRECL INNER JOIN AT_STG.AT13_IF_RECLAMOS AS AT13_IF_RECLAMOS
		ON (AT13_DETRECL.SR_SECUENCIAL = AT13_IF_RECLAMOS.SR_SECUENCIAL
		AND CONCAT(SUBSTRING(TO_CHAR(AT13_DETRECL.SR_FECHACARGA, 'DDMMYY'), 1, 2),SUBSTRING(TO_CHAR(AT13_DETRECL.SR_FECHACARGA, 'DDMMYY'), 3, 2)) = AT13_IF_RECLAMOS.SR_FECHACARGA)
	WHERE
		(CAST(AT13_IF_RECLAMOS.SR_FECHA AS DATE) < CAST('{FechaInicio}' AS DATE))
		AND (CAST(AT13_IF_RECLAMOS.SR_FECHA_CIERRE AS DATE) >= CAST('{FechaInicio}' AS DATE));'''
	hook.run(sql_query_deftxt)

def AT13_FASE_RECA(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# Recuperar las variables definidas en las tareas previas
	FechaInicio = get_variable('FechaInicio')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = ''' 
	CREATE TABLE IF NOT EXISTS AT_STG.AT13_FASE_RECA(
		HI_SECUENCIAL      NUMERIC(10),
		HI_TIPO_RCL        VARCHAR(10),
		HI_INT1            NUMERIC(10),
		HI_CEDULA          VARCHAR(20),
		HI_NOMBRE          VARCHAR(100),
		HI_FECHA           DATE,
		HI_VALOR_RECLAMO   NUMERIC(15,2),
		HI_CUENTA          VARCHAR(30),
		HI_TARJETA         VARCHAR(30),
		HI_BANCO_DBT       VARCHAR(10),
		HI_FECHA_CIERRE    DATE,
		HI_VALOR_DBT       NUMERIC(15),
		HI_FECHA_NOTIF     VARCHAR(8),
		HI_INT3            NUMERIC(10),
		HI_CAJERO_DBT      VARCHAR(30),
		HI_OFICINA         NUMERIC(5),
		HI_CATEG           VARCHAR(5),
		HI_ESTADO          VARCHAR(4),
		HI_ESTATUS         NUMERIC(3),
		HI_FECHACARGA      VARCHAR(50),
		SR_COMISION        NUMERIC(15,2),
		SR_INTERES         NUMERIC(15,2),
		SR_COMERCIO        VARCHAR(150)
	);'''
	hook.run(sql_query_deftxt)

	# Vaciamos la tabla (para no duplicar datos)
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_FASE_RECA;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO AT_STG.AT13_FASE_RECA (
	HI_SECUENCIAL,
	HI_TIPO_RCL,
	HI_INT1,
	HI_CEDULA,
	HI_NOMBRE,
	HI_FECHA,
	HI_VALOR_RECLAMO,
	HI_CUENTA,
	HI_TARJETA,
	HI_BANCO_DBT,
	HI_FECHA_CIERRE,
	HI_VALOR_DBT,
	HI_FECHA_NOTIF,
	HI_INT3,
	HI_CAJERO_DBT,
	HI_OFICINA,
	HI_CATEG,
	HI_ESTADO,
	HI_ESTATUS,
	HI_FECHACARGA,
	SR_COMISION,
	SR_INTERES,
	SR_COMERCIO
	)
	SELECT DISTINCT
	AT13_IF_RECLAMOS.SR_SECUENCIAL AS HI_SECUENCIAL,
	AT13_IF_RECLAMOS.SR_TIPO_RCL AS HI_TIPO_RCL,
	AT13_IF_RECLAMOS.SR_INT1 AS HI_INT1,
	AT13_IF_RECLAMOS.SR_CEDULA AS HI_CEDULA,
	AT13_IF_RECLAMOS.SR_NOMBRE AS HI_NOMBRE,
	AT13_IF_RECLAMOS.SR_FECHA AS HI_FECHA,
	AT13_IF_RECLAMOS.SR_VALOR_RECLAMO AS HI_VALOR_RECLAMO,
	AT13_IF_RECLAMOS.SR_CUENTA AS HI_CUENTA,
	AT13_IF_RECLAMOS.SR_TARJETA AS HI_TARJETA,
	AT13_IF_RECLAMOS.SR_BANCO_DBT AS HI_BANCO_DBT,
	AT13_IF_RECLAMOS.SR_FECHA_CIERRE AS HI_FECHA_CIERRE,
	AT13_IF_RECLAMOS.SR_VALOR_DBT AS HI_VALOR_DBT,
	AT13_IF_RECLAMOS.SR_FECHA_NOTIF AS HI_FECHA_NOTIF,
	AT13_IF_RECLAMOS.SR_INT3 AS HI_INT3,
	AT13_IF_RECLAMOS.SR_CAJERO_DBT AS HI_CAJERO_DBT,
	AT13_IF_RECLAMOS.SR_OFICINA AS HI_OFICINA,
	AT13_IF_RECLAMOS.SR_CATEG AS HI_CATEG,
	AT13_IF_RECLAMOS.SR_ESTADO AS HI_ESTADO,
	AT13_IF_RECLAMOS.SR_ESTATUS AS HI_ESTATUS,
	AT13_IF_RECLAMOS.SR_FECHACARGA AS HI_FECHACARGA,
	AT13_IF_RECLAMOS.SR_COMISION AS SR_COMISION,
	AT13_IF_RECLAMOS.SR_INTERES AS SR_INTERES,
	AT13_IF_RECLAMOS.SR_COMERCIO AS SR_COMERCIO
	FROM AT_STG.AT13_DETRECL AS AT13_DETRECL INNER JOIN AT_STG.AT13_IF_RECLAMOS AS AT13_IF_RECLAMOS
		ON (CONCAT(SUBSTRING(TO_CHAR(AT13_DETRECL.SR_FECHACARGA,'DDMMYY'), 1, 2), SUBSTRING(TO_CHAR(AT13_DETRECL.SR_FECHACARGA,'DDMMYY'), 3, 2)) = AT13_IF_RECLAMOS.SR_FECHACARGA)
		AND (AT13_DETRECL.SR_SECUENCIAL = AT13_IF_RECLAMOS.SR_SECUENCIAL)
	WHERE CAST(AT13_IF_RECLAMOS.SR_FECHA AS DATE) < CAST('{FechaInicio}' AS DATE) AND AT13_IF_RECLAMOS.SR_ESTADO = 'V';'''
	hook.run(sql_query_deftxt)

def AT13_UNION(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# Borramos tablas anteriores
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT13_DETRECL;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT13_IF_RECLAMOS;'''
	#hook.run(sql_query)

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT13_UNION (
	SR_SECUENCIAL	NUMERIC(10),
	SR_TIPO_RCL	VARCHAR(10),
	SR_INT1	    NUMERIC(10),
	SR_CEDULA	VARCHAR(20),
	SR_NOMBRE	VARCHAR(100),
	SR_FECHA	DATE,
	SR_VALOR_RECLAMO	NUMERIC(15,2),
	SR_CUENTA	VARCHAR(30),
	SR_TARJETA	VARCHAR(30),
	SR_BANCO_DBT	VARCHAR(10),
	SR_FECHA_CIERRE	DATE,
	SR_VALOR_DBT	NUMERIC(15),
	SR_FECHA_NOTIF	VARCHAR(8),
	SR_INT3	NUMERIC(8),
	SR_CAJERO_DBT	VARCHAR(30),
	SR_OFICINA	NUMERIC(5),
	SR_CATEG	VARCHAR(5),
	SR_ESTADO	VARCHAR(4),
	SR_FECHACARGA	VARCHAR(50),
	SR_ESTATUS	NUMERIC(3),
	SR_COMISION	NUMERIC(15,2),
	SR_INTERES	NUMERIC(15,2),
	SR_COMERCIO	VARCHAR(150)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_UNION'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at13_union (
	sr_secuencial,
	sr_tipo_rcl,
	sr_int1,
	sr_cedula,
	sr_nombre,
	sr_fecha,
	sr_valor_reclamo,
	sr_cuenta,
	sr_tarjeta,
	sr_banco_dbt,
	sr_fecha_cierre,
	sr_valor_dbt,
	sr_fecha_notif,
	sr_int3,
	sr_cajero_dbt,
	sr_oficina,
	sr_categ,
	sr_estado,
	sr_fechacarga,
	sr_estatus,
	sr_comision,
	sr_interes,
	sr_comercio
	)
	SELECT DISTINCT
		at13_fase_rec.hi_secuencial AS sr_secuencial,
		at13_fase_rec.hi_tipo_rcl AS sr_tipo_rcl,
		at13_fase_rec.hi_int1 AS sr_int1,
		at13_fase_rec.hi_cedula AS sr_cedula,
		at13_fase_rec.hi_nombre AS sr_nombre,
		at13_fase_rec.hi_fecha AS sr_fecha,
		at13_fase_rec.hi_valor_reclamo AS sr_valor_reclamo,
		at13_fase_rec.hi_cuenta AS sr_cuenta,
		at13_fase_rec.hi_tarjeta AS sr_tarjeta,
		at13_fase_rec.hi_banco_dbt AS sr_banco_dbt,
		at13_fase_rec.hi_fecha_cierre AS sr_fecha_cierre,
		at13_fase_rec.hi_valor_dbt AS sr_valor_dbt,
		at13_fase_rec.hi_fecha_notif AS sr_fecha_notif,
		at13_fase_rec.hi_int3 AS sr_int3,
		at13_fase_rec.hi_cajero_dbt AS sr_cajero_dbt,
		at13_fase_rec.hi_oficina AS sr_oficina,
		at13_fase_rec.hi_categ AS sr_categ,
		at13_fase_rec.hi_estado AS sr_estado,
		at13_fase_rec.hi_fechacarga AS sr_fechacarga,
		at13_fase_rec.hi_estatus AS sr_estatus,
		at13_fase_rec.sr_comision AS sr_comision,
		at13_fase_rec.sr_interes AS sr_interes,
		at13_fase_rec.sr_comercio AS sr_comercio
	FROM at_stg.at13_fase_rec

	UNION

	SELECT DISTINCT
		at13_fase_recc.hi_secuencial AS sr_secuencial,
		at13_fase_recc.hi_tipo_rcl AS sr_tipo_rcl,
		at13_fase_recc.hi_int1 AS sr_int1,
		at13_fase_recc.hi_cedula AS sr_cedula,
		at13_fase_recc.hi_nombre AS sr_nombre,
		at13_fase_recc.hi_fecha AS sr_fecha,
		at13_fase_recc.hi_valor_reclamo AS sr_valor_reclamo,
		at13_fase_recc.hi_cuenta AS sr_cuenta,
		at13_fase_recc.hi_tarjeta AS sr_tarjeta,
		at13_fase_recc.hi_banco_dbt AS sr_banco_dbt,
		at13_fase_recc.hi_fecha_cierre AS sr_fecha_cierre,
		at13_fase_recc.hi_valor_dbt AS sr_valor_dbt,
		at13_fase_recc.hi_fecha_notif AS sr_fecha_notif,
		at13_fase_recc.hi_int3 AS sr_int3,
		at13_fase_recc.hi_cajero_dbt AS sr_cajero_dbt,
		at13_fase_recc.hi_oficina AS sr_oficina,
		at13_fase_recc.hi_categ AS sr_categ,
		at13_fase_recc.hi_estado AS sr_estado,
		at13_fase_recc.hi_fechacarga AS sr_fechacarga,
		at13_fase_recc.hi_estatus AS sr_estatus,
		at13_fase_recc.sr_comision AS sr_comision,
		at13_fase_recc.sr_interes AS sr_interes,
		at13_fase_recc.sr_comercio AS sr_comercio
	FROM at_stg.at13_fase_recc

	UNION

	SELECT DISTINCT
		at13_fase_reca.hi_secuencial AS sr_secuencial,
		at13_fase_reca.hi_tipo_rcl AS sr_tipo_rcl,
		at13_fase_reca.hi_int1 AS sr_int1,
		at13_fase_reca.hi_cedula AS sr_cedula,
		at13_fase_reca.hi_nombre AS sr_nombre,
		at13_fase_reca.hi_fecha AS sr_fecha,
		at13_fase_reca.hi_valor_reclamo AS sr_valor_reclamo,
		at13_fase_reca.hi_cuenta AS sr_cuenta,
		at13_fase_reca.hi_tarjeta AS sr_tarjeta,
		at13_fase_reca.hi_banco_dbt AS sr_banco_dbt,
		at13_fase_reca.hi_fecha_cierre AS sr_fecha_cierre,
		at13_fase_reca.hi_valor_dbt AS sr_valor_dbt,
		at13_fase_reca.hi_fecha_notif AS sr_fecha_notif,
		at13_fase_reca.hi_int3 AS sr_int3,
		at13_fase_reca.hi_cajero_dbt AS sr_cajero_dbt,
		at13_fase_reca.hi_oficina AS sr_oficina,
		at13_fase_reca.hi_categ AS sr_categ,
		at13_fase_reca.hi_estado AS sr_estado,
		at13_fase_reca.hi_fechacarga AS sr_fechacarga,
		at13_fase_reca.hi_estatus AS sr_estatus,
		at13_fase_reca.sr_comision AS sr_comision,
		at13_fase_reca.sr_interes AS sr_interes,
		at13_fase_reca.sr_comercio AS sr_comercio
	FROM at_stg.at13_fase_reca;'''
	hook.run(sql_query_deftxt)

	# Borramos tablas anteriores
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT13_FASE_REC;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT13_FASE_RECC;'''
	#hook.run(sql_query)
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT13_FASE_RECA;'''
	#hook.run(sql_query)

def AT13_UNION_BC(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT13_UNION_BC (
	SR_SECUENCIAL	NUMERIC(10),
	SR_TIPO_RCL	VARCHAR(10),
	SR_INT1	NUMERIC(10),
	SR_CEDULA	VARCHAR(20),
	SR_NOMBRE	VARCHAR(100),
	SR_FECHA	DATE,
	SR_VALOR_RECLAMO	NUMERIC(15,2),
	SR_CUENTA	VARCHAR(30),
	SR_TARJETA	VARCHAR(30),
	SR_BANCO_DBT	VARCHAR(10),
	SR_FECHA_CIERRE	DATE,
	SR_VALOR_DBT	NUMERIC(15),
	SR_FECHA_NOTIF	VARCHAR(8),
	SR_INT3	NUMERIC(10),
	SR_CAJERO_DBT	VARCHAR(30),
	SR_OFICINA	NUMERIC(5),
	SR_CATEG	VARCHAR(5),
	SR_ESTADO	VARCHAR(4),
	SR_FECHACARGA	VARCHAR(50),
	SR_ESTATUS	NUMERIC(3),
	SR_COMISION	NUMERIC(15,2),
	SR_INTERES	NUMERIC(15,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_UNION_BC;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at13_union_bc (
	sr_secuencial,
	sr_tipo_rcl,
	sr_int1,
	sr_cedula,
	sr_nombre,
	sr_fecha,
	sr_valor_reclamo,
	sr_cuenta,
	sr_tarjeta,
	sr_banco_dbt,
	sr_fecha_cierre,
	sr_valor_dbt,
	sr_fecha_notif,
	sr_int3,
	sr_cajero_dbt,
	sr_oficina,
	sr_categ,
	sr_estado,
	sr_fechacarga,
	sr_estatus,
	sr_comision,
	sr_interes
	)
	SELECT
		sr_secuencial,
		sr_tipo_rcl,
		sr_int1,
		sr_cedula,
		sr_nombre,
		sr_fecha,
		sr_valor_reclamo,
		sr_cuenta,
		sr_tarjeta,
		sr_banco_dbt,
		sr_fecha_cierre,
		sr_valor_dbt,
		sr_fecha_notif,
		sr_int3,
		sr_cajero_dbt,
		sr_oficina,
		sr_categ,
		sr_estado,
		sr_fechacarga,
		sr_estatus,
		sr_comision,
		sr_interes
	FROM AT_STG.AT13_UNION 
	WHERE (SUBSTRING(at13_union.sr_cuenta,1,4) <> '0146'); '''
	hook.run(sql_query_deftxt)


def AT13_ATS(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT13_ATS (
	RECLAMO	VARCHAR(20),
	OFICINA	VARCHAR(10),
	TIPOCLIENTE	VARCHAR(50),
	IDCLIENTE	VARCHAR(50),
	NOMBRECLIENTE	VARCHAR(255),
	FECHARECLAMO	VARCHAR(8),
	TIPOINSTRUMENTO	VARCHAR(10),
	TIPORECLAMO	VARCHAR(150),
	MONTORECLAMO	NUMERIC(15,2),
	CODIGOCUENTA	VARCHAR(24),
	CREDITOAFECTADO	VARCHAR(20),
	TARJETAAFECTADA	VARCHAR(24),
	ENTESUPERVISADO	VARCHAR(10),
	ESTADORECLAMO	VARCHAR(50),
	FECHASOLUCION	VARCHAR(8),
	MONTOREINTEGRO	NUMERIC(15,2),
	FECHAREINTEGRO	VARCHAR(8),
	FECHANOTIFICACION	VARCHAR(8),
	CODIGOENTE	VARCHAR(20),
	IDPOS	VARCHAR(30),
	SISTEMA	VARCHAR(50),
	OFICINARECLAMO	VARCHAR(50),
	TIPOOPERACION	VARCHAR(50),
	CANAL	VARCHAR(50),
	FRANQUICIA	VARCHAR(50),
	RED	VARCHAR(50),
	TIPOCLIENTEDESTINO	VARCHAR(50),
	IDCLIENTEDESTINO	VARCHAR(50),
	NOMBRECLIENTEDESTINO	VARCHAR(50),
	NROCUENTADESTINO	NUMERIC(15),
	CODIGOCUENTADESTINO	VARCHAR(20),
	MONTODESTINO	NUMERIC(20,2),
	HICATEG_TIPORECL	VARCHAR(255)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_ATS;'''
	hook.run(sql_query_deftxt)

	sql_query_deftxt = '''INSERT INTO at_stg.at13_ats (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO,
	HICATEG_TIPORECL
	)
	SELECT DISTINCT
		u.SR_SECUENCIAL AS RECLAMO,
		u.SR_INT3 AS OFICINA,
		CASE
			WHEN u.SR_CEDULA IS NULL THEN 'P' ELSE SUBSTRING(TRIM(u.SR_CEDULA), 1, 1) END AS TIPOCLIENTE,
		SUBSTRING(u.SR_CEDULA, 2, LENGTH(u.SR_CEDULA)) AS IDCLIENTE,
		u.SR_NOMBRE AS NOMBRECLIENTE,
		TO_CHAR(u.SR_FECHA, 'YYYYMMDD') AS FECHARECLAMO,
		h.INSTRUMENTO AS TIPOINSTRUMENTO,
		h.SUDEBAN AS TIPORECLAMO,
		u.SR_VALOR_RECLAMO AS MONTORECLAMO,
		CASE
			WHEN h.INSTRUMENTO IN ('30', '50') THEN ''
			WHEN (u.SR_TIPO_RCL = '01' AND u.SR_CATEG = '6' AND u.SR_CUENTA IN ('00000000000000000000', 'N/P', 'N/A', NULL)) THEN u.SR_COMERCIO
			ELSE TRIM(u.SR_CUENTA)
		END AS CODIGOCUENTA,
		NULL AS CREDITOAFECTADO,
		CASE
			WHEN (u.SR_CATEG = '20' AND u.SR_TIPO_RCL = '41' AND u.SR_TARJETA IN (NULL, 'N/P', 'N/A', 'NA', 'NP', 'N')) THEN u.SR_COMERCIO
			ELSE u.SR_TARJETA
		END AS TARJETAAFECTADA,
		u.SR_BANCO_DBT AS ENTESUPERVISADO,
		CASE
			WHEN u.SR_ESTATUS IN (1, 2, 3, 4, 52, 55) THEN '1'
			WHEN u.SR_ESTATUS IN (20, 22, 30, 50) THEN '2'
			ELSE '3'
		END AS ESTADORECLAMO,
		TO_CHAR(u.SR_FECHA_CIERRE, 'YYYYMMDD') AS FECHASOLUCION,
		u.SR_VALOR_RECLAMO + COALESCE(u.SR_INTERES, 0) + COALESCE(u.SR_COMISION, 0) AS MONTOREINTEGRO,
		TO_CHAR(u.SR_FECHA_CIERRE, 'YYYYMMDD') AS FECHAREINTEGRO,
		u.SR_FECHA_NOTIF AS FECHANOTIFICACION,
		u.SR_BANCO_DBT AS CODIGOENTE,
		u.SR_CAJERO_DBT AS IDPOS,
		NULL AS SISTEMA,
		u.SR_INT1 AS OFICINARECLAMO,
		h.OPERACION AS TIPOOPERACION,
		h.CANAL AS CANAL,
		h.FRANQUICIA AS FRANQUICIA,
		h.RED AS RED,
		NULL AS TIPOCLIENTEDESTINO,
		NULL AS IDCLIENTEDESTINO,
		NULL AS NOMBRECLIENTEDESTINO,
		0 AS NROCUENTADESTINO,
		NULL AS CODIGOCUENTADESTINO,
		0 AS MONTODESTINO,
		h.DESCRIPCION AS HICATEG_TIPORECL
	FROM AT_STG.AT13_UNION AS u INNER JOIN ATSUDEBAN.HOMOLOGACIONES_SUDEBAN_RECFRA AS h
		ON CAST(u.SR_TIPO_RCL AS INTEGER) = CAST(h.RECLAMO AS INTEGER) AND CAST(u.SR_CATEG AS INTEGER) = CAST(h.CATEGORIA AS INTEGER);'''
	hook.run(sql_query_deftxt)

	# Borramos tablas anteriores
	#sql_query = '''DROP TABLE IF EXISTS AT_STG.AT13_UNION;'''
	#hook.run(sql_query)

def IMPORTAR_INSUMO(**kwargs):
    try:
        gcs_bucket = 'airflow-dags-data'
        gcs_object = 'data/AT13/INSUMOS/TRANS_PROCESADAS.csv'

        postgres_hook = PostgresHook(postgres_conn_id='repodataprd')
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

        # 1. Validar conexion a PostgreSQL
        try:
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            logger.info("Conexion a PostgreSQL validada exitosamente")
        except Exception as e:
            logger.error(f"Error al conectar a PostgreSQL: {str(e)}")
            raise

        # 2. Validar acceso a GCS
        try:
            if not gcs_hook.exists(bucket_name=gcs_bucket, object_name=gcs_object):
                raise Exception(f"El archivo {gcs_object} no existe en el bucket {gcs_bucket}")
            logger.info("Validacion de archivo GCS exitosa")
        except Exception as e:
            logger.error(f"Error al validar archivo en GCS: {str(e)}")
            raise

        # 3. Truncar tabla
        logger.info("Truncando la tabla AT_STG.AT13_TRANSACCIONES_FILE...")
        postgres_hook.run("TRUNCATE TABLE AT_STG.AT13_TRANSACCIONES_FILE;")
        logger.info("Tabla truncada exitosamente.")

        # 4. Preparar consulta COPY
        sql_copy = """
        COPY AT_STG.AT13_TRANSACCIONES_FILE (
            N_GESTION,
            RECLAMO,
            CEDULA,
            NOMBRE,
            CUENTA,
            FECHA,
            HORA,
            MONTO
        )
        FROM STDIN
        WITH (FORMAT csv, HEADER false, DELIMITER ';', ENCODING 'UTF8');
        """

        # 5. Descargar archivo
        temp_dir = tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, 'TRANS_PROCESADAS.csv')

        try:
            logger.info(f"Descargando archivo '{gcs_object}' desde GCS...")
            gcs_hook.download(
                bucket_name=gcs_bucket,
                object_name=gcs_object,
                filename=local_file_path
            )

            # 6. Reconvertir el archivo a UTF-8 (estaba en latin1/win1252)
            logger.info("Convirtiendo archivo a UTF-8...")
            with open(local_file_path, 'r', encoding='latin1') as f:
                content = f.read()
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info("Archivo convertido a UTF-8 correctamente.")

            # 7. Ejecutar COPY
            logger.info("Iniciando carga de datos a PostgreSQL...")
            start_time = datetime.now()

            postgres_hook.copy_expert(sql=sql_copy, filename=local_file_path)

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Carga completada en {duration:.2f} segundos")

            # 8. Verificar registros
            count = postgres_hook.get_first("SELECT COUNT(*) FROM AT_STG.AT13_TRANSACCIONES_FILE;")[0]
            logger.info(f"Total de registros cargados: {count}")

            if count == 0:
                raise Exception("No se cargaron registros - verificar formato del archivo CSV")

        except Exception as e:
            logger.error(f"Error durante la carga de datos: {str(e)}")
            import traceback
            logger.error("Traceback completo:\n" + traceback.format_exc())
            raise

        finally:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                logger.info(f"Archivo temporal eliminado: {local_file_path}")
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
                logger.info(f"Directorio temporal eliminado: {temp_dir}")

    except Exception as e:
        logger.error(f"Error general en IMPORTAR_INSUMO: {str(e)}")
        import traceback
        logger.error("Traceback completo:\n" + traceback.format_exc())
        raise

def AT13_TRANSACCIONES_(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT13_TRANSACCIONES (
	RECLAMO NUMERIC(15),
	TIPO_CLIENTE VARCHAR(1),
	CLIENTE VARCHAR(10),
	NOMBRE VARCHAR(150),
	CUENTA VARCHAR(50),
	FECHA VARCHAR(10),
	HORA VARCHAR(50),
	MONTO NUMERIC(15,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_TRANSACCIONES;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT13_TRANSACCIONES (
	RECLAMO,
	TIPO_CLIENTE,
	CLIENTE,
	NOMBRE,
	CUENTA,
	FECHA,
	HORA,
	MONTO
	)
	SELECT
		CAST(RECLAMO AS INTEGER) AS RECLAMO,
		SUBSTRING(CEDULA FROM 1 FOR 1) AS TIPO_CLIENTE,
		SUBSTRING(CEDULA FROM 2 FOR LENGTH(CEDULA)) AS CLIENTE,
		TRIM(NOMBRE) AS NOMBRE,
		CUENTA AS CUENTA,
		FECHA AS FECHA,
		HORA AS HORA,
		CAST(REPLACE(REPLACE(MONTO, '.', ''), ',', '.') AS DECIMAL) AS MONTO
	FROM AT_STG.AT13_TRANSACCIONES_FILE;'''
	hook.run(sql_query_deftxt)

def AT13_ATS_(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT13_ATS_ (
	RECLAMO	VARCHAR(20),
	OFICINA	VARCHAR(10),
	TIPOCLIENTE	VARCHAR(50),
	IDCLIENTE	VARCHAR(50),
	NOMBRECLIENTE	VARCHAR(255),
	FECHARECLAMO	VARCHAR(8),
	TIPOINSTRUMENTO	VARCHAR(7),
	TIPORECLAMO	VARCHAR(150),
	MONTORECLAMO	NUMERIC(15,2),
	CODIGOCUENTA	VARCHAR(24),
	CREDITOAFECTADO	VARCHAR(20),
	TARJETAAFECTADA	VARCHAR(24),
	ENTESUPERVISADO	VARCHAR(10),
	ESTADORECLAMO	VARCHAR(1),
	FECHASOLUCION	VARCHAR(8),
	MONTOREINTEGRO	NUMERIC(15,2),
	FECHAREINTEGRO	VARCHAR(8),
	FECHANOTIFICACION	VARCHAR(8),
	CODIGOENTE	VARCHAR(20),
	IDPOS	VARCHAR(30),
	SISTEMA	VARCHAR(50),
	OFICINARECLAMO	VARCHAR(50),
	TIPOOPERACION	VARCHAR(50),
	CANAL	VARCHAR(50),
	FRANQUICIA	VARCHAR(50),
	RED	VARCHAR(50),
	TIPOCLIENTEDESTINO	VARCHAR(50),
	IDCLIENTEDESTINO	VARCHAR(50),
	NOMBRECLIENTEDESTINO	VARCHAR(50),
	NROCUENTADESTINO	NUMERIC(15),
	CODIGOCUENTADESTINO	VARCHAR(20),
	MONTODESTINO	NUMERIC(20,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_ATS_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT13_ATS_ (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO
	)
	SELECT
		AT13_ATS.RECLAMO,
		AT13_ATS.OFICINA,
		AT13_ATS.TIPOCLIENTE,
		AT13_ATS.IDCLIENTE,
		AT13_ATS.NOMBRECLIENTE,
		AT13_ATS.FECHARECLAMO,
		AT13_ATS.TIPOINSTRUMENTO,
		AT13_ATS.TIPORECLAMO,
		AT13_ATS.MONTORECLAMO,
		AT13_ATS.CODIGOCUENTA,
		AT13_ATS.CREDITOAFECTADO,
		AT13_ATS.TARJETAAFECTADA,
		AT13_ATS.ENTESUPERVISADO,
		AT13_ATS.ESTADORECLAMO,
		AT13_ATS.FECHASOLUCION,
		AT13_ATS.MONTOREINTEGRO,
		AT13_ATS.FECHAREINTEGRO,
		AT13_ATS.FECHANOTIFICACION,
		AT13_ATS.CODIGOENTE,
		AT13_ATS.IDPOS,
		AT13_ATS.SISTEMA,
		AT13_ATS.OFICINARECLAMO,
		AT13_ATS.TIPOOPERACION,
		AT13_ATS.CANAL,
		AT13_ATS.FRANQUICIA,
		AT13_ATS.RED,
		CASE 
			WHEN AT13_ATS.TIPORECLAMO <> '13' THEN '0'
			ELSE AT13_TRANSACCIONES.TIPO_CLIENTE
		END AS TIPOCLIENTEDESTINO,
		CASE 
			WHEN AT13_ATS.TIPORECLAMO <> '13' THEN '0'
			ELSE AT13_TRANSACCIONES.CLIENTE
		END AS IDCLIENTEDESTINO,
		CASE 
			WHEN AT13_ATS.TIPORECLAMO <> '13' THEN '0'
			ELSE UPPER(AT13_TRANSACCIONES.NOMBRE)
		END AS NOMBRECLIENTEDESTINO,
		CASE 
			WHEN AT13_ATS.TIPORECLAMO <> '13' THEN 0
			ELSE 1
		END AS NROCUENTADESTINO,
		CASE 
			WHEN AT13_ATS.TIPORECLAMO <> '13' THEN '0'
			ELSE TRIM(AT13_TRANSACCIONES.CUENTA)
		END AS CODIGOCUENTADESTINO,
		CASE 
			WHEN AT13_ATS.TIPORECLAMO <> '13' THEN 0
			ELSE AT13_TRANSACCIONES.MONTO
		END AS MONTODESTINO 
	FROM AT_STG.AT13_ATS AS AT13_ATS LEFT OUTER JOIN AT_STG.AT13_TRANSACCIONES AS AT13_TRANSACCIONES ON (AT13_ATS.RECLAMO = AT13_TRANSACCIONES.RECLAMO::TEXT);'''
	hook.run(sql_query_deftxt)

def SP_ACTUALIZACION_31(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# PROCEDIMIENTO UPDATE
	sql_query_deftxt = '''UPDATE AT_STG.AT13_ATS_
	SET TIPOINSTRUMENTO = 
	CASE
		WHEN SUBSTRING(CODIGOCUENTA FROM 1 FOR 2) = '01' AND SUBSTRING(CODIGOCUENTA FROM 14 FOR 1) IN ('0','9','6','5','7') AND TIPOINSTRUMENTO = '31' AND TIPORECLAMO = '13' THEN '40'
		WHEN SUBSTRING(CODIGOCUENTA FROM 1 FOR 2) = '01' AND SUBSTRING(CODIGOCUENTA FROM 14 FOR 1) IN ('1','2','3','4','8') AND TIPOINSTRUMENTO = '31' AND TIPORECLAMO = '13' THEN '41'
		ELSE TIPOINSTRUMENTO
	END;'''
	hook.run(sql_query_deftxt)

def AT13_ATS_TT(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT13_ATS_TT (
	RECLAMO	VARCHAR(20),
	OFICINA	VARCHAR(10),
	TIPOCLIENTE	VARCHAR(50),
	IDCLIENTE	VARCHAR(50),
	NOMBRECLIENTE	VARCHAR(255),
	FECHARECLAMO	VARCHAR(8),
	TIPOINSTRUMENTO	VARCHAR(7),
	TIPORECLAMO	VARCHAR(150),
	MONTORECLAMO	NUMERIC(15,2),
	CODIGOCUENTA	VARCHAR(24),
	CREDITOAFECTADO	VARCHAR(20),
	TARJETAAFECTADA	VARCHAR(24),
	ENTESUPERVISADO	VARCHAR(10),
	ESTADORECLAMO	VARCHAR(1),
	FECHASOLUCION	VARCHAR(8),
	MONTOREINTEGRO	NUMERIC(15,2),
	FECHAREINTEGRO	VARCHAR(8),
	FECHANOTIFICACION	VARCHAR(8),
	CODIGOENTE	VARCHAR(20),
	IDPOS	VARCHAR(30),
	SISTEMA	VARCHAR(50),
	OFICINARECLAMO	VARCHAR(50),
	TIPOOPERACION	VARCHAR(50),
	CANAL	VARCHAR(50),
	FRANQUICIA	VARCHAR(50),
	RED	VARCHAR(50),
	TIPOCLIENTEDESTINO	VARCHAR(50),
	IDCLIENTEDESTINO	VARCHAR(50),
	NOMBRECLIENTEDESTINO	VARCHAR(50),
	NROCUENTADESTINO	NUMERIC(15),
	CODIGOCUENTADESTINO	VARCHAR(20),
	MONTODESTINO	NUMERIC(20,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT13_ATS_TT;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT13_ATS_TT (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO
	)
	SELECT
		AT13_ATS_.RECLAMO AS RECLAMO,
		CASE
			WHEN AT13_ATS_.OFICINA IS NULL THEN '147'
			WHEN AT13_ATS_.OFICINA IN ('804','805','806','801','802','803','807','407','401','905') THEN '147'
			WHEN (LENGTH(AT13_ATS_.OFICINA) > 3 OR LENGTH(AT13_ATS_.OFICINA) < 3) THEN '147'
			ELSE AT13_ATS_.OFICINA
		END AS OFICINA,
		AT13_ATS_.TIPOCLIENTE AS TIPOCLIENTE,
		AT13_ATS_.IDCLIENTE AS IDCLIENTE,
		AT13_ATS_.NOMBRECLIENTE AS NOMBRECLIENTE,
		AT13_ATS_.FECHARECLAMO AS FECHARECLAMO,
		CASE
			WHEN (SUBSTRING(AT13_ATS_.CODIGOCUENTA, 1, 2) = '01' AND SUBSTRING(AT13_ATS_.CODIGOCUENTA, 14, 1) IN ('0','9','6','5','7') AND AT13_ATS_.TIPOINSTRUMENTO = 'Definir') THEN '40'
			WHEN (SUBSTRING(AT13_ATS_.CODIGOCUENTA, 1, 2) = '01' AND SUBSTRING(AT13_ATS_.CODIGOCUENTA, 14, 1) IN ('1','2','3','4','8') AND AT13_ATS_.TIPOINSTRUMENTO = 'Definir') THEN '41'
			ELSE AT13_ATS_.TIPOINSTRUMENTO
		END AS TIPOINSTRUMENTO,
		AT13_ATS_.TIPORECLAMO AS TIPORECLAMO,
		AT13_ATS_.MONTORECLAMO AS MONTORECLAMO,
		CASE
			WHEN AT13_ATS_.CODIGOCUENTA = '00000000000000000000' THEN ''
			ELSE AT13_ATS_.CODIGOCUENTA
		END AS CODIGOCUENTA,
		AT13_ATS_.CREDITOAFECTADO AS CREDITOAFECTADO,
		CASE
			WHEN AT13_ATS_.TIPOINSTRUMENTO NOT IN ('30','31','32') THEN ''
			ELSE AT13_ATS_.TARJETAAFECTADA
		END AS TARJETAAFECTADA,
		CASE
			WHEN (AT13_ATS_.TIPORECLAMO = '13' AND AT13_ATS_.ENTESUPERVISADO IS NULL) THEN '0114'
			WHEN (AT13_ATS_.TIPORECLAMO = '13' AND SUBSTRING(AT13_ATS_.ENTESUPERVISADO, 1, 2) = '01') THEN AT13_ATS_.ENTESUPERVISADO
			WHEN (AT13_ATS_.TIPORECLAMO = '13' AND SUBSTRING(AT13_ATS_.ENTESUPERVISADO, 1, 2) <> '01') THEN '0114'
			ELSE '0'
		END AS ENTESUPERVISADO,
		AT13_ATS_.ESTADORECLAMO AS ESTADORECLAMO,
		CASE
			WHEN AT13_ATS_.ESTADORECLAMO NOT IN ('2','3') THEN '19000101'
			ELSE AT13_ATS_.FECHASOLUCION
		END AS FECHASOLUCION,
		CASE
			WHEN AT13_ATS_.ESTADORECLAMO = '3' THEN 0
			WHEN AT13_ATS_.TIPORECLAMO IN ('1','2','3','4','5','7','11','13','17','22','23') AND AT13_ATS_.ESTADORECLAMO NOT IN ('2','5') THEN 0
			WHEN AT13_ATS_.TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') THEN 0
			ELSE AT13_ATS_.MONTOREINTEGRO
		END AS MONTOREINTEGRO,
		CASE
			WHEN AT13_ATS_.ESTADORECLAMO IN ('1','3') THEN '19000101'
			WHEN AT13_ATS_.MONTOREINTEGRO = 0 THEN '19000101'
			WHEN AT13_ATS_.TIPORECLAMO IN ('1','2','3','4','5','7','11','13','17','22','23') AND AT13_ATS_.ESTADORECLAMO NOT IN ('2','5') THEN '19000101'
			WHEN AT13_ATS_.TIPORECLAMO IN ('15','16','20','21') AND AT13_ATS_.ESTADORECLAMO IN ('2') THEN '19000101'
			ELSE AT13_ATS_.FECHAREINTEGRO
		END AS FECHAREINTEGRO,
		CASE
			WHEN AT13_ATS_.ESTADORECLAMO NOT IN ('2','3','5') THEN '19000101'
			WHEN AT13_ATS_.ESTADORECLAMO IN ('2','3','5') THEN AT13_ATS_.FECHASOLUCION
		END AS FECHANOTIFICACION,
		CASE
			WHEN AT13_ATS_.TIPOINSTRUMENTO NOT IN ('30','31','32') THEN '0'
			WHEN (AT13_ATS_.TIPORECLAMO NOT IN ('2','3','5','6','11','13') AND AT13_ATS_.TIPOINSTRUMENTO IN ('30','31','32')) THEN '0'
			WHEN AT13_ATS_.CODIGOENTE IS NULL THEN '0114'
			WHEN (AT13_ATS_.TIPORECLAMO = '13' AND SUBSTRING(AT13_ATS_.ENTESUPERVISADO, 1, 2) = '01') THEN AT13_ATS_.ENTESUPERVISADO
			WHEN (AT13_ATS_.TIPORECLAMO = '13' AND SUBSTRING(AT13_ATS_.ENTESUPERVISADO, 1, 2) <> '01') THEN '0114'
			WHEN (SUBSTRING(AT13_ATS_.ENTESUPERVISADO, 1, 2) <> '01') THEN '0114'
			ELSE AT13_ATS_.ENTESUPERVISADO
		END AS CODIGOENTE,
		CASE
			WHEN (AT13_ATS_.TIPORECLAMO NOT IN ('2','3','5','6','11','13') AND AT13_ATS_.TIPOINSTRUMENTO IN ('30','31','32')) THEN ''
			WHEN AT13_ATS_.TIPOINSTRUMENTO NOT IN ('30','31','32') THEN ''
			ELSE AT13_ATS_.IDPOS
		END AS IDPOS,
		CASE
			WHEN AT13_ATS_.TIPORECLAMO NOT IN ('19') THEN ''
			ELSE AT13_ATS_.SISTEMA
		END AS SISTEMA,
		CASE
			WHEN AT13_ATS_.TIPORECLAMO NOT IN ('15','16','20','21') THEN '0'
			ELSE AT13_ATS_.OFICINARECLAMO
		END AS OFICINARECLAMO,
		AT13_ATS_.TIPOOPERACION AS TIPOOPERACION,
		AT13_ATS_.CANAL AS CANAL,
		CASE
			WHEN AT13_ATS_.TIPOINSTRUMENTO IN ('30','32') AND SUBSTRING(AT13_ATS_.TARJETAAFECTADA, 1, 1) = '4' THEN '7'
			WHEN AT13_ATS_.TIPOINSTRUMENTO IN ('30','32') AND SUBSTRING(AT13_ATS_.TARJETAAFECTADA, 1, 1) = '5' THEN '8'
			ELSE '0'
		END AS FRANQUICIA,
		CASE
			WHEN AT13_ATS_.TIPOINSTRUMENTO IN ('30', '31', '32') THEN AT13_ATS_.RED
			ELSE '0'
		END AS RED,
		AT13_ATS_.TIPOCLIENTEDESTINO AS TIPOCLIENTEDESTINO,
		AT13_ATS_.IDCLIENTEDESTINO AS IDCLIENTEDESTINO,
		AT13_ATS_.NOMBRECLIENTEDESTINO AS NOMBRECLIENTEDESTINO,
		AT13_ATS_.NROCUENTADESTINO AS NROCUENTADESTINO,
		AT13_ATS_.CODIGOCUENTADESTINO AS CODIGOCUENTADESTINO,
		AT13_ATS_.MONTODESTINO AS MONTODESTINO
	FROM AT_STG.AT13_ATS_;'''
	hook.run(sql_query_deftxt)


def ATS_TH_AT13(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT13 (
	RECLAMO VARCHAR(20),
	OFICINA VARCHAR(10),
	TIPOCLIENTE VARCHAR(50),
	IDCLIENTE VARCHAR(50),
	NOMBRECLIENTE VARCHAR(255),
	FECHARECLAMO VARCHAR(8),
	TIPOINSTRUMENTO VARCHAR(8),
	TIPORECLAMO VARCHAR(150),
	MONTORECLAMO NUMERIC(15,2),
	CODIGOCUENTA VARCHAR(24),
	CREDITOAFECTADO VARCHAR(20),
	TARJETAAFECTADA VARCHAR(24),
	ENTESUPERVISADO VARCHAR(10),
	ESTADORECLAMO VARCHAR(50),
	FECHASOLUCION VARCHAR(8),
	MONTOREINTEGRO NUMERIC(15,2),
	FECHAREINTEGRO VARCHAR(8),
	FECHANOTIFICACION VARCHAR(8),
	CODIGOENTE VARCHAR(20),
	IDPOS VARCHAR(30),
	SISTEMA VARCHAR(50),
	OFICINARECLAMO VARCHAR(50),
	TIPOOPERACION VARCHAR(255),
	CANAL VARCHAR(255),
	FRANQUICIA VARCHAR(255),
	RED VARCHAR(255),
	TIPOCLIENTEDESTINO VARCHAR(50),
	IDCLIENTEDESTINO VARCHAR(50),
	NOMBRECLIENTEDESTINO VARCHAR(50),
	NROCUENTADESTINO NUMERIC(50),
	CODIGOCUENTADESTINO VARCHAR(20),
	MONTODESTINO NUMERIC(15,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT13;'''
	hook.run(sql_query_deftxt)

	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT13 (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO
	)
	SELECT DISTINCT
		RECLAMO,
		OFICINA,
		TIPOCLIENTE,
		IDCLIENTE,
		NOMBRECLIENTE,
		FECHARECLAMO,
		TIPOINSTRUMENTO,
		TIPORECLAMO,
		MONTORECLAMO,
		CODIGOCUENTA,
		CREDITOAFECTADO,
		TARJETAAFECTADA,
		ENTESUPERVISADO,
		ESTADORECLAMO,
		FECHASOLUCION,
		MONTOREINTEGRO,
		FECHAREINTEGRO,
		FECHANOTIFICACION,
		CODIGOENTE,
		IDPOS,
		SISTEMA,
		OFICINARECLAMO,
		TIPOOPERACION,
		CANAL,
		FRANQUICIA,
		RED,
		TIPOCLIENTEDESTINO,
		IDCLIENTEDESTINO,
		NOMBRECLIENTEDESTINO,
		NROCUENTADESTINO,
		CODIGOCUENTADESTINO,
		MONTODESTINO
	FROM AT_STG.AT13_ATS_TT
	WHERE TIPOCLIENTEDESTINO IS NOT NULL;'''
	hook.run(sql_query_deftxt)

def ATS_TH_AT13_DATACHECK(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT13_ (
	RECLAMO	VARCHAR(20),
	OFICINA	VARCHAR(10),
	TIPOCLIENTE	VARCHAR(50),
	IDCLIENTE	VARCHAR(50),
	NOMBRECLIENTE	VARCHAR(255),
	FECHARECLAMO	VARCHAR(8),
	TIPOINSTRUMENTO	VARCHAR(8),
	TIPORECLAMO	VARCHAR(150),
	MONTORECLAMO	NUMERIC(15,2),
	CODIGOCUENTA	VARCHAR(24),
	CREDITOAFECTADO	VARCHAR(20),
	TARJETAAFECTADA	VARCHAR(24),
	ENTESUPERVISADO	VARCHAR(10),
	ESTADORECLAMO	VARCHAR(50),
	FECHASOLUCION	VARCHAR(8),
	MONTOREINTEGRO	NUMERIC(15,2),
	FECHAREINTEGRO	VARCHAR(8),
	FECHANOTIFICACION	VARCHAR(8),
	CODIGOENTE	VARCHAR(20),
	IDPOS	VARCHAR(15),
	SISTEMA	VARCHAR(50),
	OFICINARECLAMO	VARCHAR(50),
	TIPOOPERACION	VARCHAR(50),
	CANAL	VARCHAR(50),
	FRANQUICIA	VARCHAR(50),
	RED	VARCHAR(50),
	TIPOCLIENTEDESTINO	VARCHAR(50),
	IDCLIENTEDESTINO	VARCHAR(50),
	NOMBRECLIENTEDESTINO	VARCHAR(50),
	NROCUENTADESTINO	NUMERIC(15),
	CODIGOCUENTADESTINO	VARCHAR(20),
	MONTODESTINO	NUMERIC(20,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT13_;'''
	hook.run(sql_query_deftxt)


	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT13_ (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO
	)
	SELECT 	
		RECLAMO,
		OFICINA,
		TIPOCLIENTE,
		IDCLIENTE,
		NOMBRECLIENTE,
		FECHARECLAMO,
		TIPOINSTRUMENTO,
		TIPORECLAMO,
		MONTORECLAMO,
		CODIGOCUENTA,
		CREDITOAFECTADO,
		TARJETAAFECTADA,
		ENTESUPERVISADO,
		ESTADORECLAMO,
		FECHASOLUCION,
		MONTOREINTEGRO,
		FECHAREINTEGRO,
		FECHANOTIFICACION,
		CODIGOENTE,
		IDPOS,
		SISTEMA,
		OFICINARECLAMO,
		TIPOOPERACION,
		CANAL,
		FRANQUICIA,
		RED,
		TIPOCLIENTEDESTINO,
		IDCLIENTEDESTINO,
		NOMBRECLIENTEDESTINO,
		NROCUENTADESTINO,
		CODIGOCUENTADESTINO,
		MONTODESTINO
	FROM ATSUDEBAN.ATS_TH_AT13
	WHERE (
		OFICINA != '0' OR OFICINA IS NOT NULL
		AND (tipocliente IS NOT NULL OR tipocliente NOT IN ('0'))
		AND (
			(tipocliente IN ('V') AND idcliente::numeric BETWEEN 1 AND 40000000)
			OR (tipocliente IN ('E') AND (idcliente::numeric BETWEEN 1 AND 1500000 OR idcliente::numeric BETWEEN 80000000 AND 90000000))
			OR (tipocliente IN ('J','G','R','P') AND idcliente::numeric > 0)
		)
		AND (fechareclamo > '20081231')
		AND (
			(tipoinstrumento IN ('30') AND tiporeclamo IN ('2','4','5','6','8','9','10','11','12','18','21','22','23','25'))
			OR
			(tipoinstrumento IN ('31') AND tiporeclamo IN ('2','3','5','8','9','11','12','13','21','25'))
			OR
			(tipoinstrumento IN ('32') AND tiporeclamo IN ('2','5','7','8','9','11','12','21'))
			OR
			(tipoinstrumento IN ('33') AND tiporeclamo IN ('3','13','21','23'))
			OR
			(tipoinstrumento IN ('50') AND tiporeclamo IN ('8','15','16','18','19','20','21'))
			OR
			(tipoinstrumento IN ('34') AND tiporeclamo IN ('1','9','11','12'))
			OR
			(tipoinstrumento IN ('35') AND tiporeclamo IN ('8','9','11','12','20'))
			OR
			(tipoinstrumento IN ('36') AND tiporeclamo IN ('1','9','11','12','21'))
			OR
			(tipoinstrumento IN ('43','44','45','46','47','48','49') AND tiporeclamo IN ('9','10','11','14','23','24'))
			OR
			(tipoinstrumento IN ('40') AND tiporeclamo IN ('3','7','9','10','11','12','13','18','21','23'))
			OR
			(tipoinstrumento IN ('41') AND tiporeclamo IN ('3','7','9','11','12','13','21','23'))
			OR
			(tipoinstrumento IN ('37') AND tiporeclamo IN ('9','11'))
			OR
			(tipoinstrumento IN ('38') AND tiporeclamo IN ('17','22'))
			OR
			(tipoinstrumento IN ('39') AND tiporeclamo IN ('3','7','9','11','12'))
			OR
			(tipoinstrumento IN ('42') AND tiporeclamo IN ('3','7','9','11','12'))
		)
		AND (
			(tiporeclamo IN ('1','2','3','4','5','7','11','13','17','22','23') AND montoreclamo > 0)
			OR
			(tiporeclamo NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND montoreclamo = 0)
		)
		AND (
			(tipoinstrumento IN ('31','34','41','40') AND codigocuenta IS NOT NULL)
			OR
			(tipoinstrumento NOT IN ('31','34','41','40') AND codigocuenta IS NULL)
		)
		AND (
			(tiporeclamo IN ('9') AND tipoinstrumento IN ('43','44','45','46','47','48','49') AND creditoafectado IS NULL)
			OR
			(tiporeclamo NOT IN ('9') AND tipoinstrumento IN ('43','44','45','46','47','48','49') AND creditoafectado IS NOT NULL)
			OR
			(tipoinstrumento NOT IN ('43','44','45','46','47','48','49') AND creditoafectado IS NULL)
		)
		AND (
			(tiporeclamo IN ('9') AND tipoinstrumento IN ('30','31','32') AND tarjetaafectada IS NULL)
			OR
			(tiporeclamo NOT IN ('9') AND tipoinstrumento IN ('30','31','32') AND tarjetaafectada IS NOT NULL)
			OR
			(tipoinstrumento NOT IN ('30','31','32') AND tarjetaafectada IS NULL)
		)
		AND (
			(tiporeclamo IN ('13') AND entesupervisado != '0')
			OR
			(tiporeclamo NOT IN ('13') AND entesupervisado = '0')
		)
		AND (
			(ESTADORECLAMO IN ('2','3') AND CAST(FECHASOLUCION AS DATE) >= CAST(FECHARECLAMO AS DATE))
			OR
			(ESTADORECLAMO NOT IN ('2','3') AND FECHASOLUCION = '19000101')
		)
		AND (
			(tiporeclamo IN ('1','2','3','4','5','7','11','13','17','22','23') 
			AND estadoreclamo IN ('2','5') AND montoreintegro > 0)
			OR
			(tiporeclamo IN ('1','2','3','4','5','7','11','13','17','22','23') 
			AND estadoreclamo NOT IN ('2','5') AND montoreintegro = 0)
			OR
			(tiporeclamo NOT IN ('1','2','3','4','5','7','11','13','17','22','23') 
			AND montoreintegro = 0)
			OR
			(tiporeclamo IN ('14') 
			AND estadoreclamo IN ('2','5') AND montoreintegro >= 0)
		)
		AND (
			(TIPORECLAMO IN ('1','2','3','4','5','7','11','13','17','22','23') AND
			ESTADORECLAMO IN ('2','5') AND 
			FECHAREINTEGRO >= FECHARECLAMO)
		OR
			(TIPORECLAMO IN ('1','2','3','4','5','7','11','13','17','22','23') AND
			ESTADORECLAMO IN ('1','3') AND 
			FECHAREINTEGRO = '19000101')
		OR
			(TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND
			MONTOREINTEGRO = 0 AND 
			FECHAREINTEGRO = '19000101')
		OR
			(TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND
			MONTOREINTEGRO > 0 AND (TIPORECLAMO NOT IN ('1','2','3','4','5','7','11','13','17','22','23') AND
			MONTOREINTEGRO > 0))
		OR
			(MONTOREINTEGRO = 0 AND 
			FECHAREINTEGRO = '19000101')
		)
		AND (
			(estadoreclamo IN ('2','3','5') AND 
			(fechanotificacion::DATE >= fechasolucion::DATE OR fechanotificacion::DATE <= DATE_TRUNC('month', fechareclamo::DATE) + INTERVAL '1 month - 1 day'))
			OR
			(estadoreclamo NOT IN ('2','3','5') AND fechanotificacion = '19000101')
		)
		AND (
			(tiporeclamo IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND (codigoente NOT IN ('0') OR codigoente IS NOT NULL))
			OR
			(tiporeclamo NOT IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND codigoente = '0')
			OR
			(tipoinstrumento NOT IN ('30','31','32') AND codigoente = '0')
		)
		AND (
			(tiporeclamo IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND idpos IS NOT NULL AND idpos <> '0') 
			OR 
			(tiporeclamo NOT IN ('2','3','5','6','11','13') AND tipoinstrumento IN ('30','31','32') AND idpos = '0') 
			OR 
			(tipoinstrumento NOT IN ('30','31','32') AND idpos = '0')
		)
		AND (
			(tiporeclamo IN ('19') AND sistema IS NOT NULL)
			OR
			(tiporeclamo NOT IN ('19') AND sistema IS NULL)
		)
		AND (
			(tiporeclamo IN ('15','16','20','21') AND oficinareclamo != '0')
			OR
			(tiporeclamo NOT IN ('15','16','20','21') AND oficinareclamo = '0')
		)
		AND (
			(tipoinstrumento IN ('30','31','32') AND canal NOT IN ('5')) OR
			(tipoinstrumento IN ('33') AND canal NOT IN ('3','5','10')) OR
			(tipoinstrumento IN ('34') AND canal NOT IN ('2','3','5')) OR
			(tipoinstrumento IN ('35') AND canal NOT IN ('1','2','3','4','5','6','7','9','10')) OR
			(tipoinstrumento IN ('36','37') AND canal NOT IN ('1','2','3','5','7','9','10')) OR
			(tipoinstrumento IN ('38','39') AND canal NOT IN ('1','2','5','7','9','10')) OR
			(tipoinstrumento IN ('40','41') AND canal NOT IN ('1','2','10')) OR
			(tipoinstrumento IN ('42','43','44','45','46','47','48','49') AND canal NOT IN ('1','2','5','7','9','10')) OR
			(tipoinstrumento IN ('50') AND canal IN ('8','0'))
		)
		AND (
			(tipoinstrumento IN ('30','32') AND franquicia != '0')
			OR
			(tipoinstrumento NOT IN ('30','32') AND franquicia = '0')
		)
		AND (
			(TIPOINSTRUMENTO IN ('30','31','32') AND RED != '0')
			OR
			(TIPOINSTRUMENTO NOT IN ('30','31','32') AND RED = '0')
		)
	);'''
	hook.run(sql_query_deftxt)

def AT13_TH_BC(**kwargs):
	hook = PostgresHook(postgres_conn_id='repodataprd')

	# Recuperar las variables definidas en las tareas previas
	FechaFin = get_variable('FechaFin')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT13_TH_BC (
	RECLAMO	VARCHAR(20),
	OFICINA	VARCHAR(10),
	TIPOCLIENTE	VARCHAR(50),
	IDCLIENTE	VARCHAR(50),
	NOMBRECLIENTE	VARCHAR(255),
	FECHARECLAMO	VARCHAR(8),
	TIPOINSTRUMENTO	VARCHAR(8),
	TIPORECLAMO	VARCHAR(150),
	MONTORECLAMO	NUMERIC(15,2),
	CODIGOCUENTA	VARCHAR(24),
	CREDITOAFECTADO	VARCHAR(20),
	TARJETAAFECTADA	VARCHAR(24),
	ENTESUPERVISADO	VARCHAR(10),
	ESTADORECLAMO	VARCHAR(50),
	FECHASOLUCION	VARCHAR(8),
	MONTOREINTEGRO	NUMERIC(15,2),
	FECHAREINTEGRO	VARCHAR(8),
	FECHANOTIFICACION	VARCHAR(8),
	CODIGOENTE	VARCHAR(20),
	IDPOS	VARCHAR(15),
	SISTEMA	VARCHAR(50),
	OFICINARECLAMO	VARCHAR(50),
	TIPOOPERACION	VARCHAR(50),
	CANAL	VARCHAR(50),
	FRANQUICIA	VARCHAR(50),
	RED	VARCHAR(50),
	TIPOCLIENTEDESTINO	VARCHAR(50),
	IDCLIENTEDESTINO	VARCHAR(50),
	NOMBRECLIENTEDESTINO	VARCHAR(50),
	NROCUENTADESTINO	NUMERIC(15),
	CODIGOCUENTADESTINO	VARCHAR(20),
	MONTODESTINO	NUMERIC(20,2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT13_TH_BC;'''
	hook.run(sql_query_deftxt)


	# INSERTAR DATOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ATSUDEBAN.AT13_TH_BC (
	RECLAMO,
	OFICINA,
	TIPOCLIENTE,
	IDCLIENTE,
	NOMBRECLIENTE,
	FECHARECLAMO,
	TIPOINSTRUMENTO,
	TIPORECLAMO,
	MONTORECLAMO,
	CODIGOCUENTA,
	CREDITOAFECTADO,
	TARJETAAFECTADA,
	ENTESUPERVISADO,
	ESTADORECLAMO,
	FECHASOLUCION,
	MONTOREINTEGRO,
	FECHAREINTEGRO,
	FECHANOTIFICACION,
	CODIGOENTE,
	IDPOS,
	SISTEMA,
	OFICINARECLAMO,
	TIPOOPERACION,
	CANAL,
	FRANQUICIA,
	RED,
	TIPOCLIENTEDESTINO,
	IDCLIENTEDESTINO,
	NOMBRECLIENTEDESTINO,
	NROCUENTADESTINO,
	CODIGOCUENTADESTINO,
	MONTODESTINO
	)
	SELECT
		a.reclamo,
		a.oficina,
		a.tipocliente,
		a.idcliente,
		a.nombrecliente,
		a.fechareclamo,
		a.tipoinstrumento,
		a.tiporeclamo,
		a.montoreclamo,
		CASE
			WHEN a.codigocuenta = '00000000000000000000' THEN ''
			ELSE a.codigocuenta
		END as codigocuenta,
		a.creditoafectado,
		a.tarjetaafectada,
		a.entesupervisado,
		CASE
			WHEN CAST(a.fechasolucion AS DATE) > CAST('{FechaFin}' AS DATE) THEN '1'
			ELSE a.estadoreclamo
		END as estadoreclamo,
		CASE
			WHEN CAST(a.fechasolucion AS DATE) > CAST('{FechaFin}' AS DATE) THEN '19000101'
			ELSE a.fechasolucion
		END as fechasolucion,
		CASE
			WHEN CAST(a.fechasolucion AS DATE) > CAST('{FechaFin}' AS DATE) THEN 0
			ELSE a.montoreintegro
		END as montoreintegro,
		CASE
			WHEN CAST(a.fechasolucion AS DATE) > CAST('{FechaFin}' AS DATE) THEN '19000101'
			ELSE a.fechareintegro
		END as fechareintegro,
		CASE
			WHEN CAST(a.fechasolucion AS DATE) > CAST('{FechaFin}' AS DATE) THEN '19000101'
			ELSE a.fechanotificacion
		END as fechanotificacion,
		a.codigoente,
		a.idpos,
		a.sistema,
		a.oficinareclamo,
		a.tipooperacion,
		a.canal,
		a.franquicia,
		a.red,
		a.tipoclientedestino,
		a.idclientedestino,
		a.nombreclientedestino,
		a.nrocuentadestino,
		a.codigocuentadestino,
		a.montodestino
	FROM atsudeban.ats_th_at13_ AS a INNER JOIN at_stg.at13_union_bc AS b ON CAST(a.reclamo AS INTEGER) = b.sr_secuencial;'''
	hook.run(sql_query_deftxt)

	# Borramos tablas anteriores
	#sql_query = '''DROP TABLE IF EXISTS ATSUDEBAN.ATS_TH_AT13_;'''
	#hook.run(sql_query)


###### DEFINICION DEL DAG ######

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT13', default_args=default_args, schedule=None, catchup=False)

AT13_Catalogo_task = PythonOperator(
	task_id='AT13_Catalogo_task',
	python_callable=AT13_Catalogo,
	dag=dag
)

AT13_Catalogo_det_task = PythonOperator(
	task_id='AT13_Catalogo_det_task',
	python_callable=AT13_Catalogo_det,
	dag=dag
)

AT13_Categoria_task = PythonOperator(
	task_id='AT13_Categoria_task',
	python_callable=AT13_Categoria,
	dag=dag
)

AT13_Categoria_recl_task = PythonOperator(
	task_id='AT13_Categoria_recl_task',
	python_callable=AT13_Categoria_recl,
	dag=dag
)

AT13_DetalleRecl_task = PythonOperator(
	task_id='AT13_DetalleRecl_task',
	python_callable=AT13_DetalleRecl,
	dag=dag
)

AT13_IF_RECLAMOS_task = PythonOperator(
	task_id='AT13_IF_RECLAMOS_task',
	python_callable=AT13_IF_RECLAMOS,
	dag=dag
)

AT13_FASE_REC_task = PythonOperator(
	task_id='AT13_FASE_REC_task',
	python_callable=AT13_FASE_REC,
	dag=dag
)

AT13_FASE_RECC_task = PythonOperator(
	task_id='AT13_FASE_RECC_task',
	python_callable=AT13_FASE_RECC,
	dag=dag
)

AT13_FASE_RECA_task = PythonOperator(
	task_id='AT13_FASE_RECA_task',
	python_callable=AT13_FASE_RECA,
	dag=dag
)

AT13_UNION_task = PythonOperator(
	task_id='AT13_UNION_task',
	python_callable=AT13_UNION,
	dag=dag
)

AT13_UNION_BC_task = PythonOperator(
	task_id='AT13_UNION_BC_task',
	python_callable=AT13_UNION_BC,
	dag=dag
)

AT13_ATS_task = PythonOperator(
	task_id='AT13_ATS_task',
	python_callable=AT13_ATS,
	dag=dag
)

IMPORTAR_INSUMO_task = PythonOperator(
    task_id='IMPORTAR_INSUMO_task',
    python_callable=IMPORTAR_INSUMO,
    dag=dag
)

AT13_TRANSACCIONES__task = PythonOperator(
	task_id='AT13_TRANSACCIONES__task',
	python_callable=AT13_TRANSACCIONES_,
	dag=dag
)

AT13_ATS__task = PythonOperator(
	task_id='AT13_ATS__task',
	python_callable=AT13_ATS_,
	dag=dag
)

SP_ACTUALIZACION_31_task = PythonOperator(
	task_id='SP_ACTUALIZACION_31_task',
	python_callable=SP_ACTUALIZACION_31,
	dag=dag
)

AT13_ATS_TT_task = PythonOperator(
	task_id='AT13_ATS_TT_task',
	python_callable=AT13_ATS_TT,
	dag=dag
)

ATS_TH_AT13_task = PythonOperator(
	task_id='ATS_TH_AT13_task',
	python_callable=ATS_TH_AT13,
	dag=dag
)

ATS_TH_AT13_DATACHECK_task = PythonOperator(
	task_id='ATS_TH_AT13_DATACHECK_task',
	python_callable=ATS_TH_AT13_DATACHECK,
	dag=dag
)

AT13_TH_BC_task = PythonOperator(
	task_id='AT13_TH_BC_task',
	python_callable=AT13_TH_BC,
	dag=dag
)


###### SECUENCIA DE EJECUCION ######
AT13_Catalogo_task >> AT13_Catalogo_det_task >> AT13_Categoria_task >> AT13_Categoria_recl_task >> AT13_DetalleRecl_task >> AT13_IF_RECLAMOS_task >> AT13_FASE_REC_task >> AT13_FASE_RECC_task >> AT13_FASE_RECA_task >> AT13_UNION_task >> AT13_UNION_BC_task >> AT13_ATS_task >> IMPORTAR_INSUMO_task >> AT13_TRANSACCIONES__task >> AT13_ATS__task >> SP_ACTUALIZACION_31_task >> AT13_ATS_TT_task >> ATS_TH_AT13_task >> ATS_TH_AT13_DATACHECK_task >> AT13_TH_BC_task
