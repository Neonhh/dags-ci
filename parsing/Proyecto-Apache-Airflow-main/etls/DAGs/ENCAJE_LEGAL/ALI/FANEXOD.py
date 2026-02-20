from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import time

### FUNCIONES DE CADA TAREA ###

def varFechaInicio(**kwargs):
	hook = PostgresHook(postgres_conn_id='nombre_conexion')
	sql_query = '''  
	SELECT
	CASE
		WHEN to_char(current_date, 'fmday') IN ('monday','lunes') THEN to_char(current_date - interval '7 days',  'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('tuesday','martes') THEN to_char(current_date - interval '8 days',  'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('wednesday','miércoles') THEN to_char(current_date - interval '9 days',  'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('thursday','jueves') THEN to_char(current_date - interval '10 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('friday','viernes') THEN to_char(current_date - interval '11 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('saturday','sábado') THEN to_char(current_date - interval '12 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('sunday','domingo') THEN to_char(current_date - interval '13 days', 'DD/MM/YY')
	END;
	'''
	result = hook.get_records(sql_query)
	Variable.set('varFechaInicio', result[0][0])

def varFechaFin(**kwargs):
	hook = PostgresHook(postgres_conn_id='nombre_conexion')
	sql_query = '''  
	SELECT
	CASE
		WHEN to_char(current_date, 'fmday') IN ('monday','lunes') THEN to_char(current_date - INTERVAL '3 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('tuesday','martes') THEN to_char(current_date - INTERVAL '4 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('wednesday','miércoles') THEN to_char(current_date - INTERVAL '5 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('thursday','jueves') THEN to_char(current_date - INTERVAL '6 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('friday','viernes') THEN to_char(current_date - INTERVAL '7 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('saturday','sábado') THEN to_char(current_date - INTERVAL '8 days', 'DD/MM/YY')
		WHEN to_char(current_date, 'fmday') IN ('sunday','domingo') THEN to_char(current_date - INTERVAL '9 days', 'DD/MM/YY')
	END;
	'''
	result = hook.get_records(sql_query)
	Variable.set('varFechaFin', result[0][0])

def varCantDiasHabiles(**kwargs):
	hook = PostgresHook(postgres_conn_id='nombre_conexion')

	varFechaInicio = Variable.get('varFechaInicio')
	varFechaFin = Variable.get('varFechaFin')

	sql_query = f'''SELECT 5 - COUNT(*)
	FROM ods.cl_dias_feriados
	WHERE df_fecha BETWEEN to_date('{varFechaInicio}', 'DD/MM/YY') AND to_date('{varFechaFin}', 'DD/MM/YY');'''

	result = hook.get_records(sql_query)
	Variable.set('varCantDiasHabiles', result[0][0])

def ALI_FANEXOD(**kwargs):
	hook = PostgresHook(postgres_conn_id='mi_conexion')

	# Create target table
	sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ater.ali_fanexod (
	clave VARCHAR(15),
	clave_1 VARCHAR(25),
	lunes NUMERIC(15),
	martes NUMERIC(15),
	miercoles NUMERIC(15),
	jueves NUMERIC(15),
	viernes NUMERIC(15),
	total NUMERIC(15,1),
	promedio NUMERIC(15)
	);'''
	hook.run(sql_query_deftxt)

	# Truncate target table
	sql_query_deftxt = f'''TRUNCATE TABLE ater.ali_fanexod;'''
	hook.run(sql_query_deftxt)

	# Insert new rows
	sql_query_deftxt = f'''INSERT INTO ater.ali_fanexod (
	CLAVE, 
	CLAVE_1, 
	LUNES, 
	MARTES, 
	MIERCOLES, 
	JUEVES, 
	VIERNES, 
	TOTAL, 
	PROMEDIO
	)
	SELECT 
		TRIM(ater_parametros.clave) AS CLAVE, 
		TRIM(ater_parametros.clave_1) AS CLAVE_1,
		0 AS LUNES,
		0 AS MARTES,
		0 AS MIERCOLES,
		0 AS JUEVES,
		0 AS VIERNES,
		0 AS TOTAL,
		0 AS PROMEDIO
	FROM ater.ater_parametros AS ater_parametros
	WHERE ater_parametros.formulario = 'FANEXOD';'''
	hook.run(sql_query_deftxt)

def ACTUALIZA_FANEXOD(**kwargs):
	hook = PostgresHook(postgres_conn_id='mi_conexion')

	# CONSTANTE_FANEXOD_0
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = 0, martes = 0, miercoles = 0, jueves = 0, viernes = 0, total = 0, promedio = 0
WHERE clave IN ('21A071G','21A071K','21A072D','21A072E','21A072F','21A072H','21A072L','21A072M','21A074','21A076A','21A077','21A078','21A079A','21A081','21B071','21B072','21B073','21B074','21B076','21B077','21B078','21B079','21B080','21B081','21C072F','21C072D','21C074A','21C076E','21C076F','21C077A','21C077B','21C078','21C080','21C081','21D072J','21D072F','21D072K','21D073A','21D074B','21D074D','21D077','21D078');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('115')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('115')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('115')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('115')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('115')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('115')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('115')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('115')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('115')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('115'))
WHERE ater.ali_fanexod.clave IN ('21A071A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('157')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('157')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('157')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('157')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('157')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('157')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('157')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('157')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('157')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('157'))
WHERE ali_fanexod.clave IN ('21A071B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('151')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('151')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('151')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('151')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('151')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('151')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('151')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('151')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('151')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('151'))
WHERE ater.ali_fanexod.clave IN ('21A071C');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET 
	lunes = (
		SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('105')
	) + (
		SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('105')
	),
	martes = (
		SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('105')
	) + (
		SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('105')
	),
	miercoles = (
		SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('105')
	) + (
		SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('105')
	),
	jueves = (
		SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('105')
	) + (
		SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('105')
	),
	viernes = (
		SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('105')
	) + (
		SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('105')
	)
WHERE clave IN ('21A071D');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071E
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET 
	lunes = (
		SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('108')
	) + (
		SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('108')
	),
	martes = (
		SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('108')
	) + (
		SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('108')
	),
	miercoles = (
		SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('108')
	) + (
		SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('108')
	),
	jueves = (
		SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('108')
	) + (
		SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('108')
	),
	viernes = (
		SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('108')
	) + (
		SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('108')
	)
WHERE clave IN ('21A071E');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071G
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('104')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('104')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('104')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('104')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('104')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('104')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('104')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('104')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('104')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('104'))
WHERE ater.ali_fanexod.clave IN ('21A071G');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071H
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('102')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('102')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('102')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('102')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('102')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('102')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('102')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('102')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('102')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('102'))
WHERE ater.ali_fanexod.clave IN ('21A071H');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071I
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('116')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('116')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('116')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('116')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('116')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('116')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('116')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('116')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('116')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('116'))
WHERE ater.ali_fanexod.clave IN ('21A071I');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071L
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('137')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('137')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('137')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('137')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('137')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('137')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('137')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('137')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('137')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('137'))
WHERE ali_fanexod.clave IN ('21A071L');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071M
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('134')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('134')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('134')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('134')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('134')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('134')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('134')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('134')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('134')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('134'))
WHERE ater.ali_fanexod.clave IN ('21A071M');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071N
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('156')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('156')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('156')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('156')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('156')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('156')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('156')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('156')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('156')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('156'))
WHERE ater.ali_fanexod.clave IN ('21A071N');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071O
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('171')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('171')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('171')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('171')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('171')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('171')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('171')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('171')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('171')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('171'))
WHERE ater.ali_fanexod.clave IN ('21A071O');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071P
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('166')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('166')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('166')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('166')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('166')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('166')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('166')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('166')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('166')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('166'))
WHERE ater.ali_fanexod.clave IN ('21A071P');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071Q
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('172')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('172')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('172')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('172')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('172')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('172')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('172')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('172')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('172')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('172'))
WHERE ater.ali_fanexod.clave IN ('21A071Q');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071R
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('163')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('163')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('163')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('163')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('163')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('163')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('163')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('163')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('163')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('163'))
WHERE ater.ali_fanexod.clave IN ('21A071R');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071S
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('174')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('174')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('174')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('174')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('174')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('174')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('174')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('174')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('174')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('174'))
WHERE ater.ali_fanexod.clave IN ('21A071S');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071T
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('175')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('175')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('175')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('175')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('175')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('175')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('175')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('175')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('175')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('175'))
WHERE ater.ali_fanexod.clave IN ('21A071T');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071U
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('114')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('114')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('114')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('114')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('114')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('114')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('114')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('114')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('114')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('114'))
WHERE ater.ali_fanexod.clave IN ('21A071U');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071V
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('128')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('128')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('128')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('128')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('128')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('128')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('128')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('128')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('128')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('128'))
WHERE ali_fanexod.clave IN ('21A071V');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071W
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET 
	lunes = (
		SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('177')
	) + (
		SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('177')
	),
	martes = (
		SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('177')
	) + (
		SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('177')
	),
	miercoles = (
		SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('177')
	) + (
		SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('177')
	),
	jueves = (
		SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('177')
	) + (
		SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('177')
	),
	viernes = (
		SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('177')
	) + (
		SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('177')
	)
WHERE clave IN ('21A071W');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071X
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('173')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('173')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('173')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('173')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('173')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('173')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('173')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('173')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('173')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('173'))
WHERE ater.ali_fanexod.clave IN ('21A071X');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071Y
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('191')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('191')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('191')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('191')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('191')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('191')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('191')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('191')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('191')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('191'))
WHERE ali_fanexod.clave IN ('21A071Y');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071Z
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('138')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('138')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('138')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('138')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('138')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('138')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('138')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('138')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('138')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('138'))
WHERE clave IN ('21A071Z');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A071
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071A','21A071B','21A071C','21A071D','21A071E','21A071G','21A071H','21A071I','21A071J','21A071L','21A071M','21A071N','21A071O','21A071P','21A071Q','21A071R','21A071S','21A071T','21A071U','21A071V','21A071W','21A071X','21A071Y','21A071Z')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071A','21A071B','21A071C','21A071D','21A071E','21A071G','21A071H','21A071I','21A071J','21A071L','21A071M','21A071N','21A071O','21A071P','21A071Q','21A071R','21A071S','21A071T','21A071U','21A071V','21A071W','21A071X','21A071Y','21A071Z')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071A','21A071B','21A071C','21A071D','21A071E','21A071G','21A071H','21A071I','21A071J','21A071L','21A071M','21A071N','21A071O','21A071P','21A071Q','21A071R','21A071S','21A071T','21A071U','21A071V','21A071W','21A071X','21A071Y','21A071Z')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071A','21A071B','21A071C','21A071D','21A071E','21A071G','21A071H','21A071I','21A071J','21A071L','21A071M','21A071N','21A071O','21A071P','21A071Q','21A071R','21A071S','21A071T','21A071U','21A071V','21A071W','21A071X','21A071Y','21A071Z')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071A','21A071B','21A071C','21A071D','21A071E','21A071G','21A071H','21A071I','21A071J','21A071L','21A071M','21A071N','21A071O','21A071P','21A071Q','21A071R','21A071S','21A071T','21A071U','21A071V','21A071W','21A071X','21A071Y','21A071Z'))
WHERE ali_fanexod.clave = '21A071';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('124')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('124')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('124')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('124')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('124')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('124')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('124')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('124')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('124')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('124'))
WHERE ater.ali_fanexod.clave IN ('21A072A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (
	SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_241021 AS dv WHERE dv.id_banco = '145'
) + (
	SELECT COALESCE(SUM(lunes), 0) FROM ater.op_mesa_24108 AS om WHERE om.cod = '145'
) + (
	SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS df WHERE df.cod_banco = '145'
),
martes = (
	SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_241021 AS dv WHERE dv.id_banco = '145'
) + (
	SELECT COALESCE(SUM(martes), 0) FROM ater.op_mesa_24108 AS om WHERE om.cod = '145'
) + (
	SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS df WHERE df.cod_banco = '145'
),
miercoles = (
	SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_241021 AS dv WHERE dv.id_banco = '145'
) + (
	SELECT COALESCE(SUM(miercoles), 0) FROM ater.op_mesa_24108 AS om WHERE om.cod = '145'
) + (
	SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS df WHERE df.cod_banco = '145'
),
jueves = (
	SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_241021 AS dv WHERE dv.id_banco = '145'
) + (
	SELECT COALESCE(SUM(jueves), 0) FROM ater.op_mesa_24108 AS om WHERE om.cod = '145'
) + (
	SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS df WHERE df.cod_banco = '145'
),
viernes = (
	SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_241021 AS dv WHERE dv.id_banco = '145'
) + (
	SELECT COALESCE(SUM(viernes), 0) FROM ater.op_mesa_24108 AS om WHERE om.cod = '145'
) + (
	SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS df WHERE df.cod_banco = '145'
)
WHERE clave = '21A072B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('003')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('003')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('003')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('003')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('003')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('003')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('003')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('003')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('003')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('003'))
WHERE ater.ali_fanexod.clave IN ('21A072C');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072G
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('105A')),
	martes = (SELECT SUM(martes) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('105A')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('105A')),
	jueves = (SELECT SUM(jueves) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('105A')),
	viernes = (SELECT SUM(viernes) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('105A'))
WHERE ater.ali_fanexod.clave IN ('21A072G');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072I
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ater_correspondales_mn AS ater_correspondales_mn WHERE ater_correspondales_mn.cod_banco = '144'),
	martes = (SELECT SUM(martes) FROM ater.ater_correspondales_mn AS ater_correspondales_mn WHERE ater_correspondales_mn.cod_banco = '144'),
	miercoles = (SELECT SUM(miercoles) FROM ater.ater_correspondales_mn AS ater_correspondales_mn WHERE ater_correspondales_mn.cod_banco = '144'),
	jueves = (SELECT SUM(jueves) FROM ater.ater_correspondales_mn AS ater_correspondales_mn WHERE ater_correspondales_mn.cod_banco = '144'),
	viernes = (SELECT SUM(viernes) FROM ater.ater_correspondales_mn AS ater_correspondales_mn WHERE ater_correspondales_mn.cod_banco = '144')
WHERE clave = '21A072I';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072J
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco = '133'),
	martes = (SELECT SUM(martes) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco = '133'),
	miercoles = (SELECT SUM(miercoles) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco = '133'),
	jueves = (SELECT SUM(jueves) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco = '133'),
	viernes = (SELECT SUM(viernes) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco = '133')
WHERE ater.ali_fanexod.clave = '21A072J';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072K
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('196')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('196')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('196')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('196')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('196')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('196')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('196')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('196')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('196')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('196'))
WHERE ater.ali_fanexod.clave IN ('21A072K');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072L
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('106')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('106')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('106')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('106')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('106')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('106')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('106')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('106')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('106')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('106'))
WHERE ater.ali_fanexod.clave IN ('21A072L');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072M
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('193')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('193')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('193')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('193')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('193')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('193')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('193')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('193')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ater_corresponsales_mn AS ater_corresponsales_mn WHERE ater_corresponsales_mn.cod_banco IN ('193')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('193'))
WHERE ater.ali_fanexod.clave IN ('21A072M');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072N
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('159')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('159')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('159')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('159')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('159')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('159')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('159')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('159')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('159')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('159'))
WHERE ater.ali_fanexod.clave IN ('21A072N');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072O
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('145')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('145')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('145')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('145')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('145')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('145')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('145')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('145')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('145')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('145'))
WHERE ater.ali_fanexod.clave IN ('21A072O');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072P
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('601')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('601')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('601')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('601')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('601')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('601')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('601')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('601')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id IN ('601')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco IN ('601'))
WHERE ater.ali_fanexod.clave IN ('21A072P');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A072
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21A072A','21A072B','21A072C','21A072D','21A072E','21A072F','21A072G','21A072H','21A072I','21A072J','21A072K','21A072L','21A072M','21A072N','21A072O','21A072P')),
	MARTES = (SELECT SUM(MARTES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21A072A','21A072B','21A072C','21A072D','21A072E','21A072F','21A072G','21A072H','21A072I','21A072J','21A072K','21A072L','21A072M','21A072N','21A072O','21A072P')),
	MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21A072A','21A072B','21A072C','21A072D','21A072E','21A072F','21A072G','21A072H','21A072I','21A072J','21A072K','21A072L','21A072M','21A072N','21A072O','21A072P')),
	JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21A072A','21A072B','21A072C','21A072D','21A072E','21A072F','21A072G','21A072H','21A072I','21A072J','21A072K','21A072L','21A072M','21A072N','21A072O','21A072P')),
	VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21A072A','21A072B','21A072C','21A072D','21A072E','21A072F','21A072G','21A072H','21A072I','21A072J','21A072K','21A072L','21A072M','21A072N','21A072O','21A072P'))
WHERE CLAVE = '21A072';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A073A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '218'),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '218'),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '218'),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '218'),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '218')
WHERE ali_fanexod.clave = '21A073A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A073
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A073A')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A073A')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A073A')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A073A')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A073A'))
WHERE clave IN ('21A073');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A076B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '412'),
	martes = (SELECT SUM(martes) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '412'),
	miercoles = (SELECT SUM(miercoles) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '412'),
	jueves = (SELECT SUM(jueves) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '412'),
	viernes = (SELECT SUM(viernes) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '412')
WHERE ater.ali_fanexod.clave = '21A076B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A076C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '410'),
	martes = (SELECT SUM(martes) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '410'),
	miercoles = (SELECT SUM(miercoles) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '410'),
	jueves = (SELECT SUM(jueves) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '410'),
	viernes = (SELECT SUM(viernes) FROM ater.ccdia_totales AS ccdia_totales WHERE ccdia_totales.id = '410')
WHERE clave = '21A076B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A076
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A076A','21A076B','21A076C')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A076A','21A076B','21A076C')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A076A','21A076B','21A076C')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A076A','21A076B','21A076C')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A076A','21A076B','21A076C'))
WHERE clave = '21A076';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A079
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A079A')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A079A')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A079A')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A079A')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A079A'))
WHERE clave IN ('21A079');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A080A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.ccdia_totales AS cc WHERE cc.id = '146' AND cc.cta_banco = '146'), 0) + COALESCE((SELECT SUM(lunes) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '146'), 0),
	martes = COALESCE((SELECT SUM(martes) FROM ater.ccdia_totales AS cc WHERE cc.id = '146' AND cc.cta_banco = '146'), 0) + COALESCE((SELECT SUM(martes) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '146'), 0),
	miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.ccdia_totales AS cc WHERE cc.id = '146' AND cc.cta_banco = '146'), 0) + COALESCE((SELECT SUM(miercoles) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '146'), 0),
	jueves = COALESCE((SELECT SUM(jueves) FROM ater.ccdia_totales AS cc WHERE cc.id = '146' AND cc.cta_banco = '146'), 0) + COALESCE((SELECT SUM(jueves) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '146'), 0),
	viernes = COALESCE((SELECT SUM(viernes) FROM ater.ccdia_totales AS cc WHERE cc.id = '146' AND cc.cta_banco = '146'), 0) + COALESCE((SELECT SUM(viernes) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '146'), 0)
WHERE ater.ali_fanexod.clave = '21A080A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A080B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.ccdia_totales AS cc WHERE cc.id = '168' AND cc.cta_banco = '168') + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '168'),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.ccdia_totales AS cc WHERE cc.id = '168' AND cc.cta_banco = '168') + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '168'),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.ccdia_totales AS cc WHERE cc.id = '168' AND cc.cta_banco = '168') + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '168'),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.ccdia_totales AS cc WHERE cc.id = '168' AND cc.cta_banco = '168') + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '168'),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.ccdia_totales AS cc WHERE cc.id = '168' AND cc.cta_banco = '168') + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '168')
WHERE ater.ali_fanexod.clave = '21A080B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A080C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.ccdia_totales AS cc WHERE cc.id = '169' AND cc.cta_banco = '169'), 0) + COALESCE((SELECT SUM(lunes) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '169'), 0),
	martes = COALESCE((SELECT SUM(martes) FROM ater.ccdia_totales AS cc WHERE cc.id = '169' AND cc.cta_banco = '169'), 0) + COALESCE((SELECT SUM(martes) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '169'), 0),
	miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.ccdia_totales AS cc WHERE cc.id = '169' AND cc.cta_banco = '169'), 0) + COALESCE((SELECT SUM(miercoles) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '169'), 0),
	jueves = COALESCE((SELECT SUM(jueves) FROM ater.ccdia_totales AS cc WHERE cc.id = '169' AND cc.cta_banco = '169'), 0) + COALESCE((SELECT SUM(jueves) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '169'), 0),
	viernes = COALESCE((SELECT SUM(viernes) FROM ater.ccdia_totales AS cc WHERE cc.id = '169' AND cc.cta_banco = '169'), 0) + COALESCE((SELECT SUM(viernes) FROM ater.dep_vista_finan AS dv WHERE dv.cod_banco = '169'), 0)
WHERE clave = '21A080C';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A080
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21A080A','21A080B','21A080C')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21A080A','21A080B','21A080C')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21A080A','21A080B','21A080C')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21A080A','21A080B','21A080C')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21A080A','21A080B','21A080C'))
WHERE clave = '21A080';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A081A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco = '152'),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco = '152'),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco = '152'),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco = '152'),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_finan AS dep_vista_finan WHERE dep_vista_finan.cod_banco = '152')
WHERE clave = '21A081A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A081
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A081A')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A081A')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A081A')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A081A')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A081A'))
WHERE clave IN ('21A081');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21B070
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B071','21B072','21B073','21B074','21B076','21B077','21B078','21B079')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B071','21B072','21B073','21B074','21B076','21B077','21B078','21B079')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B071','21B072','21B073','21B074','21B076','21B077','21B078','21B079')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B071','21B072','21B073','21B074','21B076','21B077','21B078','21B079')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B071','21B072','21B073','21B074','21B076','21B077','21B078','21B079'))
WHERE clave IN ('21B070');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21B000
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B070')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B070')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B070')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B070')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B070'))
WHERE clave IN ('21B000');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('115')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('115')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('115')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('115')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('115'))
WHERE ater.ali_fanexod.clave IN ('21C071A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('157')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('157')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('157')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('157')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('157'))
WHERE ater.ali_fanexod.clave IN ('21C071B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('151')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('151')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('151')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('151')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('151'))
WHERE ater.ali_fanexod.clave IN ('21C071C');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '105'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '105'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '105'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '105'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '105')
WHERE clave = '21C071D';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071E
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '108'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '108'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '108'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '108'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '108')
WHERE clave = '21C071E';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071G
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '104'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '104'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '104'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '104'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '104')
WHERE clave = '21C071G';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071H
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('102')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('102')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('102')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('102')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('102'))
WHERE ater.ali_fanexod.clave IN ('21C071H');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071I
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '116'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '116'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '116'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '116'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '116')
WHERE ater.ali_fanexod.clave = '21C071I';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071L
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '137'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '137'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '137'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '137'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '137')
WHERE clave = '21C071L';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071M
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('134')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('134')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('134')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('134')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('134'))
WHERE ali_fanexod.clave IN ('21C071M');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071N
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '156'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '156'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '156'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '156'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '156')
WHERE clave = '21C071N';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071O
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '171'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '171'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '171'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '171'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '171')
WHERE clave = '21C071O';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071P
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '166'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '166'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '166'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '166'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '166')
WHERE clave = '21C071P';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071Q
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '172'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '172'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '172'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '172'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '172')
WHERE clave = '21C071Q';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071R
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '163'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '163'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '163'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '163'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '163')
WHERE ater.ali_fanexod.clave = '21C071R';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071S
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '174'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '174'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '174'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '174'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '174')
WHERE ater.ali_fanexod.clave = '21C071S';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071T
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '175'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '175'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '175'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '175'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '175')
WHERE ater.ali_fanexod.clave = '21C071T';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071U
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '114'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '114'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '114'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '114'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '114')
WHERE ater.ali_fanexod.clave = '21C071U';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071V
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '128'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '128'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '128'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '128'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '128')
WHERE clave = '21C071V';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071W
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '177'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '177'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '177'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '177'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '177')
WHERE clave = '21C071W';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071X
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '173'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '173'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '173'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '173'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '173')
WHERE ater.ali_fanexod.clave = '21C071W';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071Z
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138')
WHERE ater.ali_fanexod.clave = '21C071Z';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C071
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C071A','21C071B','21C071C','21C071D','21C071E','21C071F','21C071G','21C071H','21C071I','21C071L','21C071M','21C071N','21C071O','21C071P','21C071Q','21C071R','21C071S','21C071T','21C071U','21C071V','21C071W','21C071X','21C071Y','21C071Z')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C071A','21C071B','21C071C','21C071D','21C071E','21C071F','21C071G','21C071H','21C071I','21C071L','21C071M','21C071N','21C071O','21C071P','21C071Q','21C071R','21C071S','21C071T','21C071U','21C071V','21C071W','21C071X','21C071Y','21C071Z')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C071A','21C071B','21C071C','21C071D','21C071E','21C071F','21C071G','21C071H','21C071I','21C071L','21C071M','21C071N','21C071O','21C071P','21C071Q','21C071R','21C071S','21C071T','21C071U','21C071V','21C071W','21C071X','21C071Y','21C071Z')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C071A','21C071B','21C071C','21C071D','21C071E','21C071F','21C071G','21C071H','21C071I','21C071L','21C071M','21C071N','21C071O','21C071P','21C071Q','21C071R','21C071S','21C071T','21C071U','21C071V','21C071W','21C071X','21C071Y','21C071Z')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C071A','21C071B','21C071C','21C071D','21C071E','21C071F','21C071G','21C071H','21C071I','21C071L','21C071M','21C071N','21C071O','21C071P','21C071Q','21C071R','21C071S','21C071T','21C071U','21C071V','21C071W','21C071X','21C071Y','21C071Z'))
WHERE clave = '21C071';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '311'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '311'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '311'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '311'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '311')
WHERE clave = '21C072A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '138')
WHERE ater.ali_fanexod.clave = '21C072B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '149'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '149'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '149'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '149'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '149')
WHERE ater.ali_fanexod.clave = '21C072C';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072E
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '143'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '143'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '143'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '143'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '143')
WHERE ater.ali_fanexod.clave = '21C072E';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072G
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '133'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '133'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '133'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '133'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '133')
WHERE clave = '21C072G';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072N
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '159'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '159'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '159'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '159'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '159')
WHERE clave = '21C072N';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072O
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('145')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('145')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('145')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('145')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('145'))
WHERE ater.ali_fanexod.clave IN ('21C072O');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072P
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('601')),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('601')),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('601')),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('601')),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id IN ('601'))
WHERE ali_fanexod.clave IN ('21C072P');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C072
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C072A','21C072B','21C072C','21C072D','21C072E','21C072F','21C072G','21C072N','21C072O','21C072P')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C072A','21C072B','21C072C','21C072D','21C072E','21C072F','21C072G','21C072N','21C072O','21C072P')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C072A','21C072B','21C072C','21C072D','21C072E','21C072F','21C072G','21C072N','21C072O','21C072P')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C072A','21C072B','21C072C','21C072D','21C072E','21C072F','21C072G','21C072N','21C072O','21C072P')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C072A','21C072B','21C072C','21C072D','21C072E','21C072F','21C072G','21C072N','21C072O','21C072P'))
WHERE clave = '21C072';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C073A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '207'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '207'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '207'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '207'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '207')
WHERE clave = '21C073A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C073B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '215'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '215'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '215'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '215'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '215')
WHERE clave = '21C073B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C073
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C073A', '21C073B')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C073A', '21C073B')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C073A', '21C073B')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C073A', '21C073B')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C073A', '21C073B'))
WHERE clave IN ('21C073');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C074B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '317'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '317'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '317'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '317'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '317')
WHERE ater.ali_fanexod.clave = '21C074B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C074C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '364'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '364'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '364'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '364'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '364')
WHERE clave = '21C074C';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C074
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C074A', '21C074B', '21C074C')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C074A', '21C074B', '21C074C')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C074A', '21C074B', '21C074C')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C074A', '21C074B', '21C074C')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C074A', '21C074B', '21C074C'))
WHERE clave = '21C074';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C076A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '416'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '416'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '416'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '416'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '416')
WHERE clave = '21C076A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C076B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '402'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '402'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '402'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '402'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '402')
WHERE clave = '21C076B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C076C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '423'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '423'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '423'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '423'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '423')
WHERE clave = '21C076C';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C076D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '410'),
	martes = (SELECT SUM(martes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '410'),
	miercoles = (SELECT SUM(miercoles) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '410'),
	jueves = (SELECT SUM(jueves) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '410'),
	viernes = (SELECT SUM(viernes) FROM ater.dep_plazo_24104 AS dep_plazo_24104 WHERE dep_plazo_24104.id = '410')
WHERE ater.ali_fanexod.clave = '21C076D';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C076
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.CLAVE IN ('21C076A','21C076B','21C076C','21C076D','21C076E','21C076F')),
	MARTES = (SELECT SUM(MARTES) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.CLAVE IN ('21C076A','21C076B','21C076C','21C076D','21C076E','21C076F')),
	MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.CLAVE IN ('21C076A','21C076B','21C076C','21C076D','21C076E','21C076F')),
	JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.CLAVE IN ('21C076A','21C076B','21C076C','21C076D','21C076E','21C076F')),
	VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.CLAVE IN ('21C076A','21C076B','21C076C','21C076D','21C076E','21C076F'))
WHERE CLAVE = '21C076';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C077
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C077A', '21C077B')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C077A', '21C077B')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C077A', '21C077B')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C077A', '21C077B')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21C077A', '21C077B'))
WHERE clave = '21C077';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C079
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C080')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C080')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C080')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C080')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C080'))
WHERE clave IN ('21C079');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('115')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('115')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('115')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('115')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('115'))
WHERE ater.ali_fanexod.clave IN ('21D071A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('157')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('157')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('157')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('157')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('157'))
WHERE ater.ali_fanexod.clave IN ('21D071B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('151')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('151')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('151')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('151')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('151'))
WHERE ater.ali_fanexod.clave IN ('21D071C');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('105')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('105')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('105')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('105')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('105'))
WHERE ater.ali_fanexod.clave IN ('21D071D');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071E
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('108')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('108')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('108')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('108')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('108'))
WHERE ater.ali_fanexod.clave IN ('21D071E');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071G
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '104') + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '104'),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '104') + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '104'),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '104') + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '104'),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '104') + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '104'),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '104') + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '104')
WHERE clave = '21D071G';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071H
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.op_mesa_24108 AS op_mesa_24108 WHERE op_mesa_24108.cod IN ('102')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.op_mesa_24107 AS op_mesa_24107 WHERE op_mesa_24107.id IN ('102')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.op_mesa_24108 AS op_mesa_24108 WHERE op_mesa_24108.cod IN ('102')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.op_mesa_24107 AS op_mesa_24107 WHERE op_mesa_24107.id IN ('102')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.op_mesa_24108 AS op_mesa_24108 WHERE op_mesa_24108.cod IN ('102')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.op_mesa_24107 AS op_mesa_24107 WHERE op_mesa_24107.id IN ('102')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.op_mesa_24108 AS op_mesa_24108 WHERE op_mesa_24108.cod IN ('102')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.op_mesa_24107 AS op_mesa_24107 WHERE op_mesa_24107.id IN ('102')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.op_mesa_24108 AS op_mesa_24108 WHERE op_mesa_24108.cod IN ('102')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.op_mesa_24107 AS op_mesa_24107 WHERE op_mesa_24107.id IN ('102'))
WHERE ater.ali_fanexod.clave IN ('21D071H');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071I
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('116')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('116')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('116')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('116')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('116'))
WHERE ali_fanexod.clave IN ('21D071I');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071J
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('190')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('190')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('190')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('190')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('190'))
WHERE ater.ali_fanexod.clave IN ('21D071J');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071L
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('137')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('137')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('137')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('137')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('137'))
WHERE ater.ali_fanexod.clave IN ('21D071L');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071M
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('134')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('134')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('134')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('134')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('134'))
WHERE ater.ali_fanexod.clave IN ('21D071M');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071N
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('156')) + (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('156')),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('156')) + (SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('156')),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('156')) + (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('156')),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('156')) + (SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('156')),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('156')) + (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('156'))
WHERE ali_fanexod.clave IN ('21D071N');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071O
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('171')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('171')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('171')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('171')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('171'))
WHERE ater.ali_fanexod.clave IN ('21D071O');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071P
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('166')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('166')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('166')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('166')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('166'))
WHERE ater.ali_fanexod.clave IN ('21D071P');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071Q
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('172')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('172')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('172')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('172')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('172'))
WHERE ater.ali_fanexod.clave IN ('21D071Q');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071R
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '163'),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '163'),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '163'),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '163'),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '163')
WHERE ater.ali_fanexod.clave = '21D071R';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071S
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('174')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('174')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('174')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('174')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('174'))
WHERE ater.ali_fanexod.clave IN ('21D071S');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071T
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('175')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('175')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('175')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('175')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('175'))
WHERE ater.ali_fanexod.clave IN ('21D071T');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071U
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('114')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('114')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('114')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('114')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('114'))
WHERE ater.ali_fanexod.clave IN ('21D071U');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071V
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('128')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('128')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('128')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('128')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('128'))
WHERE ater.ali_fanexod.clave IN ('21D071V');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071W
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('177')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('177')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('177')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('177')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('177'))
WHERE ali_fanexod.clave IN ('21D071W');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071X
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('173')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('173')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('173')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('173')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('173'))
WHERE ater.ali_fanexod.clave IN ('21D071X');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071Y
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('191')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('191')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('191')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('191')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('191'))
WHERE ater.ali_fanexod.clave IN ('21D071Y');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071Z
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT COALESCE(SUM(lunes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '138') +
		   (SELECT COALESCE(SUM(lunes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '138'),
	martes = (SELECT COALESCE(SUM(martes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '138') +
			(SELECT COALESCE(SUM(martes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '138'),
	miercoles = (SELECT COALESCE(SUM(miercoles), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '138') +
			   (SELECT COALESCE(SUM(miercoles), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '138'),
	jueves = (SELECT COALESCE(SUM(jueves), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '138') +
			(SELECT COALESCE(SUM(jueves), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '138'),
	viernes = (SELECT COALESCE(SUM(viernes), 0) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '138') +
			 (SELECT COALESCE(SUM(viernes), 0) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco = '138')
WHERE clave = '21D071Z';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D071
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071A','21D071B','21D071C','21D071D','21D071E','21D071F','21D071G','21D071H','21D071I','21D071J','21D071K','21D071L','21D071M','21D071N','21D071O','21D071P','21D071Q','21D071R','21D071S','21D071T','21D071U','21D071V','21D071W','21D071X','21D071Y','21D071Z')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071A','21D071B','21D071C','21D071D','21D071E','21D071F','21D071G','21D071H','21D071I','21D071J','21D071K','21D071L','21D071M','21D071N','21D071O','21D071P','21D071Q','21D071R','21D071S','21D071T','21D071U','21D071V','21D071W','21D071X','21D071Y','21D071Z')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071A','21D071B','21D071C','21D071D','21D071E','21D071F','21D071G','21D071H','21D071I','21D071J','21D071K','21D071L','21D071M','21D071N','21D071O','21D071P','21D071Q','21D071R','21D071S','21D071T','21D071U','21D071V','21D071W','21D071X','21D071Y','21D071Z')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071A','21D071B','21D071C','21D071D','21D071E','21D071F','21D071G','21D071H','21D071I','21D071J','21D071K','21D071L','21D071M','21D071N','21D071O','21D071P','21D071Q','21D071R','21D071S','21D071T','21D071U','21D071V','21D071W','21D071X','21D071Y','21D071Z')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071A','21D071B','21D071C','21D071D','21D071E','21D071F','21D071G','21D071H','21D071I','21D071J','21D071K','21D071L','21D071M','21D071N','21D071O','21D071P','21D071Q','21D071R','21D071S','21D071T','21D071U','21D071V','21D071W','21D071X','21D071Y','21D071Z'))
WHERE ali_fanexod.clave = '21D071';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('194')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('194')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('194')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('194')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('194'))
WHERE ater.ali_fanexod.clave IN ('21D072A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('196')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('196')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('196')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('196')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('196'))
WHERE ater.ali_fanexod.clave IN ('21D072B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('141')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('141')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('141')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('141')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('141'))
WHERE ali_fanexod.clave IN ('21D072C');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('003')) + (SELECT SUM(lunes) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('003')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('003')) + (SELECT SUM(martes) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('003')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('003')) + (SELECT SUM(miercoles) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('003')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('003')) + (SELECT SUM(jueves) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('003')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('003')) + (SELECT SUM(viernes) FROM ater.dep_vista_241021 AS dep_vista WHERE dep_vista.id_banco IN ('003'))
WHERE clave IN ('21D072D');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072G
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('008')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('008')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('008')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('008')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('008'))
WHERE ater.ali_fanexod.clave IN ('21D072G');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072H
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('144')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('144')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('144')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('144')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('144'))
WHERE ater.ali_fanexod.clave IN ('21D072H');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072L
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('150')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('150')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('150')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('150')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('150'))
WHERE ater.ali_fanexod.clave IN ('21D072L');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072M
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('133')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('133')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('133')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('133')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('133'))
WHERE ater.ali_fanexod.clave IN ('21D072M');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072N
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('159')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('159')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('159')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('159')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('159'))
WHERE ater.ali_fanexod.clave IN ('21D072N');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072O
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('145')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('145')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('145')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('145')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('145'))
WHERE ater.ali_fanexod.clave IN ('21D072O');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072P
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('601')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('601')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('601')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('601')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('601'))
WHERE ali_fanexod.clave IN ('21D072P');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D072
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_fanexod AS ali_fanexod WHERE CLAVE IN ('21D072A','21D072B','21D072C','21D072D','21D072P','21D072F','21D072G','21D072H','21D072O','21D072J','21D072K','21D072L','21D072M','21D072N')),
	MARTES = (SELECT SUM(MARTES) FROM ater.ali_fanexod AS ali_fanexod WHERE CLAVE IN ('21D072A','21D072B','21D072C','21D072D','21D072P','21D072F','21D072G','21D072H','21D072O','21D072J','21D072K','21D072L','21D072M','21D072N')),
	MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_fanexod AS ali_fanexod WHERE CLAVE IN ('21D072A','21D072B','21D072C','21D072D','21D072P','21D072F','21D072G','21D072H','21D072O','21D072J','21D072K','21D072L','21D072M','21D072N')),
	JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_fanexod AS ali_fanexod WHERE CLAVE IN ('21D072A','21D072B','21D072C','21D072D','21D072P','21D072F','21D072G','21D072H','21D072O','21D072J','21D072K','21D072L','21D072M','21D072N')),
	VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_fanexod AS ali_fanexod WHERE CLAVE IN ('21D072A','21D072B','21D072C','21D072D','21D072P','21D072F','21D072G','21D072H','21D072O','21D072J','21D072K','21D072L','21D072M','21D072N'))
WHERE CLAVE = '21D072';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D073B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('218')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('218')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('218')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('218')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('218'))
WHERE ater.ali_fanexod.clave IN ('21D073B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D073
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D073A', '21D073B')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D073A', '21D073B')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D073A', '21D073B')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D073A', '21D073B')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D073A', '21D073B'))
WHERE clave IN ('21D073');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D074A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('323')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('323')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('323')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('323')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('323'))
WHERE ater.ali_fanexod.clave IN ('21D074A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D074C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('315')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('315')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('315')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('315')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('315'))
WHERE ater.ali_fanexod.clave IN ('21D074C');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D074
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D074A','21D074B','21D074C','21D074D')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D074A','21D074B','21D074C','21D074D')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D074A','21D074B','21D074C','21D074D')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D074A','21D074B','21D074C','21D074D')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D074A','21D074B','21D074C','21D074D'))
WHERE clave = '21D074';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D076A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('410')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('410')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('410')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('410')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('410'))
WHERE ater.ali_fanexod.clave IN ('21D076A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D076B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('425')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('425')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('425')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('425')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('425'))
WHERE ater.ali_fanexod.clave IN ('21D076B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D076
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D076A', '21D076B')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D076A', '21D076B')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D076A', '21D076B')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D076A', '21D076B')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D076A', '21D076B'))
WHERE clave IN ('21D076');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D079A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('149')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('149')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('149')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('149')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('149'))
WHERE ater.ali_fanexod.clave IN ('21D079A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D079
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D079A')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D079A')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D079A')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D079A')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod_sub WHERE ali_fanexod_sub.clave IN ('21D079A'))
WHERE clave IN ('21D079');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D080A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('146')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('146')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('146')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('146')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('146'))
WHERE ater.ali_fanexod.clave IN ('21D080A');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D080B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('168')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('168')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('168')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('168')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('168'))
WHERE ater.ali_fanexod.clave IN ('21D080B');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D080C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '169'),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '169'),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '169'),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '169'),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod = '169')
WHERE ater.ali_fanexod.clave = '21D080C';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D080D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('178')),
  martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('178')),
  miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('178')),
  jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('178')),
  viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('178'))
WHERE clave = '21D080D';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D080
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D080A','21D080B','21D080C','21D080D')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D080A','21D080B','21D080C','21D080D')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D080A','21D080B','21D080C','21D080D')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D080A','21D080B','21D080C','21D080D')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D080A','21D080B','21D080C','21D080D'))
WHERE clave = '21D080';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D081A
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '146'),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '146'),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '146'),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '146'),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '146')
WHERE clave = '21D081A';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D081B
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '168'),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '168'),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '168'),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '168'),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '168')
WHERE ali_fanexod.clave = '21D081B';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D081C
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '169'),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '169'),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '169'),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '169'),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id = '169')
WHERE clave = '21D081C';'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D081D
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id IN ('000')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id IN ('000')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id IN ('000')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id IN ('000')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24107 AS op_mesa WHERE op_mesa.id IN ('000'))
WHERE clave IN ('21D081D');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D081E
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('001')),
	martes = (SELECT SUM(martes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('001')),
	miercoles = (SELECT SUM(miercoles) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('001')),
	jueves = (SELECT SUM(jueves) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('001')),
	viernes = (SELECT SUM(viernes) FROM ater.op_mesa_24108 AS op_mesa WHERE op_mesa.cod IN ('001'))
WHERE ater.ali_fanexod.clave IN ('21D081E');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D081
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D081A','21D081B','21D081C','21D081D','21D081E')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D081A','21D081B','21D081C','21D081D','21D081E')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D081A','21D081B','21D081C','21D081D','21D081E')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D081A','21D081B','21D081C','21D081D','21D081E')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D081A','21D081B','21D081C','21D081D','21D081E'))
WHERE ali_fanexod.clave IN ('21D081');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D070
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071', '21D072', '21D073', '21D074')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071', '21D072', '21D073', '21D074')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071', '21D072', '21D073', '21D074')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071', '21D072', '21D073', '21D074')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D071', '21D072', '21D073', '21D074'))
WHERE ali_fanexod.clave IN ('21D070');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21D000
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D070','21D076','21D077','21D078','21D079','21D080','21D081')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D070','21D076','21D077','21D078','21D079','21D080','21D081')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D070','21D076','21D077','21D078','21D079','21D080','21D081')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D070','21D076','21D077','21D078','21D079','21D080','21D081')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21D070','21D076','21D077','21D078','21D079','21D080','21D081'))
WHERE clave IN ('21D000');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C070
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21C071','21C072','21C073','21C074')),
	MARTES = (SELECT SUM(MARTES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21C071','21C072','21C073','21C074')),
	MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21C071','21C072','21C073','21C074')),
	JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21C071','21C072','21C073','21C074')),
	VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.CLAVE IN ('21C071','21C072','21C073','21C074'))
WHERE CLAVE IN ('21C070');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21C000
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C070','21C076','21C077','21C078','21C079','21C080')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C070','21C076','21C077','21C078','21C079','21C080')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C070','21C076','21C077','21C078','21C079','21C080')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C070','21C076','21C077','21C078','21C079','21C080')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C070','21C076','21C077','21C078','21C079','21C080'))
WHERE ali_fanexod.clave IN ('21C000');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A070
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071', '21A072', '21A073', '21A074')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071', '21A072', '21A073', '21A074')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071', '21A072', '21A073', '21A074')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071', '21A072', '21A073', '21A074')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A071', '21A072', '21A073', '21A074'))
WHERE ali_fanexod.clave IN ('21A070');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_21A000
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A070','21A076','21A077','21A078','21A079','21A080','21A081')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A070','21A076','21A077','21A078','21A079','21A080','21A081')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A070','21A076','21A077','21A078','21A079','21A080','21A081')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A070','21A076','21A077','21A078','21A079','21A080','21A081')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A070','21A076','21A077','21A078','21A079','21A080','21A081'))
WHERE clave IN ('21A000');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_219999
	sql_query_deftxt = f'''UPDATE ater.ali_fanexod
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000','21B000','21C000','21D000')),
	martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000','21B000','21C000','21D000')),
	miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000','21B000','21C000','21D000')),
	jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000','21B000','21C000','21D000')),
	viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000','21B000','21C000','21D000'))
WHERE ali_fanexod.clave IN ('219999');'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_TOTAL
	sql_query_deftxt = f'''UPDATE ATER.ALI_FANEXOD AS ali_fanexod
SET TOTAL = (
	SELECT LUNES + MARTES + MIERCOLES + JUEVES + VIERNES
	FROM ATER.ALI_FANEXOD AS ali_fanexod_sub
	WHERE ali_fanexod_sub.CLAVE = ali_fanexod.CLAVE
);'''
	hook.run(sql_query_deftxt)

	# FORMULA_FANEXOD_PROM
	varCantDiasHabiles = Variable.get('varCantDiasHabiles')

	sql_query_deftxt = f'''UPDATE ATER.ALI_FANEXOD AS ali_fanexod
SET PROMEDIO = (ali_fanexod.TOTAL) / {varCantDiasHabiles};'''
	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': days_ago(1)
}

dag = DAG(dag_id='FANEXOD',
		  default_args=default_args,
		  schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

varFechaInicio_task = PythonOperator(
    task_id='varFechaInicio_task',
    python_callable=varFechaInicio,
    provide_context=True,
    dag=dag
)

varFechaFin_task = PythonOperator(
    task_id='varFechaInicio_task',
    python_callable=varFechaFin,
    provide_context=True,
    dag=dag
)

varCantDiasHabiles_task = PythonOperator(
    task_id='varCantDiasHabiles_task',
    python_callable=varCantDiasHabiles,
    provide_context=True,
    dag=dag
)

ALI_FANEXOD_task = PythonOperator(
	task_id='ALI_FANEXOD_task',
	python_callable=ALI_FANEXOD,
	provide_context=True,
	dag=dag
)

ACTUALIZA_FANEXOD_task = PythonOperator(
	task_id='ACTUALIZA_FANEXOD_task',
	python_callable=ACTUALIZA_FANEXOD,
	provide_context=True,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
varFechaInicio_task >> varFechaFin_task >> varCantDiasHabiles_task >> ALI_FANEXOD_task >> ACTUALIZA_FANEXOD_task
