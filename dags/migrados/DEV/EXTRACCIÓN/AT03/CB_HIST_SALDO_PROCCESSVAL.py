from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
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
	value = 'CB_HIST_SALDO'
	Variable.set('vSrcNombreTablaOds', serialize_value(value))

def vOdsFechaInicio(**kwargs):
	value = '01/01/1801'
	Variable.set('vOdsFechaInicio', serialize_value(value))

def vOdsFechaFin(**kwargs):
	value = '01/01/1801'
	Variable.set('vOdsFechaFin', serialize_value(value))

def vSrcFormatoFecha(**kwargs):
	value = 'MM/DD/YYYY'
	Variable.set('vSrcFormatoFecha', serialize_value(value))

def PROC_UPD_CONFIG_ODS(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	vSrcNombreTablaOds = get_variable('vSrcNombreTablaOds')
	vOdsFechaInicio = get_variable('vOdsFechaInicio')
	vOdsFechaFin = get_variable('vOdsFechaFin')
	vSrcFormatoFecha = get_variable('vSrcFormatoFecha')

	# UPD_CONFIG_ONE
	sql_query_deftxt = f'''UPDATE ODS.BAN_CONFIG_ODS AS ban_config_ods
	SET 
		COO_END_DATE = TO_DATE(CASE WHEN '{vOdsFechaFin}' = '01/01/1801' THEN '{vOdsFechaInicio}' ELSE '{vOdsFechaFin}' END, '{vSrcFormatoFecha}'),
		COO_START_DATE = TO_DATE('{vOdsFechaInicio}', '{vSrcFormatoFecha}'),
		COO_STATUS_CARGA = 'POR PROCESAR'
	WHERE 
		COO_UPDATE_DATE_USER = 'NOUSAR' 
		AND COO_SOURCE = '{vSrcNombreTablaOds}'
		AND '{vOdsFechaInicio}' <> '01/01/1801'
		AND TO_DATE('{vOdsFechaInicio}', '{vSrcFormatoFecha}') <= TO_DATE(CASE WHEN '{vOdsFechaFin}' = '01/01/1801' THEN '{vOdsFechaInicio}' ELSE '{vOdsFechaFin}' END, '{vSrcFormatoFecha}');'''
	logger.info("Accion a ejecutarse: UPD_CONFIG_ONE")
	hook.run(sql_query_deftxt)
	logger.info("Accion: UPD_CONFIG_ONE, ejecutada exitosamente")

def vSrcFechaCarga(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	vSrcFormatoFecha = get_variable('vSrcFormatoFecha')
	vSrcNombreTablaOds = get_variable('vSrcNombreTablaOds')

	sql_query = f'''SELECT COALESCE(TO_CHAR(MIN(bcs.cds_date), '{vSrcFormatoFecha}'), '01/01/1801')
	FROM ods.BAN_CONFIG_ODS_DATE AS bcs INNER JOIN ods.BAN_CONFIG_ODS AS o ON o.coo_source = bcs.cds_source
	WHERE bcs.cds_source = '{vSrcNombreTablaOds}' AND bcs.cds_status_carga = 'POR PROCESAR'
	AND bcs.cds_date BETWEEN o.coo_start_date AND o.coo_end_date;'''

	result = hook.get_records(sql_query)
	Variable.set('vSrcFechaCarga', serialize_value(result[0][0]))

def PROC_UPD_STATUS_BAN_CONFIG_ODS(**kwargs):
	hook = PostgresHook(postgres_conn_id='ods')

	vSrcNombreTablaOds = get_variable('vSrcNombreTablaOds')

	# Actualiza el status en la tabla BAN_CONFIG_ODS
	sql_query_deftxt = f'''DO $$
	DECLARE
		contEstatus INTEGER;
		nuevoEstatus VARCHAR(255);
	BEGIN
		SELECT COUNT(1) INTO contEstatus 
		FROM ods.BAN_CONFIG_ODS_DATE AS bcsd
		INNER JOIN ods.BAN_CONFIG_ODS AS bcs ON bcs.coo_source = bcsd.cds_source
		WHERE bcsd.cds_status_carga NOT IN ('PROCESADO', 'NO HABIL', 'REPROCESO MANUAL')
		AND bcs.coo_source = '{vSrcNombreTablaOds}'
		AND bcsd.cds_date BETWEEN bcs.coo_start_date AND bcs.coo_end_date;

		IF (contEstatus = 0) THEN
			SELECT COUNT(1) INTO contEstatus
			FROM ods.BAN_CONFIG_ODS_DATE AS bcsd
			INNER JOIN ods.BAN_CONFIG_ODS AS bcs ON bcs.coo_source = bcsd.cds_source
			WHERE bcsd.cds_status_carga = 'NO HABIL'
			AND bcs.coo_source = '{vSrcNombreTablaOds}'
			AND bcsd.cds_date = bcs.coo_end_date;

			IF (contEstatus = 0) THEN
				nuevoEstatus := 'PROCESADO';
			ELSE
				nuevoEstatus := 'NO HABIL';
			END IF;
		ELSE
			SELECT COUNT(1) INTO contEstatus 
			FROM ods.BAN_CONFIG_ODS_DATE AS bcsd
			INNER JOIN ods.BAN_CONFIG_ODS AS bcs ON bcs.coo_source = bcsd.cds_source
			WHERE bcsd.cds_status_carga = 'ERROR'
			AND bcs.coo_source = '{vSrcNombreTablaOds}'
			AND bcsd.cds_date BETWEEN bcs.coo_start_date AND bcs.coo_end_date;

			IF (contEstatus = 0) THEN
				nuevoEstatus := 'POR PROCESAR';
			ELSE
				nuevoEstatus := 'ERROR';
			END IF;
		END IF;

		IF nuevoEstatus IS NOT NULL THEN
			UPDATE ods.BAN_CONFIG_ODS
			SET COO_STATUS_CARGA = nuevoEstatus
			WHERE COO_SOURCE = '{vSrcNombreTablaOds}'
			AND COO_STATUS_CARGA != nuevoEstatus;
		END IF;
	END $$;'''
	logger.info("Accion a ejecutarse: Actualiza el status en la tabla BAN_CONFIG_ODS")
	hook.run(sql_query_deftxt)
	logger.info("Accion: Actualiza el status en la tabla BAN_CONFIG_ODS, ejecutada exitosamente")


###### DEFINICION DEL DAG ######

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_CON_PROCCESSVAL_CB_HIST_SALDO_ODS',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False,
		  tags=['Principal'])

vSrcNombreTablaOds_task = PythonOperator(
	task_id='vSrcNombreTablaOds_task',
	python_callable=vSrcNombreTablaOds,
	dag=dag
)

vOdsFechaInicio_task = PythonOperator(
	task_id='vOdsFechaInicio_task',
	python_callable=vOdsFechaInicio,
	dag=dag
)

vOdsFechaFin_task = PythonOperator(
	task_id='vOdsFechaFin_task',
	python_callable=vOdsFechaFin,
	dag=dag
)

vSrcFormatoFecha_task = PythonOperator(
	task_id='vSrcFormatoFecha_task',
	python_callable=vSrcFormatoFecha,
	dag=dag
)

PROC_UPD_CONFIG_ODS_task = PythonOperator(
	task_id='PROC_UPD_CONFIG_ODS_task',
	python_callable=PROC_UPD_CONFIG_ODS,
	dag=dag
)

vSrcFechaCarga_task = PythonOperator(
	task_id='vSrcFechaCarga_task',
	python_callable=vSrcFechaCarga,
	dag=dag
)

PKG_CON_LOAD_CB_HIST_SALDO_ODS_DIARIA_task = TriggerDagRunOperator(
	task_id='PKG_CON_LOAD_CB_HIST_SALDO_ODS_DIARIA_task',
	trigger_dag_id='PKG_CON_LOAD_CB_HIST_SALDO_ODS_DIARIA_version2',
	wait_for_completion=True,
	dag=dag
)

PROC_UPD_STATUS_BAN_CONFIG_ODS_task = PythonOperator(
	task_id='PROC_UPD_STATUS_BAN_CONFIG_ODS_task',
	python_callable=PROC_UPD_STATUS_BAN_CONFIG_ODS,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
vSrcNombreTablaOds_task >> vOdsFechaInicio_task >> vOdsFechaFin_task >> vSrcFormatoFecha_task >> PROC_UPD_CONFIG_ODS_task >> vSrcFechaCarga_task >> PKG_CON_LOAD_CB_HIST_SALDO_ODS_DIARIA_task >> PROC_UPD_STATUS_BAN_CONFIG_ODS_task
