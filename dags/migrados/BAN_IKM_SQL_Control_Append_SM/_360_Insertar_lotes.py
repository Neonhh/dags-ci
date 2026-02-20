import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insertarLotes(**context):
	logger = logging.getLogger(__name__)

	vNombreTablaAuxSM = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaAuxSM')
	vNombreTablaLotesSM = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaLotesSM')
	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
INSERT INTO {{ vNombreTablaLotesSM }} (LOTE, STATUS)
SELECT DISTINCT LOTE, 'POR PROCESAR' AS STATUS
FROM {{ vNombreTablaAuxSM }}
UNION
SELECT NULL AS LOTE, 'POR PROCESAR' AS STATUS
ORDER BY LOTE ASC;
""")

	query = template.render(
		vNombreTablaAuxSM=vNombreTablaAuxSM,
		vNombreTablaLotesSM=vNombreTablaLotesSM,
	)

	# Ensure STG schema is used by default for unqualified table names
	query = 'SET search_path TO STG, public;\n' + query

	if query.strip():
		logger.info(f'Executing query: {query}')
		result = hook.run(query)
		logger.info(f'Query executed successfully. Result: {result}')
	else:
		logger.info('No query to execute. Query is empty.')
