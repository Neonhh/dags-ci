import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def crearTablaNombreCamposSaldosPuntuales(**context):
	logger = logging.getLogger(__name__)

	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
{% set table_name = 'STG.BAN_SM_CAMPOS' %}
CREATE TABLE IF NOT EXISTS {{ table_name }} (
	"TABLA_STG" VARCHAR(50),
	"NOMBRE_CAMPO" VARCHAR(50),
	"DESC_CAMPO" VARCHAR(50),
	"TIPO_CAMPO" VARCHAR(50),
	"NUMERO" NUMERIC(5,0),
	"FLAG_REPRECIO" VARCHAR(1)
);
""")

	query = template.render(
	)

	# Ensure STG schema is used by default for unqualified table names
	query = 'SET search_path TO STG, public;\n' + query

	if query.strip():
		logger.info(f'Executing query: {query}')
		result = hook.run(query)
		logger.info(f'Query executed successfully. Result: {result}')
	else:
		logger.info('No query to execute. Query is empty.')
