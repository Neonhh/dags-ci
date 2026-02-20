import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insertarNombreCamposSaldosPuntuales(**context):
	logger = logging.getLogger(__name__)

	vStgNombreTablaStg = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgNombreTablaStg')
	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
DO $$
BEGIN
    {{ getCampos("INSERT INTO BAN_SM_CAMPOS VALUES ({{ vStgNombreTablaStg }}, '[CLAVE_PROD]', '[CLAVE_DESC]', 'CLAVE_PRODUCTO', [CLAVE_PROD_NUM], NULL) ON CONFLICT DO NOTHING; ", "CLAVE_PRODUCTO", "", "", "", "    ") }}
    {{ getCampos("INSERT INTO BAN_SM_CAMPOS VALUES ({{ vStgNombreTablaStg }}, '[SALDO_PUNT]', '[SALDO_DESC]', 'SALDO_PUNTUAL', [SALDO_NUM], CASE WHEN '[FLAG_REPRECIO]' IS NULL THEN NULL ELSE 'Y' END) ON CONFLICT DO NOTHING; ", "SALDO_PUNTUAL", "", "", "", "    ") }}
    {{ getCampos("INSERT INTO BAN_SM_CAMPOS VALUES ({{ vStgNombreTablaStg }}, '[FECHA_REPRECIO]', '[FECHA_REPRECIO_DESC]', 'FECHA_REPRECIO', 1, NULL) ON CONFLICT DO NOTHING; ", "FECHA_REPRECIO", "", "", "", "    ") }}
    {{ getCampos("INSERT INTO BAN_SM_CAMPOS VALUES ({{ vStgNombreTablaStg }}, '[FECHA_PROX_REPRE]', '[FECHA_PROX_REPRE_DESC]', 'FECHA_PROX_REPRE', 1, NULL) ON CONFLICT DO NOTHING; ", "FECHA_PROX_REPRE", "", "", "", "    ") }}
    {{ getCampos("INSERT INTO BAN_SM_CAMPOS VALUES ({{ vStgNombreTablaStg }}, '[TIPO_INTERES]', '[TIPO_INTERES_DESC]', 'TIPO_INTERES', 1, NULL) ON CONFLICT DO NOTHING; ", "TIPO_INTERES", "", "", "", "    ") }}
END $$;
""".replace("{{ vStgNombreTablaStg }}", f"'{vStgNombreTablaStg}'"))  # Replacing here to avoid issues with single quotes in SQL

	query = template.render(
		getCampos=getCampos,
		vStgNombreTablaStg=vStgNombreTablaStg,
	)

	# Ensure STG schema is used by default for unqualified table names
	query = 'SET search_path TO STG, public;\n' + query

	if query.strip():
		logger.info(f'Executing query: {query}')
		result = hook.run(query)
		logger.info(f'Query executed successfully. Result: {result}')
	else:
		logger.info('No query to execute. Query is empty.')
