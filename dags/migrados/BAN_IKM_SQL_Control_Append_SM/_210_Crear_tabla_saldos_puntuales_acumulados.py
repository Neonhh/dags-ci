import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def crearTablaSaldosPuntualesAcumulados(**context):
	logger = logging.getLogger(__name__)

	vNombreTablaSM = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaSM')
	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
CREATE TABLE IF NOT EXISTS {{ vNombreTablaSM }} (
      FECHA TIMESTAMP,
      {{ getCampos("[CLAVE_PROD_SM] [CLAVE_DESC]", "CLAVE_PRODUCTO", ",", "", ",", "      ") }}
      {{ getCampos("[SALDO_ACUM] [SALDO_DESC]", "SALDO_PUNTUAL", ",", "", ",", "      ") }}
      {{ getCampos("[SALDO_ULT] [SALDO_DESC]", "SALDO_PUNTUAL", ",", "", ",", "      ") }}
      {{ getCampos("[SALDO_PRIOR] [SALDO_DESC]", "SALDO_PUNTUAL", ",", "", ",", "      ") }}
      {{ getCampos("[SALDO_CURR] [SALDO_DESC]", "SALDO_PUNTUAL", ",", "", ",", "      ") }}
      {{ getCampos("[TIPO_INTERES] [TIPO_INTERES_DESC]", "TIPO_INTERES", "", "", ",", "      ") }}
      {{ getCampos("[FECHA_REPRECIO] [FECHA_REPRECIO_DESC]", "FECHA_REPRECIO", "", "", ",", "      ") }}
      {{ getCampos("[FECHA_PROX_REPRE] [FECHA_PROX_REPRE_DESC]", "FECHA_PROX_REPRE", "", "", ",", "      ") }}
      {{ getCampos("NUM_DIAS_PRIOR NUMERIC(2)", "FECHA_REPRECIO", "", "", ",", "      ") }}
      {{ getCampos("NUM_DIAS_CURR NUMERIC(2)", "FECHA_REPRECIO", "", "", ",", "      ") }}
      "NUM_DIAS" NUMERIC(2),
      "CONTINGENCIA" CHAR(1)
);
""")

	query = template.render(
		vNombreTablaSM=vNombreTablaSM,
		getCampos=getCampos,
	)

	# Ensure STG schema is used by default for unqualified table names
	query = 'SET search_path TO STG, public;\n' + query

	if query.strip():
		logger.info(f'Executing query: {query}')
		result = hook.run(query)
		logger.info(f'Query executed successfully. Result: {result}')
	else:
		logger.info('No query to execute. Query is empty.')
