import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extraerSaldosPuntualesContratosDiaFeriado(**context):
	logger = logging.getLogger(__name__)

	vNombreTablaSM = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaSM')
	vStgSmFechaProcesar = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgSmFechaProcesar')
	vStgFormatoFecha = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgFormatoFecha')
	vNombreTablaAuxSMExt = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaAuxSMExt')
	vStgSmFlagDiaFeriado = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgSmFlagDiaFeriado')
	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
{% if vStgSmFlagDiaFeriado == "y" %}

CREATE TABLE IF NOT EXISTS {{ vNombreTablaAuxSMExt }} AS
SELECT row_number() OVER () AS numero_fila,
       TO_TIMESTAMP('{{ vStgSmFechaProcesar }}', '{{ vStgFormatoFecha }}') AS fecha,
       {{ getCampos("[CLAVE_PROD_SM]", "CLAVE_PRODUCTO", ",", "", "", "       ") }}
FROM {{ vNombreTablaSM }} AS sm
WHERE contingencia IS NULL
  AND DATE_TRUNC('month', TO_TIMESTAMP(fecha, '{{ vStgFormatoFecha }}')) = 
    CASE 
      WHEN DATE_TRUNC('month', TO_TIMESTAMP('{{ vStgSmFechaProcesar }}', '{{ vStgFormatoFecha }}')) = DATE_TRUNC('month', TO_TIMESTAMP('{{ vStgSmFechaProcesar }}', '{{ vStgFormatoFecha }}')) 
      THEN DATE_TRUNC('month', TO_TIMESTAMP('{{ vStgSmFechaProcesar }}', '{{ vStgFormatoFecha }}') - INTERVAL '1 month')
      ELSE DATE_TRUNC('month', TO_TIMESTAMP('{{ vStgSmFechaProcesar }}', '{{ vStgFormatoFecha }}'))
    END;

{% endif %}
""")

	query = template.render(
		vNombreTablaSM=vNombreTablaSM,
		vStgSmFechaProcesar=vStgSmFechaProcesar,
		vStgFormatoFecha=vStgFormatoFecha,
		vNombreTablaAuxSMExt=vNombreTablaAuxSMExt,
		getCampos=getCampos,
		vStgSmFlagDiaFeriado=vStgSmFlagDiaFeriado,
	)

	# Ensure STG schema is used by default for unqualified table names
	query = 'SET search_path TO STG, public;\n' + query

	if query.strip():
		logger.info(f'Executing query: {query}')
		result = hook.run(query)
		logger.info(f'Query executed successfully. Result: {result}')
	else:
		logger.info('No query to execute. Query is empty.')
