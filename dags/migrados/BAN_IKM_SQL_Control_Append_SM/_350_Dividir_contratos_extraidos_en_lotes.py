import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def dividirContratosExtraidosEnLotes(**context):
	logger = logging.getLogger(__name__)

	vStgSmLotSize = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgSmLotSize')
	vNombreTablaAuxSM = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaAuxSM')
	vNombreTablaAuxSMExt = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaAuxSMExt')
	vStgSmFlagDiaFeriado = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgSmFlagDiaFeriado')
	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
DO $$
DECLARE
    vRowCount NUMERIC := 0;
    vNumLotes NUMERIC := 0;
BEGIN
    SELECT COUNT(1) INTO vRowCount
    FROM {{ vNombreTablaAuxSMExt }} AS tab_aux_ext;
    
    vNumLotes := CEIL(vRowCount / {{ vStgSmLotSize }});
    
    INSERT INTO {{ vNombreTablaAuxSM }}
    ( 
        LOTE, 
        FECHA,
        {{ getCampos("[CLAVE_PROD_SM]", "CLAVE_PRODUCTO", ",", "", "", "        ") }}
    {% if vStgSmFlagDiaFeriado == "n" %}
        {{ getCampos("[SALDO_PUNT]", "SALDO_PUNTUAL", ",", ",", "", "        ") }}
        {{ getCampos("[TIPO_INTERES]", "TIPO_INTERES", "", ",", "", "        ") }}
        {{ getCampos("[FECHA_REPRECIO]", "FECHA_REPRECIO", "", ",", "", "        ") }}
        {{ getCampos("[FECHA_PROX_REPRE]", "FECHA_PROX_REPRE", "", ",", "", "        ") }}
    {% endif %}
    )
    SELECT NTILE(CAST(vNumLotes AS INTEGER)) OVER (ORDER BY tab_aux_ext.numero_fila ASC) AS lote,
           fecha,
    {% if vStgSmFlagDiaFeriado == "y" %}
           {{ getCampos("[CLAVE_PROD_SM]", "CLAVE_PRODUCTO", ",", "", "", "        ") }}
    {% elif vStgSmFlagDiaFeriado == "n" %}
           {{ getCampos("[CLAVE_PROD]", "CLAVE_PRODUCTO", ",", "", ",", "        ") }}
           {{ getCampos("[SALDO_PUNT]", "SALDO_PUNTUAL", ",", "", "", "        ") }}
           {{ getCampos("[TIPO_INTERES]", "TIPO_INTERES", "", ",", "", "        ") }}
           {{ getCampos("[FECHA_REPRECIO]", "FECHA_REPRECIO", "", ",", "", "        ") }}
           {{ getCampos("[FECHA_PROX_REPRE]", "FECHA_PROX_REPRE", "", ",", "", "        ") }}
    {% endif %}
    FROM {{ vNombreTablaAuxSMExt }} AS tab_aux_ext;		
END $$;
""")

	query = template.render(
		vStgSmLotSize=vStgSmLotSize,
		vNombreTablaAuxSM=vNombreTablaAuxSM,
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
