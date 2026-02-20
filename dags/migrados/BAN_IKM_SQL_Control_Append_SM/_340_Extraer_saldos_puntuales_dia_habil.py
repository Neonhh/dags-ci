import logging
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo, getCampos
from jinja2 import Template
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extraerSaldosPuntualesDiaHabil(**context):
	logger = logging.getLogger(__name__)

	vStgSmFechaProcesar = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgSmFechaProcesar')
	vNombreTablaAuxSMExt = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vNombreTablaAuxSMExt')
	vStgFormatoFecha = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgFormatoFecha')
	vStgFechaCarga = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgFechaCarga')
	vStgUltimoDiaHabilMesAnt = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgUltimoDiaHabilMesAnt')
	vStgSmFlagDiaFeriado = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgSmFlagDiaFeriado')
	vStgPartition = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vStgPartition')
	postgres_conn_id = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='postgres_conn_id')
	vListaCampos = context['ti'].xcom_pull(task_ids='_100_DeclaracionDeVariablesSaldosMedios', key='vListaCampos')

	hook = PostgresHook(postgres_conn_id=postgres_conn_id)

	Campo.vListaCampos = [Campo.from_dict(c) for c in vListaCampos]

	template = Template("""
{% if vStgSmFlagDiaFeriado == "n" %}
CREATE TABLE IF NOT EXISTS {{ vNombreTablaAuxSMExt }} AS
SELECT row_number() OVER () AS numero_fila,
       TO_TIMESTAMP('{{ vStgSmFechaProcesar }}', '{{ vStgFormatoFecha }}') AS fecha,
       {{ getCampos("[CLAVE_PROD]", "CLAVE_PRODUCTO", ",", "", ",", "        ") }}
       {{ getCampos("[SALDO_PUNT]", "SALDO_PUNTUAL", ",", "", "", "        ") }}
       {{ getCampos("[TIPO_INTERES]", "TIPO_INTERES", "", ",", "", "      ") }}
       {{ getCampos("[FECHA_REPRECIO]", "FECHA_REPRECIO", "", ",", "", "        ") }}
       {{ getCampos("[FECHA_PROX_REPRE]", "FECHA_PROX_REPRE", "", ",", "", "        ") }}
FROM (
    SELECT 
        CC.CU_CUENTA AS V_GL_CODE,
        CASE 
            WHEN CC.CU_CUENTA LIKE '1%' OR CC.CU_CUENTA LIKE '2%' OR CC.CU_CUENTA LIKE '3%' OR CC.CU_CUENTA LIKE '6%' OR CC.CU_CUENTA LIKE '7%' OR CC.CU_CUENTA LIKE '8%' THEN 'ENDBAL'
            WHEN CC.CU_CUENTA LIKE '41%' OR CC.CU_CUENTA LIKE '51%' THEN 'INTEREST'
            WHEN CC.CU_CUENTA LIKE '4%' AND CC.CU_CUENTA NOT LIKE '41%' THEN 'NONINTEXP'
            WHEN CC.CU_CUENTA LIKE '5%' AND CC.CU_CUENTA NOT LIKE '51%' THEN 'NONINTINC'
            ELSE '0'
        END AS V_FINANCIAL_ELEMENT_CODE,
        TMPHISSAL.N_AMOUNT_LCY - COALESCE(TMPHISSAL45.N_AMOUNT_LCY, 0) AS N_AMOUNT_LCY,
        TMPHISSAL.N_AMOUNT_ACY - COALESCE(TMPHISSAL45.N_AMOUNT_ACY, 0) AS N_AMOUNT_ACY,
        TMPHISSAL.HI_OFICINA AS V_ORG_UNIT_CODE,
        CASE WHEN CC.CU_ESTADO = 'C' THEN CC.CU_FECHA_ESTADO END AS D_FECHA_ESTADO
    FROM (
        SELECT 
            CC.CU_CUENTA AS HI_CUENTA,
            STG.FU_EQUIVALENCIA_VARCHAR('ODS.CL_OFICINA', 'OF_OFICINA', COALESCE(CBHS.HI_OFICINA, '0')) AS HI_OFICINA,
            SUM(CBHS.HI_SALDO) AS N_AMOUNT_LCY,
            SUM(CASE WHEN CC.CU_MONEDA = 0 THEN CBHS.HI_SALDO WHEN CC.CU_MONEDA != 0 THEN CBHS.HI_SALDO_ME ELSE NULL END) AS N_AMOUNT_ACY,
            MAX(CC.CU_MONEDA) AS V_CCY_CODE
        FROM 
            ODS.CB_CUENTA AS CC
        INNER JOIN 
            ODS.CB_HIST_SALDO AS CBHS ON CC.CU_CUENTA = CBHS.HI_CUENTA
        LEFT JOIN 
            ODS.RE_OFICINA_BANGENTE AS OFIC_BANG ON OFIC_BANG.OB_OFICINA = CBHS.HI_OFICINA AND OFIC_BANG.OB_PRODUCTO = 3
        INNER JOIN 
            ODS.CB_CORTE AS CBCOR ON CBCOR.CO_CORTE = CBHS.HI_CORTE 
                                                AND CBCOR.CO_PERIODO = CBHS.HI_PERIODO 
                                                AND CBCOR.CO_EMPRESA = CBHS.HI_EMPRESA 
                                                AND CBCOR.CO_FECHA_INI = CBHS.HI_FECHA_INI
        WHERE 
            CBCOR.CO_FECHA_INI >= TO_TIMESTAMP('{{ vStgFechaCarga }}', '{{ vStgFormatoFecha }}')
            AND CBCOR.CO_FECHA_FIN <= TO_TIMESTAMP('{{ vStgFechaCarga }}', '{{ vStgFormatoFecha }}')
            AND CBHS.HI_OFICINA <> 0
            AND CBHS.HI_FECHA_INI = TO_TIMESTAMP('{{ vStgFechaCarga }}', '{{ vStgFormatoFecha }}')
        GROUP BY 
            CC.CU_CUENTA,
            STG.FU_EQUIVALENCIA_VARCHAR('ODS.CL_OFICINA', 'OF_OFICINA', COALESCE(CBHS.HI_OFICINA, '0'))
    ) AS TMPHISSAL
    LEFT JOIN (
        SELECT 
            CBC.CU_CUENTA AS HI_CUENTA,
            STG.FU_EQUIVALENCIA_VARCHAR('ODS.CL_OFICINA', 'OF_OFICINA', COALESCE(CBHS.HI_OFICINA, '0')) AS HI_OFICINA,
            SUM(CBHS.HI_SALDO) AS N_AMOUNT_LCY,
            SUM(CASE WHEN CBC.CU_MONEDA = 0 THEN CBHS.HI_SALDO WHEN CBC.CU_MONEDA != 0 THEN CBHS.HI_SALDO_ME ELSE NULL END) AS N_AMOUNT_ACY
        FROM 
            ODS.CB_CUENTA AS CBC
        INNER JOIN 
            ODS.CB_HIST_SALDO AS CBHS ON CBC.CU_CUENTA = CBHS.HI_CUENTA
        INNER JOIN 
            ODS.CB_CORTE AS CB_CORTE ON CB_CORTE.CO_CORTE = CBHS.HI_CORTE 
                                                  AND CB_CORTE.CO_PERIODO = CBHS.HI_PERIODO 
                                                  AND CB_CORTE.CO_EMPRESA = CBHS.HI_EMPRESA 
                                                  AND CB_CORTE.CO_FECHA_INI = CBHS.HI_FECHA_INI
        WHERE 
            CBHS.HI_OFICINA <> 0
            AND CB_CORTE.CO_FECHA_FIN <= TO_TIMESTAMP('{{ vStgUltimoDiaHabilMesAnt }}', '{{ vStgFormatoFecha }}')
            AND CB_CORTE.CO_FECHA_INI >= TO_TIMESTAMP('{{ vStgUltimoDiaHabilMesAnt }}', '{{ vStgFormatoFecha }}')
            AND SUBSTRING(CBC.CU_CUENTA, 1, 1) IN ('4', '5') 
            AND TO_CHAR(TO_TIMESTAMP('{{ vStgFechaCarga }}', '{{ vStgFormatoFecha }}'), 'MM') NOT IN ('01', '07')
            AND CBHS.HI_FECHA_INI = TO_TIMESTAMP('{{ vStgUltimoDiaHabilMesAnt }}', '{{ vStgFormatoFecha }}')
        GROUP BY 
            CBC.CU_CUENTA,
            STG.FU_EQUIVALENCIA_VARCHAR('ODS.CL_OFICINA', 'OF_OFICINA', COALESCE(CBHS.HI_OFICINA, '0'))
    ) AS TMPHISSAL45 ON TMPHISSAL.HI_OFICINA = TMPHISSAL45.HI_OFICINA AND TMPHISSAL.HI_CUENTA = TMPHISSAL45.HI_CUENTA
    INNER JOIN 
        ODS.CB_CUENTA AS CC ON CC.CU_CUENTA = TMPHISSAL.HI_CUENTA
    WHERE 
        (CC.CU_FECHA_ESTADO IS NULL OR CC.CU_FECHA_ESTADO BETWEEN DATE_TRUNC('month', TO_TIMESTAMP('{{ vStgFechaCarga }}', '{{ vStgFormatoFecha }}')) AND TO_TIMESTAMP('{{ vStgFechaCarga }}', '{{ vStgFormatoFecha }}') OR CC.CU_ESTADO = 'V')
        AND (CC.CU_CUENTA LIKE '1%' OR CC.CU_CUENTA LIKE '2%' OR CC.CU_CUENTA LIKE '3%' OR CC.CU_CUENTA LIKE '6%' OR CC.CU_CUENTA LIKE '7%' OR CC.CU_CUENTA LIKE '8%')
        AND NOT EXISTS (SELECT 1 FROM ODS.CB_CUENTA AS PADRE WHERE PADRE.CU_CUENTA_PADRE = CC.CU_CUENTA AND PADRE.CU_ESTADO <> 'C')
        AND FECHACARGA = '{{ vStgPartition }}'
) AS subquery;
{% endif %}
""")

	query = template.render(
		vStgSmFechaProcesar=vStgSmFechaProcesar,
		vNombreTablaAuxSMExt=vNombreTablaAuxSMExt,
		vStgFormatoFecha=vStgFormatoFecha,
		vStgFechaCarga=vStgFechaCarga,
		vStgUltimoDiaHabilMesAnt=vStgUltimoDiaHabilMesAnt,
		vStgSmFlagDiaFeriado=vStgSmFlagDiaFeriado,
		getCampos=getCampos,
		vStgPartition=vStgPartition,
	)

	# Ensure STG schema is used by default for unqualified table names
	query = 'SET search_path TO STG, public;\n' + query

	if query.strip():
		logger.info(f'Executing query: {query}')
		result = hook.run(query)
		logger.info(f'Query executed successfully. Result: {result}')
	else:
		logger.info('No query to execute. Query is empty.')
