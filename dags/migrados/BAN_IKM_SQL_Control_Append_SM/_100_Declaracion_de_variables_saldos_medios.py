# se conoce con antelacion que utiliza la clase Campo
# la parte que corresponde con la declaración de la clase se extrajo
# ahora esta función consiste solamente en la declaracion de variables
# y luego agregar estas variables al contexto de ejecución
from BAN_IKM_SQL_Control_Append_SM.Campo import Campo
import logging

# * CODIGO IGNORADO:
# - definicion de campo
# - definicion de funcion getCampos
# esto es porque estas funciones no dependen del contexto donde se ejecutan

# * CODIGO TRADUCIDO
# definicion de consultarCampos
# declaraciones de variables globales
def consultarCampos(vListaCampos):
	vCampoAux = Campo()
	vCampoAux.vNumero = 1
	vCampoAux.vTipoCampo = "CLAVE_PRODUCTO"
	vCampoAux.setNombreCampo("V_GL_CODE", 1)
	vCampoAux.vDescCampo = "VARCHAR(20)"
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 2
	vCampoAux.vTipoCampo = "CLAVE_PRODUCTO"
	vCampoAux.setNombreCampo("V_ORG_UNIT_CODE", 2)
	vCampoAux.vDescCampo = "VARCHAR(40)"
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 3
	vCampoAux.vTipoCampo = "CLAVE_PRODUCTO"
	vCampoAux.setNombreCampo("", 3)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 4
	vCampoAux.vTipoCampo = "CLAVE_PRODUCTO"
	vCampoAux.setNombreCampo("", 4)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 5
	vCampoAux.vTipoCampo = "CLAVE_PRODUCTO"
	vCampoAux.setNombreCampo("", 5)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 1
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("N_AMOUNT_LCY", 1)
	vCampoAux.vDescCampo = "NUMERIC(22,3)"
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 2
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("N_AMOUNT_ACY", 2)
	vCampoAux.vDescCampo = "NUMERIC(22,3)"
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 3
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 3)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 4
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 4)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 5
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 5)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 6
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 6)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 7
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 7)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 8
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 8)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 9
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 9)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 10
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 10)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 11
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 11)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 12
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 12)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 13
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 13)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 14
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 14)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 15
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 15)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 16
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 16)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 17
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 17)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 18
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 18)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 19
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 19)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 20
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 20)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)
	vCampoAux = Campo()
	vCampoAux.vNumero = 21
	vCampoAux.vTipoCampo = "SALDO_PUNTUAL"
	vCampoAux.vFlagCambioInteres = ""
	vCampoAux.setNombreCampo("", 21)
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)

	# Otros Campos
	# Fecha Utimo Reprecio
	vFechaUltRe = ""
	vCampoAux = Campo()
	vCampoAux.vNumero = 1
	vCampoAux.vTipoCampo = "FECHA_REPRECIO"
	vCampoAux.vNombreCampo = vFechaUltRe
	vCampoAux.vDescCampo = "DATE"
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)

	# Fecha Proximo Reprecio
	vFechaUltRe = ""
	vCampoAux = Campo()
	vCampoAux.vNumero = 1
	vCampoAux.vTipoCampo = "FECHA_PROX_REPRE"
	vCampoAux.vNombreCampo = vFechaUltRe
	vCampoAux.vDescCampo = "DATE"
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)

	# Tipo de interes
	vFechaUltRe = ""
	vCampoAux = Campo()
	vCampoAux.vNumero = 1
	vCampoAux.vTipoCampo = "TIPO_INTERES"
	vCampoAux.vNombreCampo = vFechaUltRe
	vCampoAux.vDescCampo = ""
	if vCampoAux.vNombreCampo: vListaCampos.append(vCampoAux)

def declaracionDeVariablesSaldosMedios(**context):
	# ******************************************************************************
	# * INSERT DEFAULT VALUES for variables that might not be set
	# ******************************************************************************
	# Set default values for other required variables if not already set
	default_vars = context['ti'].xcom_pull(task_ids='default_vars')

	# Push default values to XCom if they don't exist
	for key, default_value in default_vars.items():
		existing_value = context['ti'].xcom_pull(key=key)
		if existing_value is None:
			context['ti'].xcom_push(key=key, value=default_value)
			# Debug: Log what we're pushing
			logger = logging.getLogger(__name__)
			logger.info(f"Pushed {key}: {default_value}")
		else:
			# Debug: Log what already exists
			logger = logging.getLogger(__name__)
			logger.info(f"Key {key} already exists with value: {existing_value}")
	# ******************************************************************************

	# Nombre de la tabla STG, se limpia en caso de que tenga el nombre del esquema o del dblink
	# * CODIGO ORIGINAL: vNombreTablaSTG = '<?=snpRef.getObjectName("L", "STG_GL_DATA", "LS_STG", "", "D") ?>';
	vNombreTablaSTG = 'STG_GL_DATA'
	context['ti'].xcom_push(key='vNombreTablaSTG', value=vNombreTablaSTG)
	if '.' in vNombreTablaSTG:
		vNombreTablaSTG = vNombreTablaSTG.split('.', 1)[1]
	if '@' in vNombreTablaSTG:
		vNombreTablaSTG = vNombreTablaSTG.split('@', 1)[0]

	# Nombre de la tabla de saldos puntuales acumulados, se crea una por cada producto
	vNombreTablaSM = "SM_" + vNombreTablaSTG[:27]

	# Fecha para el cual se va crear la tabla temporal de saldos puntuales del día, se toma los dias para diferenciar la distintas tablas de saldos puntuales diarios
	# * CODIGO ORIGINAL: vFechaEjecutar = "#SIG.vStgSmFechaProcesar";
	# Use execution date as processing date, fallback to xcom_pull if available
	vFechaEjecutar = context['ti'].xcom_pull(key='vStgSmFechaProcesar')

	# Nombre de la tabla temporal de saldos puntuales del dia extraidos de ods
	vNombreTablaAuxSMExt = "SM" + vFechaEjecutar[3:5] + "E$" + vNombreTablaSTG[:24]

	# Nombre de la tabla temporal de saldos puntuales del dia divido en lotes
	vNombreTablaAuxSM = "SM" + vFechaEjecutar[3:5] + "$" + vNombreTablaSTG[:23]

	# Nombre de la tabla temporal que almacena el número de lotes generado
	vNombreTablaLotesSM = "SM" + vFechaEjecutar[3:5] + "L$" + vNombreTablaSTG[:24]

	# Nombre del indice de tabla saldos puntuales acumulados
	vNombreIndiceSM = "IDX01_" + vNombreTablaSM[:24]

	# Nombre del indice de tabla temporal saldos puntuales dia
	vNombreIndiceAuxSM = "IDX01_" + vNombreTablaAuxSM[:24]

	vListaCampos = []  # Lista que contiene los campos de saldos puntuales, clave producto y otros campos que no sean saldos medios
	consultarCampos(vListaCampos)

	# ? podria hacerse el guardado de una en una asi guardando SOLO las variables que se declararon
	# //context['ti'].xcom_push(key='vNombreTablaSTG', value=vNombreTablaSTG)
	# //context['ti'].xcom_push(key='vNombreTablaSM', value=vNombreTablaSM)
	# //context['ti'].xcom_push(key='vFechaEjecutar', value=vFechaEjecutar)
	# //context['ti'].xcom_push(key='vNombreTablaAuxSMExt', value=vNombreTablaAuxSMExt)
	# //context['ti'].xcom_push(key='vNombreTablaAuxSM', value=vNombreTablaAuxSM)
	# //context['ti'].xcom_push(key='vNombreTablaLotesSM', value=vNombreTablaLotesSM)
	# //context['ti'].xcom_push(key='vNombreIndiceSM', value=vNombreIndiceSM)
	# //context['ti'].xcom_push(key='vNombreIndiceAuxSM', value=vNombreIndiceAuxSM)
	# //context['ti'].xcom_push(key='vListaCampos', value=vListaCampos.__dict__)

	# ? podrian guardarse las variables del scope local
	# NOTE: habria que tomar en cuenta que podrian terminar guardandose mas
	# variables de las que se espera, se podrian agregar condiciones
	for k, v in locals().items():
		if not k.startswith('v') or len(k) < 2:
			continue

		# *DEBUG: Log what we're pushing to XCom
		logger = logging.getLogger(__name__)
		logger.info(f"Pushing to XCom - {k}: {v}")

		# Si es una lista de objetos Campo
		if isinstance(v, list) and v and hasattr(v[0], '__dict__'):
			context['ti'].xcom_push(key=k, value=[obj.__dict__ for obj in v])
		# Si es un objeto con __dict__
		elif hasattr(v, '__dict__'):
			context['ti'].xcom_push(key=k, value=v.__dict__)
		else:
			context['ti'].xcom_push(key=k, value=v)
