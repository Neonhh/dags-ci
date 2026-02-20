from airflow.decorators import dag, task
from pendulum import datetime
from datetime import timedelta
import logging
from BAN_IKM_SQL_Control_Append_SM import *

DEFAULT_CONTEXT_VARS = {
	'vStgSmFechaProcesar': '10/30/2025',
	'vStgFechaCarga': '08/29/2025',
	'vStgSmFlagDiaFeriado': 'n',
	'vStgFormatoFecha': 'MM/DD/YYYY',
	'vStgUltimoDiaHabilMesAnt': '08/29/2025',
	'vStgSmLotSize': '30000',
	'vStgPartition': '3006',
	'postgres_conn_id': 'ods'
}

@dag(
	start_date=datetime(2024, 1, 1),
	schedule="@daily",
	catchup=False,
	doc_md=__doc__,
	default_args={
		"owner": "Astro", 
		"retries": 0,
		"retry_delay": timedelta(minutes=5),
		"execution_timeout": timedelta(seconds=30),
		"depends_on_past": False,
		"email_on_failure": False,
		"email_on_retry": False
	},
	tags=["SALDOS MEDIOS", "Interface"],
)
def IT_SM_STG_GL_DATA():

	# ! --------------- DEVELOPMENT ONLY -------------------------------------------
	# ! default variables for testing in development environment
	# ! ----------------------------------------------------------------------------
	@task
	def default_vars():
		return DEFAULT_CONTEXT_VARS
	# ! ----------------------------------------------------------------------------

	# 100
	@task
	def _100_DeclaracionDeVariablesSaldosMedios(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _100_DeclaracionDeVariablesSaldosMedios")
			result = declaracionDeVariablesSaldosMedios(**context)
			logger.info("Successfully completed _100_DeclaracionDeVariablesSaldosMedios")
			return result
		except Exception as e:
			logger.error(f"Error in _100_DeclaracionDeVariablesSaldosMedios: {str(e)}")
			raise

	# 110
	@task
	def _110_DropTablaTemporalSaldosDiariosExtraidosDeOds(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _110_DropTablaTemporalSaldosDiariosExtraidosDeOds")
			result = dropTablaTemporalSaldosDiariosExtraidosDeOds(**context)
			logger.info("Successfully completed _110_DropTablaTemporalSaldosDiariosExtraidosDeOds")
			return result
		except Exception as e:
			logger.error(f"Error in _110_DropTablaTemporalSaldosDiariosExtraidosDeOds: {str(e)}")
			raise
	
	# 120
	@task
	def _120_DropTablaTemporalSaldosDiariosDividosEnLotes(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _120_DropTablaTemporalSaldosDiariosDividosEnLotes")
			result = dropTablaTemporalSaldosDiariosDividosEnLotes(**context)
			logger.info("Successfully completed _120_DropTablaTemporalSaldosDiariosDividosEnLotes")
			return result
		except Exception as e:
			logger.error(f"Error in _120_DropTablaTemporalSaldosDiariosDividosEnLotes: {str(e)}")
			raise

	# 130
	@task
	def _130_DropTablaTemporalLotesThreads(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _130_DropTablaTemporalLotesThreads")
			result = dropTablaTemporalLotesThreads(**context)
			logger.info("Successfully completed _130_DropTablaTemporalLotesThreads")
			return result
		except Exception as e:
			logger.error(f"Error in _130_DropTablaTemporalLotesThreads: {str(e)}")
			raise

	# 210
	@task
	def _210_CrearTablaSaldosPuntualesAcumulados(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _210_CrearTablaSaldosPuntualesAcumulados")
			result = crearTablaSaldosPuntualesAcumulados(**context)
			logger.info("Successfully completed _210_CrearTablaSaldosPuntualesAcumulados")
			return result
		except Exception as e:
			logger.error(f"Error in _210_CrearTablaSaldosPuntualesAcumulados: {str(e)}")
			raise

	# 230
	@task
	def _230_CrearIndiceTablaSaldosPuntualesAcumulados(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _230_CrearIndiceTablaSaldosPuntualesAcumulados")
			result = crearIndiceTablaSaldosPuntualesAcumulados(**context)
			logger.info("Successfully completed _230_CrearIndiceTablaSaldosPuntualesAcumulados")
			return result
		except Exception as e:
			logger.error(f"Error in _230_CrearIndiceTablaSaldosPuntualesAcumulados: {str(e)}")
			raise

	# 250
	@task
	def _250_CrearTablaTemporalSaldosPuntualesDia(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _250_CrearTablaTemporalSaldosPuntualesDia")
			result = crearTablaTemporalSaldosPuntualesDia(**context)
			logger.info("Successfully completed _250_CrearTablaTemporalSaldosPuntualesDia")
			return result
		except Exception as e:
			logger.error(f"Error in _250_CrearTablaTemporalSaldosPuntualesDia: {str(e)}")
			raise

	# 260
	@task
	def _260_CrearIndiceTablaTemporalSaldosPuntualesDia(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _260_CrearIndiceTablaTemporalSaldosPuntualesDia")
			result = crearIndiceTablaTemporalSaldosPuntualesDia(**context)
			logger.info("Successfully completed _260_CrearIndiceTablaTemporalSaldosPuntualesDia")
			return result
		except Exception as e:
			logger.error(f"Error in _260_CrearIndiceTablaTemporalSaldosPuntualesDia: {str(e)}")
			raise

	# 270
	@task
	def _270_CrearTablaTemporalLotesThreads(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _270_CrearTablaTemporalLotesThreads")
			result = crearTablaTemporalLotesThreads(**context)
			logger.info("Successfully completed _270_CrearTablaTemporalLotesThreads")
			return result
		except Exception as e:
			logger.error(f"Error in _270_CrearTablaTemporalLotesThreads: {str(e)}")
			raise

	# 280
	@task
	def _280_CrearTablaNombreCamposSaldosPuntuales(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _280_CrearTablaNombreCamposSaldosPuntuales")
			result = crearTablaNombreCamposSaldosPuntuales(**context)
			logger.info("Successfully completed _280_CrearTablaNombreCamposSaldosPuntuales")
			return result
		except Exception as e:
			logger.error(f"Error in _280_CrearTablaNombreCamposSaldosPuntuales: {str(e)}")
			raise

	# 290
	@task
	def _290_CrearIndiceUnicoTablaCamposSaldos(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _290_CrearIndiceUnicoTablaCamposSaldos")
			result = crearIndiceUnicoTablaCamposSaldos(**context)
			logger.info("Successfully completed _290_CrearIndiceUnicoTablaCamposSaldos")
			return result
		except Exception as e:
			logger.error(f"Error in _290_CrearIndiceUnicoTablaCamposSaldos: {str(e)}")
			raise

	# 300
	@task
	def _300_InsertarNombreCamposSaldosPuntuales(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _300_InsertarNombreCamposSaldosPuntuales")
			result = insertarNombreCamposSaldosPuntuales(**context)
			logger.info("Successfully completed _300_InsertarNombreCamposSaldosPuntuales")
			return result
		except Exception as e:
			logger.error(f"Error in _300_InsertarNombreCamposSaldosPuntuales: {str(e)}")
			raise

	# 320
	@task
	def _320_ExtraerSaldosPuntualesContratosDiaFeriado(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _320_ExtraerSaldosPuntualesContratosDiaFeriado")
			result = extraerSaldosPuntualesContratosDiaFeriado(**context)
			logger.info("Successfully completed _320_ExtraerSaldosPuntualesContratosDiaFeriado")
			return result
		except Exception as e:
			logger.error(f"Error in _320_ExtraerSaldosPuntualesContratosDiaFeriado: {str(e)}")
			raise

	# 340
	@task
	def _340_ExtraerSaldosPuntualesDiaHabil(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _340_ExtraerSaldosPuntualesDiaHabil")
			result = extraerSaldosPuntualesDiaHabil(**context)
			logger.info("Successfully completed _340_ExtraerSaldosPuntualesDiaHabil")
			return result
		except Exception as e:
			logger.error(f"Error in _340_ExtraerSaldosPuntualesDiaHabil: {str(e)}")
			raise

	# 350
	@task
	def _350_DividirContratosExtraidosEnLotes(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _350_DividirContratosExtraidosEnLotes")
			result = dividirContratosExtraidosEnLotes(**context)
			logger.info("Successfully completed _350_DividirContratosExtraidosEnLotes")
			return result
		except Exception as e:
			logger.error(f"Error in _350_DividirContratosExtraidosEnLotes: {str(e)}")
			raise

	# 360
	@task
	def _360_InsertarLotes(**context):
		logger = logging.getLogger(__name__)
		try:
			logger.info("Starting _360_InsertarLotes")
			result = insertarLotes(**context)
			logger.info("Successfully completed _360_InsertarLotes")
			return result
		except Exception as e:
			logger.error(f"Error in _360_InsertarLotes: {str(e)}")
			raise

	@task
	def setup_extraction():
		pass
	
	# Define task execution and dependencies
	declare_vars = _100_DeclaracionDeVariablesSaldosMedios()
	drop_extraidos = _110_DropTablaTemporalSaldosDiariosExtraidosDeOds()
	drop_aux = _120_DropTablaTemporalSaldosDiariosDividosEnLotes()
	drop_lotes = _130_DropTablaTemporalLotesThreads()
	crear_tabla = _210_CrearTablaSaldosPuntualesAcumulados()
	crear_indice_tabla = _230_CrearIndiceTablaSaldosPuntualesAcumulados()
	crear_aux = _250_CrearTablaTemporalSaldosPuntualesDia()
	crear_indice_aux = _260_CrearIndiceTablaTemporalSaldosPuntualesDia()
	crear_lotes = _270_CrearTablaTemporalLotesThreads()
	crear_tabla_campos = _280_CrearTablaNombreCamposSaldosPuntuales()
	crear_indice_campos = _290_CrearIndiceUnicoTablaCamposSaldos()
	insertar_campos = _300_InsertarNombreCamposSaldosPuntuales()
	extraer_feriado = _320_ExtraerSaldosPuntualesContratosDiaFeriado()
	extraer_habil = _340_ExtraerSaldosPuntualesDiaHabil()
	dividir_lotes = _350_DividirContratosExtraidosEnLotes()
	insertar_lotes = _360_InsertarLotes()
	setup = setup_extraction()

	# ! --------------- DEVELOPMENT ONLY -------------------------------------------
	defaults = default_vars()
	defaults >> declare_vars
	# ! ----------------------------------------------------------------------------

	# Declare variables, drop tables
	declare_vars >> [drop_extraidos, drop_aux, drop_lotes]

	# Create tables, indexes and insert data
	declare_vars >> [crear_tabla, crear_tabla_campos]
	crear_tabla >> crear_indice_tabla
	crear_tabla_campos >> crear_indice_campos >> insertar_campos
	drop_aux >> crear_aux >> crear_indice_aux
	drop_lotes >> crear_lotes

	# Finish setup before extraction
	[crear_indice_tabla, insertar_campos, crear_indice_aux, crear_lotes] >> setup

	# Extraction and processing
	setup >> [extraer_feriado, extraer_habil] >> dividir_lotes >> insertar_lotes

# Instantiate the DAG
IT_SM_STG_GL_DATA()
