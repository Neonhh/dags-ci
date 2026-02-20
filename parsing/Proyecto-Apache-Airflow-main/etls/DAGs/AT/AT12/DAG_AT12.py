from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
import time
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import tempfile

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

def AT12_LIMP_TRANS_ATM(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	FechaInicio = get_variable('FechaInicio')
	FechaFin = get_variable('FechaFin')

	sql_query_deftxt = f'''DELETE FROM ods.tm_atm_consorcio
	WHERE ac_fecha_tran BETWEEN TO_DATE('{FechaInicio}', 'MM/DD/YY') AND TO_DATE('{FechaFin}', 'MM/DD/YY');'''
	hook.run(sql_query_deftxt)

def IT_TM_ATM_CONSORCIO_COBIS_ODS_MENSUAL_AT12(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	FechaInicio = get_variable('FechaInicio')
	FechaFin = get_variable('FechaFin')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ODS.TM_ATM_CONSORCIO (
	AC_REGISTRO	CHAR(2),
	AC_TARJETA	CHAR(32),
	AC_AUTORIZA	NUMERIC(10),
	AC_MONTO	NUMERIC(15,4),
	AC_MONTO_ME	NUMERIC(15,4),
	AC_MONEDA	NUMERIC(5),
	AC_TRANSACCION	CHAR(1),
	AC_FECHA_TRAN	TIMESTAMP(6),
	AC_FECHA_POS	TIMESTAMP(6),
	AC_REFERENCIA	CHAR(13),
	AC_COMERCIO	CHAR(27),
	AC_COD_COMERCIO	NUMERIC(10),
	AC_CAT_COMERCIO	NUMERIC(5),
	AC_TTRANSACCION	CHAR(2),
	AC_BANCO	NUMERIC(5),
	AC_SOBRECARGO	NUMERIC(15,4),
	AC_HORA	CHAR(8),
	AC_TRACE	NUMERIC(10),
	AC_MENSAJE	CHAR(30),
	AC_EDO_REGISTRO	CHAR(4)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE ODS.TM_ATM_CONSORCIO;'''
	hook.run(sql_query_deftxt)
	
	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ods.tm_atm_consorcio (
	ac_registro,
	ac_tarjeta,
	ac_autoriza,
	ac_monto,
	ac_monto_me,
	ac_moneda,
	ac_transaccion,
	ac_fecha_tran,
	ac_fecha_pos,
	ac_referencia,
	ac_comercio,
	ac_cod_comercio,
	ac_cat_comercio,
	ac_ttransaccion,
	ac_banco,
	ac_sobrecargo,
	ac_hora,
	ac_trace,
	ac_mensaje,
	ac_edo_registro
	)
	SELECT
		tm_atm_consorcio.ac_registro,
		tm_atm_consorcio.ac_tarjeta,
		tm_atm_consorcio.ac_autoriza,
		tm_atm_consorcio.ac_monto,
		tm_atm_consorcio.ac_monto_me,
		tm_atm_consorcio.ac_moneda,
		tm_atm_consorcio.ac_transaccion,
		tm_atm_consorcio.ac_fecha_tran,
		tm_atm_consorcio.ac_fecha_pos,
		tm_atm_consorcio.ac_referencia,
		tm_atm_consorcio.ac_comercio,
		tm_atm_consorcio.ac_cod_comercio,
		tm_atm_consorcio.ac_cat_comercio,
		tm_atm_consorcio.ac_ttransaccion,
		tm_atm_consorcio.ac_banco,
		tm_atm_consorcio.ac_sobrecargo,
		tm_atm_consorcio.ac_hora,
		tm_atm_consorcio.ac_trace,
		tm_atm_consorcio.ac_mensaje,
		tm_atm_consorcio.ac_edo_registro
	FROM source_cob_atm_his.tm_atm_consorcio AS tm_atm_consorcio
	WHERE (tm_atm_consorcio.ac_fecha_tran >= ('{FechaInicio}') AND tm_atm_consorcio.ac_fecha_tran <= ('{FechaFin}'))
		AND tm_atm_consorcio.ac_ttransaccion IN ('CC', 'CI')
		AND tm_atm_consorcio.ac_tarjeta IS NOT NULL;'''
	hook.run(sql_query_deftxt)

def IMPORTAR_INSUMO(**kwargs):
    try:
        # Define la informacion del bucket y el objeto en GCS
        gcs_bucket = 'airflow-dags-data'
        gcs_object = 'data/AT12/INSUMOS/PMPFP301TT.PMPFP301TT'
        
        # Inicializa los hooks
        postgres_hook = PostgresHook(postgres_conn_id='at12')
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

        # 1. Validar conexion a PostgreSQL
        try:
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            logger.info("Conexion a PostgreSQL validada exitosamente")
        except Exception as e:
            logger.error(f"Error al conectar a PostgreSQL: {str(e)}")
            raise

        # 2. Validar acceso a GCS
        try:
            if not gcs_hook.exists(bucket_name=gcs_bucket, object_name=gcs_object):
                raise Exception(f"El archivo {gcs_object} no existe en el bucket {gcs_bucket}")
            logger.info("Validacion de archivo GCS exitosa")
        except Exception as e:
            logger.error(f"Error al validar archivo en GCS: {str(e)}")
            raise

        # 3. Truncar tabla
        logger.info("Truncando la tabla FILE_AT12.AT12_PMPFP301TT...")
        postgres_hook.run("TRUNCATE TABLE FILE_AT12.AT12_PMPFP301TT;")
        logger.info("Tabla FILE_AT12.AT12_PMPFP301TT truncada exitosamente.")

        # 4. Preparar consulta COPY
        sql_copy = """
        COPY FILE_AT12.AT12_PMPFP301TT (
            LINEA
        )
        FROM STDIN
        WITH (FORMAT text, HEADER true, DELIMITER ';', ENCODING 'UTF8');
        """
        
        # 5. Descargar archivo temporal y crea directorio temporal
        temp_dir = tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, 'PMPFP301TT.PMPFP301TT')
        
        try:
            logger.info(f"Descargando archivo '{gcs_object}' desde GCS...")
            gcs_hook.download(
                bucket_name=gcs_bucket,
                object_name=gcs_object,
                filename=local_file_path
            )
            
            # 6. Validar archivo descargado
            file_size = os.path.getsize(local_file_path)
            if file_size == 0:
                raise Exception("El archivo descargado esta vacio")
                
            logger.info(f"Archivo descargado correctamente. Tamaño: {file_size} bytes")
            
            # 7. Mostrar primeras li­neas para debug (ya usa 'windows-1252' segun tu codigo)
            with open(local_file_path, 'r', encoding='utf8') as f:
                lines = [next(f) for _ in range(5)]
                logger.info("Primeras lineas del archivo:\n" + "".join(lines))

            # 8. Ejecutar COPY
            logger.info("Iniciando carga de datos a PostgreSQL...")
            start_time = datetime.now()
            
            postgres_hook.copy_expert(sql=sql_copy, filename=local_file_path)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Carga completada en {duration:.2f} segundos")
            
            # 9. Verificar conteo de registros
            count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_AT12.AT12_PMPFP301TT;")[0]
            logger.info(f"Total de registros cargados: {count}")
            
            if count == 0:
                raise Exception("No se cargaron registros - verificar formato del archivo")
                
        except Exception as e:
            logger.error(f"Error durante la carga de datos: {str(e)}")
            
            # Intentar leer el archivo para debug (ya usa 'windows-1252' segun tu codigo)
            try:
                with open(local_file_path, 'r', encoding='utf8') as f:
                    sample = f.read(1000)
                    logger.info(f"Contenido parcial del archivo:\n{sample}")
            except Exception as read_error:
                logger.error(f"No se pudo leer el archivo para debug: {str(read_error)}")
            
            raise
            
        finally:
            # Limpieza siempre se ejecuta
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                logger.info(f"Archivo temporal eliminado: {local_file_path}")
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
                logger.info(f"Directorio temporal eliminado: {temp_dir}")
                
    except Exception as e:
        logger.error(f"Error general en IMPORTAR_INSUMO: {str(e)}")
        # Registrar el error completo para diagnostico
        import traceback
        logger.error("Traceback completo:\n" + traceback.format_exc())
        raise

def AT12_IMPORTAR_PMPFP301TT(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT12_PMPFP301TT (
	LINEA VARCHAR(500)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_PMPFP301TT;'''
	hook.run(sql_query_deftxt)

	# INSERT EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at12_pmpfp301tt (
	linea
	)
	SELECT 
		linea
	FROM FILE_AT12.AT12_PMPFP301TT;'''
	hook.run(sql_query_deftxt)

def AT12_GENERAR_PMPFP301TT(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT12_PMPFP301TT_ (
	CODIGOAFILIADO NUMERIC(8),
	CUENTABANCO VARCHAR(12),
	COMISION NUMERIC(10,2),
	MES NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_PMPFP301TT_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT12_PMPFP301TT_ (
	CODIGOAFILIADO, 
	CUENTABANCO, 
	COMISION, 
	MES
	)
	SELECT
		CAST(SUBSTRING(AT12_PMPFP301TT.LINEA, 1, 8) AS NUMERIC) AS CODIGOAFILIADO,
		SUBSTRING(AT12_PMPFP301TT.LINEA, 9, 12) AS CUENTABANCO,
		CAST(CONCAT(CONCAT(SUBSTRING(AT12_PMPFP301TT.LINEA, 147, 3), '.'), SUBSTRING(AT12_PMPFP301TT.LINEA, 150, 2)) AS DECIMAL) AS COMISION,
		CAST(substring(to_char(current_date, 'DD/MM/YYYY') FROM 4 FOR 2) AS INTEGER) - 1 AS MES
	FROM AT_STG.AT12_PMPFP301TT;'''
	hook.run(sql_query_deftxt)

def AT12_IMPORTAR_PARROQUIAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT12_PARROQUIAS (
	OFICINA NUMERIC(10),
	PARROQUIA VARCHAR(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_PARROQUIAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at12_parroquias (
	OFICINA, 
	PARROQUIA
	)
	SELECT 
		CAST(oficina AS NUMERIC) AS OFICINA, 
		CODIGOPARROQUIA AS PARROQUIA
	FROM ATSUDEBAN.SIIF_PARROQUIAS;'''
	hook.run(sql_query_deftxt)

def AT12_IMPORTAR_HOMOLOGACION(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT12_HOMOLOGACION_SUDEBAN (
	VALORSUDEBAN NUMERIC(10), 
	DESCRIPCIONSUDEBAN VARCHAR(80), 
	VALORCOBIS NUMERIC(10), 
	DESCRIPCIONCOBIS VARCHAR(80)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCATE
	sql_query_deftxt = '''TRUNCATE TABLE at_stg.at12_homologacion_sudeban;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at12_homologacion_sudeban (
	valorsudeban,
	descripcionsudeban,
	valorcobis,
	descripcioncobis
	)
	SELECT DISTINCT
		CAST(valorsudeban AS NUMERIC) AS valorsudeban,
		descripcionsudeban,
		CAST(valorcobis AS NUMERIC) AS valorcobis,
		descripcioncobis 
	FROM ATSUDEBAN.HOMOLOGACIONES_SUDEBAN
	WHERE (NOMBRETABLA='AT12');'''
	hook.run(sql_query_deftxt)

def AT12_RE_TRAN_MONET(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	FechaInicio = get_variable('FechaInicio')
	FechaFin = get_variable('FechaFin')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at12_re_tran_monet (
	cta_banco	CHAR(24),
	valor	NUMERIC(19,4),
	tarjeta_atm	VARCHAR(32),
	ssn_local	NUMERIC(10),
	hora	TIMESTAMP(6),
	oficina	NUMERIC(5),
	srv_local	VARCHAR(16),
	srv_host	VARCHAR(16),
	monto4	NUMERIC(19,4),
	nro_tarjeta	VARCHAR(50),
	autoriza	NUMERIC(10),
	fecha_tran	DATE,
	parroquia NUMERIC(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_RE_TRAN_MONET;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at12_re_tran_monet (
	CTA_BANCO,
	VALOR,
	TARJETA_ATM,
	SSN_LOCAL,
	HORA,
	OFICINA,
	SRV_LOCAL,
	SRV_HOST,
	MONTO4,
	NRO_TARJETA,
	AUTORIZA,
	FECHA_TRAN,
	PARROQUIA
	)
	SELECT
		RE_TRAN_MONET_HIS.TH_CTA_BANCO,
		RE_TRAN_MONET_HIS.TH_VALOR,
		TRIM(RE_TRAN_MONET_HIS.TH_TARJETA_ATM),
		RE_TRAN_MONET_HIS.TH_SSN_LOCAL,
		RE_TRAN_MONET_HIS.TH_HORA,
		RE_TRAN_MONET_HIS.TH_OFICINA,
		RE_TRAN_MONET_HIS.TH_SRV_LOCAL,
		RE_TRAN_MONET_HIS.TH_SRV_HOST,
		RE_TRAN_MONET_HIS.TH_MONTO4,
		RE_TRAN_MONET_HIS.TH_TARJETA_ATM,
		RE_TRAN_MONET_HIS.TH_SSN_LOCAL,
		RE_TRAN_MONET_HIS.TH_FECHA,
		NULL AS PARROQUIA
	FROM ODS.RE_TRAN_MONET_HIS AS RE_TRAN_MONET_HIS
	WHERE RE_TRAN_MONET_HIS.TH_FECHA BETWEEN TO_DATE('{FechaInicio}', 'MM/DD/YY') AND TO_DATE('{FechaFin}', 'MM/DD/YY')
		AND RE_TRAN_MONET_HIS.TH_ESTADO_EJECUCION = 'EJ'
		AND RE_TRAN_MONET_HIS.TH_TIPO_TRAN IN (500, 503, 504, 510, 513, 520, 523, 130, 140, 150, 230, 231, 240, 241, 250, 251);'''
	hook.run(sql_query_deftxt)

def AT12_ATM_CONSORCIO(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	FechaInicio = get_variable('FechaInicio')
	FechaFin = get_variable('FechaFin')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at12_atm_consorcio (
	monto DECIMAL(18,2),
	tarjeta VARCHAR(32),
	fecha_tran DATE,
	cod_comercio NUMERIC(10),
	cat_comercio NUMERIC(5),
	ttransaccion VARCHAR(2),
	banco NUMERIC(5),
	autoriza NUMERIC(10)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAMOS
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_ATM_CONSORCIO;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = f'''INSERT INTO at_stg.at12_atm_consorcio (
	MONTO,
	TARJETA,
	FECHA_TRAN,
	COD_COMERCIO,
	CAT_COMERCIO,
	TTRANSACCION,
	BANCO,
	AUTORIZA
	)
	SELECT DISTINCT
		TM_ATM_CONSORCIO.AC_MONTO,
		TRIM(TM_ATM_CONSORCIO.AC_TARJETA),
		TM_ATM_CONSORCIO.AC_FECHA_TRAN,
		TM_ATM_CONSORCIO.AC_COD_COMERCIO,
		TM_ATM_CONSORCIO.AC_CAT_COMERCIO,
		TM_ATM_CONSORCIO.AC_TTRANSACCION,
		TM_ATM_CONSORCIO.AC_BANCO,
		TM_ATM_CONSORCIO.AC_AUTORIZA
	FROM ODS.TM_ATM_CONSORCIO AS TM_ATM_CONSORCIO
	WHERE TM_ATM_CONSORCIO.AC_FECHA_TRAN BETWEEN TO_DATE('{FechaInicio}', 'MM/DD/YY') AND TO_DATE('{FechaFin}', 'MM/DD/YY')
		AND TM_ATM_CONSORCIO.AC_TTRANSACCION IN ('CC', 'CI')
		AND TM_ATM_CONSORCIO.AC_TARJETA IS NOT NULL;'''
	hook.run(sql_query_deftxt)

def AT12_GENERACION_POS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# creamos tabla destino
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at12_generacion_pos (
	paisconsumo VARCHAR(2),
	municipio VARCHAR(6),
	extranjera NUMERIC(2),
	clasetarjeta NUMERIC(2),
	tipotarjeta NUMERIC(2),
	concepto NUMERIC(20),
	franquicia NUMERIC(2),
	cantidadconsumos NUMERIC(10),
	montoconsumo NUMERIC(20, 2),
	comisionespropias NUMERIC(20, 2),
	comisionestercero NUMERIC(20, 2),
	red NUMERIC(2),
	codigoafiliado NUMERIC(20),
	oficina NUMERIC(5),
	nro_tarjeta VARCHAR(50),
	autoriza NUMERIC(10),
	fecha_tran DATE
	);'''
	hook.run(sql_query_deftxt)

	# truncar
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_GENERACION_POS;'''
	hook.run(sql_query_deftxt)

	# insertar en destino
	sql_query_deftxt = '''INSERT INTO at_stg.at12_generacion_pos (
	municipio,
	concepto,
	montoconsumo,
	comisionespropias,
	codigoafiliado,
	oficina,
	nro_tarjeta,
	autoriza,
	fecha_tran,
	paisconsumo,
	extranjera,
	clasetarjeta,
	tipotarjeta,
	franquicia,
	cantidadconsumos,
	comisionestercero,
	red
	)
	SELECT
		CASE 
			WHEN at12_atm_consorcio.ttransaccion = 'CI' THEN '0101' 
			ELSE SUBSTRING(COALESCE(at12_parroquias.parroquia::TEXT, '0101'), 1, 4) 
		END AS municipio,
		CASE 
			WHEN at12_homologacion_sudeban.valorsudeban IS NOT NULL THEN at12_homologacion_sudeban.valorsudeban 
			ELSE at12_atm_consorcio.cat_comercio 
		END AS concepto,
		at12_atm_consorcio.monto AS montoconsumo,
		CASE 
			WHEN at12_pmpfp301tt_.comision > 0 THEN at12_re_tran_monet.valor * at12_pmpfp301tt_.comision / 100 
			ELSE 0 
		END AS comisionespropias,
		at12_atm_consorcio.cod_comercio AS codigoafiliado,
		at12_re_tran_monet.oficina AS oficina,
		at12_atm_consorcio.tarjeta AS nro_tarjeta,
		at12_atm_consorcio.autoriza AS autoriza,
		at12_atm_consorcio.fecha_tran AS fecha_tran,
		'VE',
		1,
		2,
		0,
		0,
		1,
		0,
		2
	FROM at_stg.at12_atm_consorcio  AS at12_atm_consorcio LEFT JOIN at_stg.at12_pmpfp301tt_ AS at12_pmpfp301tt_
		ON at12_atm_consorcio.cod_comercio = at12_pmpfp301tt_.codigoafiliado
	LEFT JOIN at_stg.at12_parroquias AS at12_parroquias
		ON CAST(SUBSTRING(at12_pmpfp301tt_.cuentabanco::TEXT FROM 3 FOR 3) AS INTEGER) = at12_parroquias.oficina
	INNER JOIN at_stg.at12_homologacion_sudeban AS at12_homologacion_sudeban
		ON at12_atm_consorcio.cat_comercio = at12_homologacion_sudeban.valorcobis
	INNER JOIN at_stg.at12_re_tran_monet AS at12_re_tran_monet
		ON at12_atm_consorcio.tarjeta = at12_re_tran_monet.tarjeta_atm
	WHERE SUBSTRING(at12_re_tran_monet.cta_banco FROM 1 FOR 4) = '0114'
		AND RIGHT(at12_re_tran_monet.ssn_local::TEXT, LENGTH(at12_atm_consorcio.autoriza::TEXT)) = at12_atm_consorcio.autoriza::TEXT
		AND TO_CHAR(at12_atm_consorcio.fecha_tran, 'YYYYMMDD') = TO_CHAR(at12_re_tran_monet.hora, 'YYYYMMDD')
		AND at12_re_tran_monet.srv_local = 'MAESTRO';'''
	hook.run(sql_query_deftxt)

def AT12_BANCARIBE_PRE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at12_bancaribe_pre_ (
	paisconsumo VARCHAR(2),
	municipio VARCHAR(6),
	extranjera NUMERIC(2),
	clasetarjeta NUMERIC(2),
	tipotarjeta NUMERIC(2),
	concepto NUMERIC(6),
	franquicia NUMERIC(2),
	cantidadconsumos NUMERIC(10),
	montoconsumo NUMERIC(20, 2),
	comisionespropias NUMERIC(20, 2),
	comisionesterceros NUMERIC(20, 2),
	red NUMERIC(2),
	codigoafiliado NUMERIC(20),
	oficina NUMERIC(5),
	nro_tarjeta VARCHAR(50),
	autoriza NUMERIC(10),
	fecha_tran DATE
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_BANCARIBE_PRE_;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO AT_STG.AT12_BANCARIBE_PRE_ (
	MUNICIPIO, 
	MONTOCONSUMO, 
	COMISIONESPROPIAS, 
	OFICINA, 
	NRO_TARJETA, 
	AUTORIZA, 
	FECHA_TRAN, 
	PAISCONSUMO, 
	EXTRANJERA, 
	CLASETARJETA, 
	TIPOTARJETA, 
	CONCEPTO, 
	FRANQUICIA, 
	CANTIDADCONSUMOS, 
	COMISIONESTERCEROS, 
	RED, 
	CODIGOAFILIADO
	) 
	SELECT 
		'', 
		AT12_RE_TRAN_MONET.VALOR, 
		AT12_RE_TRAN_MONET.MONTO4, 
		AT12_RE_TRAN_MONET.OFICINA, 
		AT12_RE_TRAN_MONET.NRO_TARJETA, 
		AT12_RE_TRAN_MONET.AUTORIZA, 
		AT12_RE_TRAN_MONET.FECHA_TRAN, 
		'VE', 
		1, 
		2, 
		0, 
		18, 
		0, 
		1, 
		0, 
		2, 
		0 
	FROM AT_STG.AT12_RE_TRAN_MONET 
	WHERE (SUBSTRING(AT12_RE_TRAN_MONET.CTA_BANCO FROM 1 FOR 4) = '0114' AND AT12_RE_TRAN_MONET.SRV_HOST = 'CARIBESRV');'''
	hook.run(sql_query_deftxt)

def AT12_BANCARIBE_PREP(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at12_bancaribe_pre (
	paisconsumo VARCHAR(2),
	municipio VARCHAR(6),
	extranjera NUMERIC(2),
	clasetarjeta NUMERIC(2),
	tipotarjeta NUMERIC(2),
	concepto NUMERIC(6),
	franquicia NUMERIC(2),
	cantidadconsumos NUMERIC(10),
	montoconsumo NUMERIC(20, 2),
	comisionespropias NUMERIC(20, 2),
	comisionesterceros NUMERIC(20, 2),
	red NUMERIC(2),
	codigoafiliado NUMERIC(20),
	oficina NUMERIC(5),
	nro_tarjeta VARCHAR(50),
	autoriza NUMERIC(10),
	fecha_tran DATE
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_BANCARIBE_PRE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at12_bancaribe_pre (
	paisconsumo,
	municipio,
	extranjera,
	clasetarjeta,
	tipotarjeta,
	concepto,
	franquicia,
	cantidadconsumos,
	montoconsumo,
	comisionespropias,
	comisionesterceros,
	red,
	codigoafiliado,
	oficina,
	nro_tarjeta,
	autoriza,
	fecha_tran
	)
	SELECT
		at12_bancaribe_pre_.paisconsumo AS paisconsumo,
		CASE 
			WHEN at12_parroquias.parroquia IS NOT NULL THEN SUBSTRING(CAST(at12_parroquias.parroquia AS TEXT) FROM 1 FOR 4) 
			ELSE NULL 
		END AS municipio,
		at12_bancaribe_pre_.extranjera AS extranjera,
		at12_bancaribe_pre_.clasetarjeta AS clasetarjeta,
		at12_bancaribe_pre_.tipotarjeta AS tipotarjeta,
		at12_bancaribe_pre_.concepto AS concepto,
		at12_bancaribe_pre_.franquicia AS franquicia,
		at12_bancaribe_pre_.cantidadconsumos AS cantidadconsumos,
		at12_bancaribe_pre_.montoconsumo AS montoconsumo,
		at12_bancaribe_pre_.comisionespropias AS comisionespropias,
		at12_bancaribe_pre_.comisionesterceros AS comisionesterceros,
		at12_bancaribe_pre_.red AS red,
		at12_bancaribe_pre_.codigoafiliado AS codigoafiliado,
		at12_bancaribe_pre_.oficina AS oficina,
		at12_bancaribe_pre_.nro_tarjeta AS nro_tarjeta,
		at12_bancaribe_pre_.autoriza AS autoriza,
		at12_bancaribe_pre_.fecha_tran AS fecha_tran
	FROM at_stg.at12_bancaribe_pre_ AS AT12_BANCARIBE_PRE_ LEFT OUTER JOIN at_stg.at12_parroquias AS AT12_PARROQUIAS
		ON AT12_BANCARIBE_PRE_.oficina = AT12_PARROQUIAS.oficina;'''
	hook.run(sql_query_deftxt)

def AT12_ATOMO_BANCARIBE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAR TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS at_stg.at12_atomo_bancaribe (
	paisconsumo VARCHAR(2),
	municipio VARCHAR(6),
	extranjera NUMERIC(2),
	clasetarjeta NUMERIC(2),
	tipotarjeta NUMERIC(2),
	concepto NUMERIC(6),
	franquicia NUMERIC(2),
	cantidadconsumos NUMERIC(10),
	montoconsumo NUMERIC(20, 2),
	comisionespropias NUMERIC(20, 2),
	comisionesterceros NUMERIC(20, 2),
	red NUMERIC(2),
	oficina NUMERIC(5),
	nro_tarjeta VARCHAR(50),
	autoriza NUMERIC(10),
	fecha_tran DATE,
	codigoafiliado NUMERIC(20)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT12_ATOMO_BANCARIBE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO at_stg.at12_atomo_bancaribe (
	paisconsumo,
	municipio,
	extranjera,
	clasetarjeta,
	tipotarjeta,
	concepto,
	franquicia,
	cantidadconsumos,
	montoconsumo,
	comisionespropias,
	comisionesterceros,
	red,
	nro_tarjeta,
	autoriza,
	fecha_tran,
	codigoafiliado
	)
	SELECT
		at12_bancaribe_pre.paisconsumo AS paisconsumo,
		CASE 
			WHEN at12_bancaribe_pre.municipio IS NULL THEN '0101' 
			ELSE at12_bancaribe_pre.municipio::TEXT 
		END AS municipio,
		at12_bancaribe_pre.extranjera AS extranjera,
		at12_bancaribe_pre.clasetarjeta AS clasetarjeta,
		at12_bancaribe_pre.tipotarjeta AS tipotarjeta,
		at12_bancaribe_pre.concepto AS concepto,
		at12_bancaribe_pre.franquicia AS franquicia,
		at12_bancaribe_pre.cantidadconsumos AS cantidadconsumos,
		at12_bancaribe_pre.montoconsumo AS montoconsumo,
		at12_bancaribe_pre.comisionespropias AS comisionespropias,
		at12_bancaribe_pre.comisionesterceros AS comisionesterceros,
		at12_bancaribe_pre.red AS red,
		at12_bancaribe_pre.nro_tarjeta AS nro_tarjeta,
		at12_bancaribe_pre.autoriza AS autoriza,
		at12_bancaribe_pre.fecha_tran AS fecha_tran,
		at12_bancaribe_pre.codigoafiliado AS codigo_afiliado
	FROM at_stg.at12_bancaribe_pre AS at12_bancaribe_pre

	UNION ALL

	SELECT
		at12_generacion_pos.paisconsumo AS paisconsumo,
		at12_generacion_pos.municipio::TEXT AS municipio,
		at12_generacion_pos.extranjera AS extranjera,
		at12_generacion_pos.clasetarjeta AS clasetarjeta,
		at12_generacion_pos.tipotarjeta AS tipotarjeta,
		at12_generacion_pos.concepto AS concepto,
		at12_generacion_pos.franquicia AS franquicia,
		at12_generacion_pos.cantidadconsumos AS cantidadconsumos,
		at12_generacion_pos.montoconsumo AS montoconsumo,
		at12_generacion_pos.comisionespropias AS comisionespropias,
		at12_generacion_pos.comisionestercero AS comisionesterceros,
		at12_generacion_pos.red AS red,
		at12_generacion_pos.nro_tarjeta AS nro_tarjeta,
		at12_generacion_pos.autoriza AS autoriza,
		at12_generacion_pos.fecha_tran AS fecha_tran,
		at12_generacion_pos.codigoafiliado AS codigo_afiliado
	FROM at_stg.at12_generacion_pos AS at12_generacion_pos;'''
	hook.run(sql_query_deftxt)

def AT12_SUDEBAN_BANCARIBE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT12_PRE (
	PAISCONSUMO VARCHAR(2),
	MUNICIPIO VARCHAR(6),
	EXTRANJERA NUMERIC(2),
	CLASETARJETA NUMERIC(2),
	TIPOTARJETA NUMERIC(2),
	CONCEPTO NUMERIC(4),
	FRANQUICIA NUMERIC(2),
	CANTIDADCONSUMOS NUMERIC(10),
	MONTOCONSUMO NUMERIC(32,2),
	COMISIONESPROPIAS NUMERIC(32,2),
	COMISIONESTERCEROS NUMERIC(32,2),
	RED NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT12_PRE;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT12_PRE (
	PAISCONSUMO,
	MUNICIPIO,
	EXTRANJERA,
	CLASETARJETA,
	TIPOTARJETA,
	CONCEPTO,
	FRANQUICIA,
	CANTIDADCONSUMOS,
	MONTOCONSUMO,
	COMISIONESPROPIAS,
	COMISIONESTERCEROS,
	RED
	)
	SELECT
		AT12_ATOMO_BANCARIBE.PAISCONSUMO,
		AT12_ATOMO_BANCARIBE.MUNICIPIO,
		AT12_ATOMO_BANCARIBE.EXTRANJERA,
		AT12_ATOMO_BANCARIBE.CLASETARJETA,
		AT12_ATOMO_BANCARIBE.TIPOTARJETA,
		AT12_ATOMO_BANCARIBE.CONCEPTO,
		AT12_ATOMO_BANCARIBE.FRANQUICIA,
		SUM(AT12_ATOMO_BANCARIBE.CANTIDADCONSUMOS) AS CANTIDADCONSUMOS,
		SUM(AT12_ATOMO_BANCARIBE.MONTOCONSUMO) AS MONTOCONSUMO,
		SUM(AT12_ATOMO_BANCARIBE.COMISIONESPROPIAS) AS COMISIONESPROPIAS,
		SUM(AT12_ATOMO_BANCARIBE.COMISIONESTERCEROS) AS COMISIONESTERCEROS,
		AT12_ATOMO_BANCARIBE.RED
	FROM AT_STG.AT12_ATOMO_BANCARIBE AS AT12_ATOMO_BANCARIBE
	GROUP BY 
		AT12_ATOMO_BANCARIBE.PAISCONSUMO,
		AT12_ATOMO_BANCARIBE.MUNICIPIO,
		AT12_ATOMO_BANCARIBE.EXTRANJERA,
		AT12_ATOMO_BANCARIBE.CLASETARJETA,
		AT12_ATOMO_BANCARIBE.TIPOTARJETA,
		AT12_ATOMO_BANCARIBE.CONCEPTO,
		AT12_ATOMO_BANCARIBE.FRANQUICIA,
		AT12_ATOMO_BANCARIBE.RED;'''
	hook.run(sql_query_deftxt)

def ATS_TH_AT12_BANCARIBE(**kwargs):
	hook = PostgresHook(postgres_conn_id='at12')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT12 (
	PAISCONSUMO VARCHAR(2),
	MUNICIPIO VARCHAR(6),
	EXTRANJERA NUMERIC(2),
	CLASETARJETA NUMERIC(2),
	TIPOTARJETA NUMERIC(2),
	CONCEPTO NUMERIC(4),
	FRANQUICIA NUMERIC(2),
	CANTIDADCONSUMOS NUMERIC(10),
	MONTOCONSUMO NUMERIC(32,2),
	COMISIONESPROPIAS NUMERIC(32,2),
	COMISIONESTERCEROS NUMERIC(32,2),
	RED NUMERIC(2)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT12;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.ATS_TH_AT12 (
	PAISCONSUMO, 
	MUNICIPIO, 
	EXTRANJERA, 
	CLASETARJETA, 
	TIPOTARJETA, 
	CONCEPTO, 
	FRANQUICIA, 
	CANTIDADCONSUMOS, 
	MONTOCONSUMO, 
	COMISIONESPROPIAS, 
	COMISIONESTERCEROS, 
	RED
	) 
	SELECT 
		PAISCONSUMO, 
		MUNICIPIO, 
		EXTRANJERA, 
		CLASETARJETA, 
		TIPOTARJETA, 
		CONCEPTO, 
		FRANQUICIA, 
		CANTIDADCONSUMOS, 
		MONTOCONSUMO, 
		COMISIONESPROPIAS, 
		COMISIONESTERCEROS, 
		RED 
	FROM ATSUDEBAN.ATS_TH_AT12_PRE;'''
	hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT12',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

AT12_LIMP_TRANS_ATM_task = PythonOperator(
	task_id='AT12_LIMP_TRANS_ATM_task',
	python_callable=AT12_LIMP_TRANS_ATM,
	dag=dag
)

IT_TM_ATM_CONSORCIO_COBIS_ODS_MENSUAL_AT12_task = PythonOperator(
	task_id='IT_TM_ATM_CONSORCIO_COBIS_ODS_MENSUAL_AT12_task',
	python_callable=IT_TM_ATM_CONSORCIO_COBIS_ODS_MENSUAL_AT12,
	dag=dag
)

IMPORTAR_INSUMO_task = PythonOperator(
    task_id='IMPORTAR_INSUMO_task',
    python_callable=IMPORTAR_INSUMO,
    dag=dag
)

AT12_IMPORTAR_PMPFP301TT_task = PythonOperator(
	task_id='AT12_IMPORTAR_PMPFP301TT_task',
	python_callable=AT12_IMPORTAR_PMPFP301TT,
	dag=dag
)

AT12_GENERAR_PMPFP301TT_task = PythonOperator(
	task_id='AT12_GENERAR_PMPFP301TT_task',
	python_callable=AT12_GENERAR_PMPFP301TT,
	dag=dag
)

AT12_IMPORTAR_PARROQUIAS_task = PythonOperator(
	task_id='AT12_IMPORTAR_PARROQUIAS_task',
	python_callable=AT12_IMPORTAR_PARROQUIAS,
	dag=dag
)

AT12_IMPORTAR_HOMOLOGACION_task = PythonOperator(
	task_id='AT12_IMPORTAR_HOMOLOGACION_task',
	python_callable=AT12_IMPORTAR_HOMOLOGACION,
	dag=dag
)

AT12_RE_TRAN_MONET_task = PythonOperator(
	task_id='AT12_RE_TRAN_MONET_task',
	python_callable=AT12_RE_TRAN_MONET,
	dag=dag
)

AT12_ATM_CONSORCIO_task = PythonOperator(
	task_id='AT12_ATM_CONSORCIO_task',
	python_callable=AT12_ATM_CONSORCIO,
	dag=dag
)

AT12_GENERACION_POS_task = PythonOperator(
	task_id='AT12_GENERACION_POS_task',
	python_callable=AT12_GENERACION_POS,
	dag=dag
)

AT12_BANCARIBE_PRE_task = PythonOperator(
	task_id='AT12_BANCARIBE_PRE_task',
	python_callable=AT12_BANCARIBE_PRE,
	dag=dag
)

AT12_BANCARIBE_PREP_task = PythonOperator(
	task_id='AT12_BANCARIBE_PREP_task',
	python_callable=AT12_BANCARIBE_PREP,
	dag=dag
)

AT12_ATOMO_BANCARIBE_task = PythonOperator(
	task_id='AT12_ATOMO_BANCARIBE_task',
	python_callable=AT12_ATOMO_BANCARIBE,
	dag=dag
)

AT12_SUDEBAN_BANCARIBE_task = PythonOperator(
	task_id='AT12_SUDEBAN_BANCARIBE_task',
	python_callable=AT12_SUDEBAN_BANCARIBE,
	dag=dag
)

ATS_TH_AT12_BANCARIBE_task = PythonOperator(
	task_id='ATS_TH_AT12_BANCARIBE_task',
	python_callable=ATS_TH_AT12_BANCARIBE,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT12_LIMP_TRANS_ATM_task >> IT_TM_ATM_CONSORCIO_COBIS_ODS_MENSUAL_AT12_task >> IMPORTAR_INSUMO_task >> AT12_IMPORTAR_PMPFP301TT_task >> AT12_GENERAR_PMPFP301TT_task >> AT12_IMPORTAR_PARROQUIAS_task >> AT12_IMPORTAR_HOMOLOGACION_task >> AT12_RE_TRAN_MONET_task >> AT12_ATM_CONSORCIO_task >> AT12_GENERACION_POS_task >> AT12_BANCARIBE_PRE_task >> AT12_BANCARIBE_PREP_task >> AT12_ATOMO_BANCARIBE_task >> AT12_SUDEBAN_BANCARIBE_task >> ATS_TH_AT12_BANCARIBE_task
