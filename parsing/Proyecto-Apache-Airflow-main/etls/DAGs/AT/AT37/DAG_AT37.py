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
import time
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import tempfile

logger = logging.getLogger(__name__)

### FUNCIONES DE CADA TAREA ###

def AT37_ALL(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

    # CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_ATSS (
	TIPOTRANSFERENCIA NUMERIC,
	CLASIFICACION NUMERIC,
	TIPOCLIENTE VARCHAR(3),
	IDENTIFICACIONCLIENTE VARCHAR(15),
	NOMBRECLIENTE VARCHAR(250),
	TIPOINSTRUMENTO VARCHAR(5),
	CUENTACLIENTE CHAR(20),
	MONEDA CHAR(3),
	MONTO NUMERIC,
	FECHA CHAR(8),
	REFERENCIA VARCHAR(30),
	EMPRESAFORMACION VARCHAR(3),
	MOTIVO VARCHAR(100),
	DIRECCIONIP VARCHAR(50),
	TIPOCONTRAPARTE VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE VARCHAR(30),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(5),
	CUENTACONTRAPARTE VARCHAR(20),
	OFICINA NUMERIC
	);'''
    hook.run(sql_query_deftxt)

	# TRUNCATE
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_ATSS;'''
    hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO atsudeban.at37_atss (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	moneda,
	monto,
	fecha,
	referencia,
	empresaformacion,
	motivo,
	direccionip,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	oficina
	)
	SELECT DISTINCT
		at37_atms.tipotransferencia AS tipotransferencia,
		at37_atms.clasificacion AS clasificacion,
		TRIM(at37_atms.tipocliente) AS tipocliente,
		TRIM(at37_atms.identificacioncliente) AS identificacioncliente,
		at37_atms.nombrecliente AS nombrecliente,
		at37_atms.tipoinstrumento AS tipoinstrumento,
		at37_atms.cuentacliente AS cuentacliente,
		at37_atms.moneda AS moneda,
		at37_atms.monto AS monto,
		at37_atms.fecha AS fecha,
		at37_atms.referencia AS referencia,
		CASE
			WHEN TRIM(at37_atms.identificacioncliente) IN ('V','E','P') AND at37_atms.empresaformacion IN ('0','1','2') THEN at37_atms.empresaformacion
			WHEN TRIM(at37_atms.identificacioncliente) NOT IN ('V','E','P') THEN '0'
		END AS empresaformacion,
		COALESCE(at37_atms.motivo, '0') AS motivo,
		at37_atms.direccionip AS direccionip,
		TRIM(at37_atms.tipocontraparte) AS tipocontraparte,
		at37_atms.identificacioncontraparte AS identificacioncontraparte,
		at37_atms.nombrecontraparte AS nombrecontraparte,
		at37_atms.tipoinstrumentocontraparte AS tipoinstrumentocontraparte,
		at37_atms.cuentacontraparte AS cuentacontraparte,
		CASE
			WHEN at37_atms.oficina = 250 THEN 251
			ELSE at37_atms.oficina
		END AS oficina
	FROM atsudeban.at37_atms AS at37_atms

	UNION

	SELECT DISTINCT
		at37_otrosbancos.tipotransferencia AS tipotransferencia,
		at37_otrosbancos.clasificacion AS clasificacion,
		TRIM(at37_otrosbancos.tipocliente) AS tipocliente,
		TRIM(at37_otrosbancos.identificacioncliente) AS identificacioncliente,
		at37_otrosbancos.nombrecliente AS nombrecliente,
		at37_otrosbancos.tipoinstrumento AS tipoinstrumento,
		at37_otrosbancos.cuentacliente AS cuentacliente,
		at37_otrosbancos.moneda AS moneda,
		at37_otrosbancos.monto AS monto,
		at37_otrosbancos.fecha AS fecha,
		at37_otrosbancos.referencia AS referencia,
		CASE
			WHEN TRIM(at37_otrosbancos.identificacioncliente) IN ('V','E','P') AND at37_otrosbancos.empresaformacion IN ('0','1','2') THEN at37_otrosbancos.empresaformacion
			WHEN TRIM(at37_otrosbancos.identificacioncliente) NOT IN ('V','E','P') THEN '0'
		END AS empresaformacion,
		COALESCE(at37_otrosbancos.motivo, '0') AS motivo,
		at37_otrosbancos.direccionip AS direccionip,
		TRIM(at37_otrosbancos.tipocontraparte) AS tipocontraparte,
		at37_otrosbancos.identificacioncontraparte AS identificacioncontraparte,
		at37_otrosbancos.nombrecontraparte AS nombrecontraparte,
		at37_otrosbancos.tipoinstrumentocontraparte AS tipoinstrumentocontraparte,
		at37_otrosbancos.cuentacontraparte AS cuentacontraparte,
		CASE
			WHEN at37_otrosbancos.oficina = 250 THEN 251
			ELSE at37_otrosbancos.oficina
		END AS oficina
	FROM atsudeban.at37_otrosbancos_ AS at37_otrosbancos

	UNION

	SELECT DISTINCT
		at37_lbtrs.tipotransferencia AS tipotransferencia,
		at37_lbtrs.clasificacion AS clasificacion,
		TRIM(at37_lbtrs.tipocliente) AS tipocliente,
		TRIM(at37_lbtrs.identificacioncliente) AS identificacioncliente,
		at37_lbtrs.nombrecliente AS nombrecliente,
		at37_lbtrs.tipoinstrumento AS tipoinstrumento,
		at37_lbtrs.cuentacliente AS cuentacliente,
		at37_lbtrs.moneda AS moneda,
		at37_lbtrs.monto AS monto,
		at37_lbtrs.fecha AS fecha,
		at37_lbtrs.referencia AS referencia,
		CASE
			WHEN TRIM(at37_lbtrs.identificacioncliente) IN ('V','E','P') AND at37_lbtrs.empresaformacion IN ('0','1','2') THEN at37_lbtrs.empresaformacion
			WHEN TRIM(at37_lbtrs.identificacioncliente) NOT IN ('V','E','P') THEN '0'
		END AS empresaformacion,
		COALESCE(at37_lbtrs.motivo, '0') AS motivo,
		at37_lbtrs.direccionip AS direccionip,
		TRIM(at37_lbtrs.tipocontraparte) AS tipocontraparte,
		at37_lbtrs.identificacioncontraparte AS identificacioncontraparte,
		at37_lbtrs.nombrecontraparte AS nombrecontraparte,
		at37_lbtrs.tipoinstrumentocontraparte AS tipoinstrumentocontraparte,
		at37_lbtrs.cuentacontraparte AS cuentacontraparte,
		CASE
			WHEN at37_lbtrs.oficina = 250 THEN 251
			ELSE at37_lbtrs.oficina
		END AS oficina
	FROM atsudeban.at37_lbtrs AS at37_lbtrs

	UNION

	SELECT DISTINCT
		at37_propias.tipotransferencia AS tipotransferencia,
		at37_propias.clasificacion AS clasificacion,
		TRIM(at37_propias.tipocliente) AS tipocliente,
		TRIM(at37_propias.identificacioncliente) AS identificacioncliente,
		at37_propias.nombrecliente AS nombrecliente,
		at37_propias.tipoinstrumento AS tipoinstrumento,
		at37_propias.cuentacliente AS cuentacliente,
		at37_propias.moneda AS moneda,
		at37_propias.monto AS monto,
		at37_propias.fecha AS fecha,
		at37_propias.referencia AS referencia,
		CASE
			WHEN TRIM(at37_propias.identificacioncliente) IN ('V','E','P') AND at37_propias.empresaformacion IN ('0','1','2') THEN at37_propias.empresaformacion
			WHEN TRIM(at37_propias.identificacioncliente) NOT IN ('V','E','P') THEN '0'
		END AS empresaformacion,
		COALESCE(at37_propias.motivo, '0') AS motivo,
		at37_propias.direccionip AS direccionip,
		TRIM(at37_propias.tipocontraparte) AS tipocontraparte,
		at37_propias.identificacioncontraparte AS identificacioncontraparte,
		at37_propias.nombrecontraparte AS nombrecontraparte,
		at37_propias.tipoinstrumentocontraparte AS tipoinstrumentocontraparte,
		at37_propias.cuentacontraparte AS cuentacontraparte,
		CASE
			WHEN at37_propias.oficina = 250 THEN 251
			ELSE at37_propias.oficina
		END AS oficina
	FROM atsudeban.at37_propias_ AS at37_propias;'''
    hook.run(sql_query_deftxt)

def AT37_BC(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS atsudeban.at37_bc (
	tipotransferencia	NUMERIC(38),
	clasificacion	NUMERIC(38),
	tipocliente	VARCHAR(3),
	identificacioncliente	VARCHAR(15),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	VARCHAR(5),
	cuentacliente	CHAR(20),
	moneda	CHAR(3),
	monto	NUMERIC(38,2),
	fecha	CHAR(8),
	referencia	VARCHAR(30),
	empresaformacion	VARCHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	tipocontraparte	VARCHAR(3),
	identificacioncontraparte	VARCHAR(30),
	nombrecontraparte	VARCHAR(100),
	tipoinstrumentocontraparte	VARCHAR(5),
	cuentacontraparte	VARCHAR(20),
	oficina	NUMERIC(38)
	);'''
    hook.run(sql_query_deftxt)

	# TRUNCATE
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_BC;'''
    hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO atsudeban.at37_bc (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	moneda,
	monto,
	fecha,
	referencia,
	empresaformacion,
	motivo,
	direccionip,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	oficina
	)
	SELECT 
		at37_atss.tipotransferencia AS tipotransferencia,
		at37_atss.clasificacion AS clasificacion,
		at37_atss.tipocliente AS tipocliente,
		at37_atss.identificacioncliente AS identificacioncliente,
		regexp_replace(at37_atss.nombrecliente,'(^[[:space:]]+|[[:space:]]+$)','','g') AS nombrecliente,
		at37_atss.tipoinstrumento AS tipoinstrumento,
		at37_atss.cuentacliente AS cuentacliente,
		at37_atss.moneda AS moneda,
		at37_atss.monto AS monto,
		at37_atss.fecha AS fecha,
		at37_atss.referencia AS referencia,
		trim(at37_atss.empresaformacion) AS empresaformacion,
		at37_atss.motivo AS motivo,
		at37_atss.direccionip AS direccionip,
		trim(tipocontraparte) AS tipocontraparte,
		at37_atss.identificacioncontraparte AS identificacioncontraparte,
		regexp_replace(at37_atss.nombrecontraparte,'(^[[:space:]]+|[[:space:]]+$)','','g') AS nombrecontraparte,
		at37_atss.tipoinstrumentocontraparte AS tipoinstrumentocontraparte,
		at37_atss.cuentacontraparte AS cuentacontraparte,
		at37_atss.oficina AS oficina
	FROM atsudeban.at37_atss AS at37_atss
	WHERE (
		
		AT37_ATSS.TIPOCONTRAPARTE IN ('V','E','J','G','P','R','C')
		
		AND (
			AT37_ATSS.CLASIFICACION = 1 AND AT37_ATSS.IDENTIFICACIONCONTRAPARTE <> AT37_ATSS.IDENTIFICACIONCLIENTE OR AT37_ATSS.CLASIFICACION = 2
		)
		AND (
			(AT37_ATSS.TIPOCLIENTE = 'V' AND AT37_ATSS.IDENTIFICACIONCLIENTE BETWEEN 1 AND 40000000)
			OR
			(AT37_ATSS.TIPOCLIENTE = 'E' AND AT37_ATSS.IDENTIFICACIONCLIENTE BETWEEN 1 AND 1500000)
			OR
			(AT37_ATSS.TIPOCLIENTE = 'E' AND AT37_ATSS.IDENTIFICACIONCLIENTE BETWEEN 80000000 AND 100000000)
			OR
			AT37_ATSS.TIPOCLIENTE NOT IN ('V','E')
		)
		AND (
			AT37_ATSS.TIPOCLIENTE IN ('E','G','J','P','V','R','I','C')
		)
		AND (
			AT37_ATSS.TIPOINSTRUMENTO IN ('1','2','13','20','14')
		)
		AND (
			AT37_ATSS.TIPOINSTRUMENTO IN ('1','2') AND AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE IN ('1','2','0') AND AT37_ATSS.MONEDA = 'VES'
			OR
			AT37_ATSS.TIPOINSTRUMENTO IN ('13','14') AND AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE IN ('13','14') AND AT37_ATSS.MONEDA <> 'VES'
		)
		AND (
			AT37_ATSS.MONEDA = 'VES' AND AT37_ATSS.MONTO >= 100 OR AT37_ATSS.MONEDA <> 'VES' AND AT37_ATSS.MONTO > 0
		)
		AND (
			AT37_ATSS.TIPOCLIENTE IN ('V','E','P') AND AT37_ATSS.EMPRESAFORMACION IN ('0','1','2')
			OR
			AT37_ATSS.TIPOCLIENTE NOT IN ('V','E','P') AND AT37_ATSS.EMPRESAFORMACION IN ('0')
		)
		AND (
			AT37_ATSS.TIPOTRANSFERENCIA = 1 AND AT37_ATSS.DIRECCIONIP <> '0'
			OR
			AT37_ATSS.TIPOTRANSFERENCIA <> 1 AND AT37_ATSS.DIRECCIONIP = '0'
		)
		AND (
			(AT37_ATSS.NOMBRECLIENTE IS NOT NULL) AND (AT37_ATSS.NOMBRECLIENTE <> ' ') AND (AT37_ATSS.NOMBRECLIENTE <> '0') AND (AT37_ATSS.NOMBRECLIENTE <> '')
		)
		AND (
			AT37_ATSS.TIPOINSTRUMENTOCONTRAPARTE IN ('0','1','2','13','14')
		)
		AND (SUBSTRING(at37_atss.cuentacliente FROM 1 FOR 4) <> '0146' AND at37_atss.oficina NOT IN (700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715))
		
		);'''
    hook.run(sql_query_deftxt)

def IMPORTAR_INSUMO(**kwargs):
    try:
        # Define la informacion del bucket y el objeto en GCS
        gcs_bucket = 'airflow-dags-data'
        gcs_object = 'data/AT37/INSUMOS/Asignaciones_Direcciones_IPVE.csv'
        
        # Inicializa los hooks
        postgres_hook = PostgresHook(postgres_conn_id='at37')
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
        logger.info("Truncando la tabla FILE_AT37P.AT37_IP_SUDEBAN...")
        postgres_hook.run("TRUNCATE TABLE FILE_AT37P.AT37_IP_SUDEBAN;")
        logger.info("Tabla FILE_AT37P.AT37_IP_SUDEBAN truncada exitosamente.")

        # 4. Preparar consulta COPY
        sql_copy = """
        COPY FILE_AT37P.AT37_IP_SUDEBAN (
            PAIS, 
			IP, 
			LONGITUD, 
			TIPO
        )
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');
        """
        
        # 5. Descargar archivo temporal y crea directorio temporal
        temp_dir = tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, 'Asignaciones_Direcciones_IPVE.csv')
        
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
                logger.info("Primeras lineas del CSV:\n" + "".join(lines))

            # 8. Ejecutar COPY
            logger.info("Iniciando carga de datos a PostgreSQL...")
            start_time = datetime.now()
            
            postgres_hook.copy_expert(sql=sql_copy, filename=local_file_path)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Carga completada en {duration:.2f} segundos")
            
            # 9. Verificar conteo de registros
            count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_AT37P.AT37_IP_SUDEBAN;")[0]
            logger.info(f"Total de registros cargados: {count}")
            
            if count == 0:
                raise Exception("No se cargaron registros - verificar formato del archivo CSV")
                
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

def AT37_IP_CONATEL(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_IP_SUDEBAN (
    PAIS VARCHAR(10), 
    IP VARCHAR(50), 
    LONGITUD NUMERIC(15), 
    TIPO VARCHAR(50));'''
    hook.run(sql_query_deftxt)

	# TRUNCAR
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_IP_SUDEBAN;'''
    hook.run(sql_query_deftxt)
    
	# INSERTAR EN DESTINO
    sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_IP_SUDEBAN (
    PAIS, 
    IP, 
    LONGITUD, 
    TIPO
    )
	SELECT
		AT37_IP_SUDEBAN.PAIS,
		AT37_IP_SUDEBAN.IP,
		AT37_IP_SUDEBAN.LONGITUD,
		AT37_IP_SUDEBAN.TIPO
	FROM FILE_AT37P.AT37_IP_SUDEBAN AS AT37_IP_SUDEBAN;'''
    hook.run(sql_query_deftxt)

def AT37_IP_CON_SUDE(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_ASIG_IP (
	PAIS VARCHAR(5),
	IP_INICIO VARCHAR(20),
	IP_FIN VARCHAR(20),
	LONGITUD NUMERIC(20),
	IP_NUMINI NUMERIC(20),
	IP_NUMFIN NUMERIC(20)
	);'''
    hook.run(sql_query_deftxt)

	# truncar
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_ASIG_IP;'''
    hook.run(sql_query_deftxt)

	# insertar en destino
    sql_query_deftxt = '''INSERT INTO atsudeban.at37_asig_ip (
	pais,
	ip_inicio,
	longitud,
	ip_numini,
	ip_numfin,
	ip_fin
	)
	SELECT
		at37_ip_sudeban.pais AS pais,
		at37_ip_sudeban.ip AS ip_inicio,
		at37_ip_sudeban.longitud AS longitud,
		(
			CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 1) AS INTEGER) * (256 * 256 * 256) +
			CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 2) AS INTEGER) * (256 * 256) +
			CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 3) AS INTEGER) * 256 +
			CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 4) AS INTEGER)
		) AS ip_numini,
		(
			(
				CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 1) AS INTEGER) * (256 * 256 * 256) +
				CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 2) AS INTEGER) * (256 * 256) +
				CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 3) AS INTEGER) * 256 +
				CAST(SPLIT_PART(at37_ip_sudeban.ip, '.', 4) AS INTEGER)
			) + at37_ip_sudeban.longitud
		) - 1 AS ip_numfin,
        '' AS ip_fin
	FROM atsudeban.at37_ip_sudeban AS at37_ip_sudeban;'''
    hook.run(sql_query_deftxt)

def AT37_BC_I(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_BC_I (
	TIPOTRANSFERENCIA	NUMERIC(38),
	CLASIFICACION	NUMERIC(38),
	TIPOCLIENTE	VARCHAR(3),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	VARCHAR(5),
	CUENTACLIENTE	CHAR(20),
	MONEDA	CHAR(3),
	MONTO	NUMERIC(38,2),
	FECHA	CHAR(8),
	REFERENCIA	VARCHAR(30),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP	VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(30),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	VARCHAR(5),
	CUENTACONTRAPARTE	VARCHAR(20),
	OFICINA	NUMERIC(38),
	UBICACION_IP	VARCHAR(2),
	IP_NUM	NUMERIC(32)
	);'''
    hook.run(sql_query_deftxt)

	# truncar
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_BC_I;'''
    hook.run(sql_query_deftxt)

	# insertar en destino
    sql_query_deftxt = '''INSERT INTO atsudeban.at37_bc_i (
	tipotransferencia,
	clasificacion,
	tipocliente,
	identificacioncliente,
	nombrecliente,
	tipoinstrumento,
	cuentacliente,
	moneda,
	monto,
	fecha,
	referencia,
	empresaformacion,
	motivo,
	direccionip,
	tipocontraparte,
	identificacioncontraparte,
	nombrecontraparte,
	tipoinstrumentocontraparte,
	cuentacontraparte,
	oficina,
	ip_num,
	ubicacion_ip
	)
	SELECT DISTINCT
		tipotransferencia,
		clasificacion,
		tipocliente,
		identificacioncliente,
		REPLACE(nombrecliente,'          ',''),
		tipoinstrumento,
		cuentacliente,
		moneda,
		monto,
		fecha,
		referencia,
		empresaformacion,
		motivo,
		direccionip,
		tipocontraparte,
		identificacioncontraparte,
		nombrecontraparte,
		tipoinstrumentocontraparte,
		cuentacontraparte,
		oficina,
		(SPLIT_PART(direccionip, '.', 1)::INTEGER * (256 * 256 * 256) + 
		SPLIT_PART(direccionip, '.', 2)::INTEGER * (256 * 256) + 
		SPLIT_PART(direccionip, '.', 3)::INTEGER * 256 + 
		SPLIT_PART(direccionip, '.', 4)::INTEGER) AS ip_num,
		'' AS ubicacion_ip
    FROM atsudeban.at37_bc;'''
    hook.run(sql_query_deftxt)

def AT37_BC_I_IP(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_BC_I_IP (
	TIPOTRANSFERENCIA	NUMERIC(38),
	CLASIFICACION	NUMERIC(38),
	TIPOCLIENTE	VARCHAR(3),
	IDENTIFICACIONCLIENTE	VARCHAR(15),
	NOMBRECLIENTE	VARCHAR(250),
	TIPOINSTRUMENTO	VARCHAR(5),
	CUENTACLIENTE	CHAR(20),
	MONEDA	CHAR(3),
	MONTO	NUMERIC(38,2),
	FECHA	CHAR(8),
	REFERENCIA	VARCHAR(30),
	EMPRESAFORMACION	VARCHAR(3),
	MOTIVO	VARCHAR(100),
	DIRECCIONIP	VARCHAR(50),
	TIPOCONTRAPARTE	VARCHAR(3),
	IDENTIFICACIONCONTRAPARTE	VARCHAR(30),
	NOMBRECONTRAPARTE	VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE	VARCHAR(5),
	CUENTACONTRAPARTE	VARCHAR(20),
	OFICINA	NUMERIC(38),
	UBICACION_IP	VARCHAR(10),
	IP_NUM	NUMERIC(32)
	);'''
    hook.run(sql_query_deftxt)

	# truncar
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_BC_I_IP;'''
    hook.run(sql_query_deftxt)

	# insertar en destino
    sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_BC_I_IP (
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA,
	UBICACION_IP,
	IP_NUM
	)
	SELECT DISTINCT
		AT37_BC_I.TIPOTRANSFERENCIA,
		AT37_BC_I.CLASIFICACION,
		AT37_BC_I.TIPOCLIENTE,
		AT37_BC_I.IDENTIFICACIONCLIENTE,
		AT37_BC_I.NOMBRECLIENTE,
		AT37_BC_I.TIPOINSTRUMENTO,
		AT37_BC_I.CUENTACLIENTE,
		AT37_BC_I.MONEDA,
		AT37_BC_I.MONTO,
		AT37_BC_I.FECHA,
		AT37_BC_I.REFERENCIA,
		TRIM(AT37_BC_I.EMPRESAFORMACION),
		AT37_BC_I.MOTIVO,
		AT37_BC_I.DIRECCIONIP,
		TRIM(AT37_BC_I.TIPOCONTRAPARTE),
		AT37_BC_I.IDENTIFICACIONCONTRAPARTE,
		AT37_BC_I.NOMBRECONTRAPARTE,
		AT37_BC_I.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_BC_I.CUENTACONTRAPARTE,
		AT37_BC_I.OFICINA,
		CASE
			WHEN (SELECT AT37_ASIG_IP.PAIS 
            	  FROM ATSUDEBAN.AT37_ASIG_IP 
                  WHERE AT37_BC_I.IP_NUM BETWEEN AT37_ASIG_IP.IP_NUMINI AND AT37_ASIG_IP.IP_NUMFIN) = 'VE' THEN '1'
			WHEN AT37_BC_I.TIPOTRANSFERENCIA <> 1 THEN '0'
			WHEN AT37_BC_I.TIPOTRANSFERENCIA = 1 AND AT37_BC_I.DIRECCIONIP = '192.168.11.30' THEN '1'
			ELSE '2'
		END,
		AT37_BC_I.IP_NUM
	FROM ATSUDEBAN.AT37_BC_I as AT37_BC_I;'''
    hook.run(sql_query_deftxt)

def AT37_BC_TOTAL(**kwargs):
    hook = PostgresHook(postgres_conn_id='at37')

	# creamos tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS atsudeban.at37_bc_total (
	tipotransferencia	NUMERIC(38),
	clasificacion	NUMERIC(38),
	tipocliente	VARCHAR(3),
	identificacioncliente	VARCHAR(15),
	nombrecliente	VARCHAR(250),
	tipoinstrumento	VARCHAR(5),
	cuentacliente	CHAR(20),
	moneda	CHAR(3),
	monto	NUMERIC(38,2),
	fecha	CHAR(8),
	referencia	VARCHAR(30),
	empresaformacion	VARCHAR(3),
	motivo	VARCHAR(100),
	direccionip	VARCHAR(50),
	tipocontraparte	VARCHAR(3),
	identificacioncontraparte	VARCHAR(30),
	nombrecontraparte	VARCHAR(100),
	tipoinstrumentocontraparte	VARCHAR(5),
	cuentacontraparte	VARCHAR(20),
	oficina	NUMERIC(38),
	ubicacion_ip	VARCHAR(2)
	);'''
    hook.run(sql_query_deftxt)

	# truncar
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_BC_TOTAL;'''
    hook.run(sql_query_deftxt)

	# insertar en destino
    sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_BC_TOTAL (
	TIPOTRANSFERENCIA,
	CLASIFICACION,
	TIPOCLIENTE,
	IDENTIFICACIONCLIENTE,
	NOMBRECLIENTE,
	TIPOINSTRUMENTO,
	CUENTACLIENTE,
	MONEDA,
	MONTO,
	FECHA,
	REFERENCIA,
	EMPRESAFORMACION,
	MOTIVO,
	DIRECCIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	OFICINA,
	UBICACION_IP
	)
	SELECT DISTINCT
		TIPOTRANSFERENCIA,
		CLASIFICACION,
		TIPOCLIENTE,
		IDENTIFICACIONCLIENTE,
		NOMBRECLIENTE,
		TIPOINSTRUMENTO,
		CUENTACLIENTE,
		MONEDA,
		MONTO,
		FECHA,
		REFERENCIA,
		EMPRESAFORMACION,
		MOTIVO,
		DIRECCIONIP,
		TIPOCONTRAPARTE,
		IDENTIFICACIONCONTRAPARTE,
		REPLACE(NOMBRECONTRAPARTE, '                ', ' ') AS NOMBRECONTRAPARTE,
		TIPOINSTRUMENTOCONTRAPARTE,
		CUENTACONTRAPARTE,
		OFICINA,
		UBICACION_IP
	FROM ATSUDEBAN.AT37_BC_I_IP;'''
    hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT37',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

Execution_of_the_Scenario_PKG_ATM37_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PKG_ATM37_version_002_task',
    trigger_dag_id='PKG_ATM37', 
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_PKG_ATM37_DUPLICADAS_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PKG_ATM37_DUPLICADAS_version_002_task',
    trigger_dag_id='PKG_ATM37_DUPLICADAS', 
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_AT37_IP_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT37_IP_version_002_task',
    trigger_dag_id='AT37_IP', 
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_PKG_LBTR37_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PKG_LBTR37_version_002_task',
    trigger_dag_id='PKG_LBTR37', 
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_PKG_OTROS37_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PKG_OTROS37_version_002_task',
    trigger_dag_id='PKG_OTROS37',
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_PK_PROPIAS37_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PK_PROPIAS37_version_002_task',
    trigger_dag_id='PK_PROPIAS37', 
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_PKG_AT37_PROPIAS_DUPLICADAS_version_002_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_PKG_AT37_PROPIAS_DUPLICADAS_version_002_task',
    trigger_dag_id='PKG_AT37_PROPIAS_DUPLICADAS', 
    wait_for_completion=True,
    dag=dag
)

AT37_ALL_task = PythonOperator(
    task_id='AT37_ALL_task',
    python_callable=AT37_ALL,
    dag=dag
)

AT37_BC_task = PythonOperator(
    task_id='AT37_BC_task',
    python_callable=AT37_BC,
    dag=dag
)

IMPORTAR_INSUMO_task = PythonOperator(
    task_id='IMPORTAR_INSUMO_task',
    python_callable=IMPORTAR_INSUMO,
    dag=dag
)

AT37_IP_CONATEL_task = PythonOperator(
    task_id='AT37_IP_CONATEL_task',
    python_callable=AT37_IP_CONATEL,
    dag=dag
)

AT37_IP_CON_SUDE_task = PythonOperator(
    task_id='AT37_IP_CON_SUDE_task',
    python_callable=AT37_IP_CON_SUDE,
    dag=dag
)

AT37_BC_I_task = PythonOperator(
    task_id='AT37_BC_I_task',
    python_callable=AT37_BC_I,
    dag=dag
)

AT37_BC_I_IP_task = PythonOperator(
    task_id='AT37_BC_I_IP_task',
    python_callable=AT37_BC_I_IP,
    dag=dag
)

AT37_BC_TOTAL_task = PythonOperator(
    task_id='AT37_BC_TOTAL_task',
    python_callable=AT37_BC_TOTAL,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
Execution_of_the_Scenario_PKG_ATM37_version_002_task >> Execution_of_the_Scenario_PKG_ATM37_DUPLICADAS_version_002_task >> Execution_of_the_Scenario_AT37_IP_version_002_task >> Execution_of_the_Scenario_PKG_LBTR37_version_002_task >> Execution_of_the_Scenario_PKG_OTROS37_version_002_task >> Execution_of_the_Scenario_PK_PROPIAS37_version_002_task >> Execution_of_the_Scenario_PKG_AT37_PROPIAS_DUPLICADAS_version_002_task >> AT37_ALL_task >> AT37_BC_task >> IMPORTAR_INSUMO_task >> AT37_IP_CONATEL_task >> AT37_IP_CON_SUDE_task >> AT37_BC_I_task >> AT37_BC_I_IP_task >> AT37_BC_TOTAL_task
