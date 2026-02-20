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

def vFecha_Carga(**kwargs):
	hook = PostgresHook(postgres_conn_id='mi_conexion')
	sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'DD/MM/YYYY');'''
	result = hook.get_records(sql_query)
	Variable.set('vFecha_Carga', serialize_value(result[0][0]))

def DELETE_REPROCESO(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')
	
	vFecha_Carga = get_variable('vFecha_Carga')

	sql_query_deftxt = f'''DELETE FROM atsudeban.at37_lbtrs_excluidas WHERE fechacarga = TO_DATE('{vFecha_Carga}', 'DD/MM/YY');'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_EXCLUIDAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	vFecha_Carga = get_variable('vFecha_Carga')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_LBTRS_EXCLUIDAS (
	FECHA_EJECUCION DATE,
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
	OFICINA NUMERIC,
	FECHACARGA DATE
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_LBTRS_EXCLUIDAS;'''
	hook.run(sql_query_deftxt)

	# INSERTAMOS EN DESTINO
	sql_query_deftxt = f'''INSERT INTO ATSUDEBAN.AT37_LBTRS_EXCLUIDAS (
	FECHA_EJECUCION,
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
	FECHACARGA
	)
	SELECT
		FECHA_EJECUCION,
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
		TO_DATE('{vFecha_Carga}', 'DD/MM/YY')
	FROM ATSUDEBAN.AT37_LBTRS AS AT37_LBTRS
	WHERE (AT37_LBTRS.NOMBRECONTRAPARTE LIKE '%DISPONIBLE%');'''
	hook.run(sql_query_deftxt)

def DELETE_LBTR_EXCLUIDAS(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	sql_query_deftxt = '''DELETE FROM atsudeban.at37_lbtrs WHERE nombrecontraparte LIKE '%DISPONIBLE%';'''
	hook.run(sql_query_deftxt)

def IMPORTAR_INSUMO(**kwargs):
    try:
        # Define la informacion del bucket y el objeto en GCS
        gcs_bucket = 'airflow-dags-data'
        gcs_object = 'data/AT37/INSUMOS/PLANTILLA_AT37_MANUAL.txt'
        
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
        logger.info("Truncando la tabla FILE_AT.AT37_FILE_MANUALES...")
        postgres_hook.run("TRUNCATE TABLE FILE_AT.AT37_FILE_MANUALES;")
        logger.info("Tabla FILE_AT.AT37_FILE_MANUALES truncada exitosamente.")

        # 4. Preparar consulta COPY
        sql_copy = """
        COPY FILE_AT.AT37_FILE_MANUALES (
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
			DIRECIONIP,
			TIPOCONTRAPARTE,
			IDENTIFICACIONCONTRAPARTE,
			NOMBRECONTRAPARTE,
			TIPOINSTRUMENTOCONTRAPARTE,
			CUENTACONTRAPARTE,
			ENTE,
			CANAL
        )
        FROM STDIN
        WITH (FORMAT text, DELIMITER ';', ENCODING 'UTF8');
        """
        
        # 5. Descargar archivo temporal y crea directorio temporal
        temp_dir = tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, 'PLANTILLA_AT37_MANUAL.txt')
        
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
            count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_AT.AT37_FILE_MANUALES;")[0]
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

def AT37_LBTR_MANUALES(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# CREAMOS TABLA DESTINO
	sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT37_LBTR_OPMANUALES (
	TIPOTRANSFERENCIA VARCHAR(100),
	CLASIFICACION VARCHAR(100),
	TIPOCLIENTE VARCHAR(100),
	IDENTIFICACIONCLIENTE VARCHAR(100),
	NOMBRECLIENTE VARCHAR(100),
	TIPOINSTRUMENTO VARCHAR(100),
	CUENTACLIENTE VARCHAR(100),
	MONEDA VARCHAR(100),
	MONTO NUMERIC(38,2),
	FECHA VARCHAR(100),
	REFERENCIA VARCHAR(100),
	EMPRESAFORMACION VARCHAR(100),
	MOTIVO VARCHAR(100),
	DIRECIONIP VARCHAR(100),
	TIPOCONTRAPARTE VARCHAR(100),
	IDENTIFICACIONCONTRAPARTE VARCHAR(100),
	NOMBRECONTRAPARTE VARCHAR(100),
	TIPOINSTRUMENTOCONTRAPARTE VARCHAR(100),
	CUENTACONTRAPARTE VARCHAR(100),
	ENTE VARCHAR(100),
	CANAL VARCHAR(100)
	);'''
	hook.run(sql_query_deftxt)

	# TRUNCAR
	sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT37_LBTR_OPMANUALES;'''
	hook.run(sql_query_deftxt)

	# INSERTAR EN DESTINO
	sql_query_deftxt = '''INSERT INTO ATSUDEBAN.AT37_LBTR_OPMANUALES (
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
	DIRECIONIP,
	TIPOCONTRAPARTE,
	IDENTIFICACIONCONTRAPARTE,
	NOMBRECONTRAPARTE,
	TIPOINSTRUMENTOCONTRAPARTE,
	CUENTACONTRAPARTE,
	ENTE,
	CANAL
	)
	SELECT
		AT37_FILE_MANUALES.TIPOTRANSFERENCIA,
		AT37_FILE_MANUALES.CLASIFICACION,
		AT37_FILE_MANUALES.TIPOCLIENTE,
		AT37_FILE_MANUALES.IDENTIFICACIONCLIENTE,
		AT37_FILE_MANUALES.NOMBRECLIENTE,
		AT37_FILE_MANUALES.TIPOINSTRUMENTO,
		AT37_FILE_MANUALES.CUENTACLIENTE,
		AT37_FILE_MANUALES.MONEDA,
		AT37_FILE_MANUALES.MONTO,
		AT37_FILE_MANUALES.FECHA,
		AT37_FILE_MANUALES.REFERENCIA,
		AT37_FILE_MANUALES.EMPRESAFORMACION,
		AT37_FILE_MANUALES.MOTIVO,
		AT37_FILE_MANUALES.DIRECIONIP,
		AT37_FILE_MANUALES.TIPOCONTRAPARTE,
		AT37_FILE_MANUALES.IDENTIFICACIONCONTRAPARTE,
		AT37_FILE_MANUALES.NOMBRECONTRAPARTE,
		AT37_FILE_MANUALES.TIPOINSTRUMENTOCONTRAPARTE,
		AT37_FILE_MANUALES.CUENTACONTRAPARTE,
		AT37_FILE_MANUALES.ENTE,
		AT37_FILE_MANUALES.CANAL
	FROM FILE_AT.AT37_FILE_MANUALES AS AT37_FILE_MANUALES;'''
	hook.run(sql_query_deftxt)

def AT37_LBTR_ADDMANUALES(**kwargs):
	hook = PostgresHook(postgres_conn_id='at37')

	# insertar en destino
	sql_query_deftxt = '''INSERT INTO atsudeban.at37_lbtrs (
	fecha_ejecucion,
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
	empresformacion,
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
		at37_lbtr_opmanuales.tipotransferencia AS tipotransferencia,
		at37_lbtr_opmanuales.clasificacion AS clasificacion,
		at37_lbtr_opmanuales.tipocliente AS tipocliente,
		at37_lbtr_opmanuales.identificacioncliente AS identificacioncliente,
		at37_lbtr_opmanuales.nombrecliente AS nombrecliente,
		at37_lbtr_opmanuales.tipoinstrumento AS tipoinstrumento,
		at37_lbtr_opmanuales.cuentacliente AS cuentacliente,
		at37_lbtr_opmanuales.moneda AS moneda,
		at37_lbtr_opmanuales.monto AS monto,
		at37_lbtr_opmanuales.fecha AS fecha,
		at37_lbtr_opmanuales.referencia AS referencia,
		at37_lbtr_opmanuales.empresformacion AS empresformacion,
		at37_lbtr_opmanuales.motivo AS motivo,
		at37_lbtr_opmanuales.direcionip AS direccionip,
		at37_lbtr_opmanuales.tipocontraparte AS tipocontraparte,
		at37_lbtr_opmanuales.identificacioncontraparte AS identificacioncontraparte,
		at37_lbtr_opmanuales.nombrecontraparte AS nombrecontraparte,
		at37_lbtr_opmanuales.tipoinstrumentocontraparte AS tipoinstrumentocontraparte,
		at37_lbtr_opmanuales.cuentacontraparte AS cuentacontraparte,
		at37_lbtr_opmanuales.ente AS oficina
	FROM atsudeban.at37_lbtr_opmanuales as at37_lbtr_opmanuales;'''
	hook.run(sql_query_deftxt)

###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='PKG_EXCLUIDAS',
		  default_args=default_args,
		  schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
		  catchup=False)

vFecha_Carga_task = PythonOperator(
	task_id='vFecha_Carga_task',
	python_callable=vFecha_Carga,
	dag=dag
)

AT37_LBTR_EXCLUIDAS_task = PythonOperator(
	task_id='AT37_LBTR_EXCLUIDAS_task',
	python_callable=AT37_LBTR_EXCLUIDAS,
	dag=dag
)

DELETE_REPROCESO_task = PythonOperator(
	task_id='DELETE_REPROCESO_task',
	python_callable=DELETE_REPROCESO,
	dag=dag
)

DELETE_LBTR_EXCLUIDAS_task = PythonOperator(
	task_id='DELETE_LBTR_EXCLUIDAS_task',
	python_callable=DELETE_LBTR_EXCLUIDAS,
	dag=dag
)

IMPORTAR_INSUMO_task = PythonOperator(
    task_id='IMPORTAR_INSUMO_task',
    python_callable=IMPORTAR_INSUMO,
    dag=dag
)

AT37_LBTR_MANUALES_task = PythonOperator(
	task_id='AT37_LBTR_MANUALES_task',
	python_callable=AT37_LBTR_MANUALES,
	dag=dag
)

AT37_LBTR_ADDMANUALES_task = PythonOperator(
	task_id='AT37_LBTR_ADDMANUALES_task',
	python_callable=AT37_LBTR_ADDMANUALES,
	dag=dag
)

###### SECUENCIA DE EJECUCION ######
vFecha_Carga_task >> AT37_LBTR_EXCLUIDAS_task >> DELETE_REPROCESO_task >> DELETE_LBTR_EXCLUIDAS_task >> IMPORTAR_INSUMO_task >> AT37_LBTR_MANUALES_task >> AT37_LBTR_ADDMANUALES_task
