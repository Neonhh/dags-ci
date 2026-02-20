from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import time
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import tempfile 

### FUNCIONES DE CADA TAREA ###

def IMPORTAR_INSUMO(**kwargs):
    try:
        # Define la informacion del bucket y el objeto en GCS
        gcs_bucket = 'us-central1-composer-bancar-794f4b44-bucket'
        gcs_object = 'data/ALI/INSUMOS/SOURCE/FILE_ENCAJE/ENCAJE.csv'
        
        # Inicializa los hooks
        postgres_hook = PostgresHook(postgres_conn_id='nombre_conexion')
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

        # 1. Validar conexion a PostgreSQL
        try:
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            kwargs['ti'].log.info("Conexion a PostgreSQL validada exitosamente")
        except Exception as e:
            kwargs['ti'].log.error(f"Error al conectar a PostgreSQL: {str(e)}")
            raise

        # 2. Validar acceso a GCS
        try:
            if not gcs_hook.exists(bucket_name=gcs_bucket, object_name=gcs_object):
                raise Exception(f"El archivo {gcs_object} no existe en el bucket {gcs_bucket}")
            kwargs['ti'].log.info("Validacion de archivo GCS exitosa")
        except Exception as e:
            kwargs['ti'].log.error(f"Error al validar archivo en GCS: {str(e)}")
            raise

        # 3. Truncar tabla
        kwargs['ti'].log.info("Truncando la tabla FILE_ENCAJE.ENCAJE...")
        postgres_hook.run("TRUNCATE TABLE FILE_ENCAJE.ENCAJE;")
        kwargs['ti'].log.info("Tabla FILE_ENCAJE.ENCAJE truncada exitosamente.")

        # 4. Preparar consulta COPY
        sql_copy = """
        COPY FILE_ENCAJE.ENCAJE (
            DESCRIPCION,
            INSUMO,
            LUNES,
            MARTES,
            MIERCOLES,
            JUEVES,
            VIERNES
        )
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');
        """
        
        # 5. Descargar archivo temporal y crea directorio temporal
        temp_dir = tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, 'ENCAJE.csv')
        
        try:
            kwargs['ti'].log.info(f"Descargando archivo '{gcs_object}' desde GCS...")
            gcs_hook.download(
                bucket_name=gcs_bucket,
                object_name=gcs_object,
                filename=local_file_path
            )
            
            # 6. Validar archivo descargado
            file_size = os.path.getsize(local_file_path)
            if file_size == 0:
                raise Exception("El archivo descargado esta vacio")
                
            kwargs['ti'].log.info(f"Archivo descargado correctamente. Tamaño: {file_size} bytes")
            
            # 7. Mostrar primeras li­neas para debug (ya usa 'windows-1252' segun tu codigo)
            with open(local_file_path, 'r', encoding='utf8') as f:
                lines = [next(f) for _ in range(5)]
                kwargs['ti'].log.info("Primeras lineas del CSV:\n" + "".join(lines))

            # 8. Ejecutar COPY
            kwargs['ti'].log.info("Iniciando carga de datos a PostgreSQL...")
            start_time = datetime.now()
            
            postgres_hook.copy_expert(sql=sql_copy, filename=local_file_path)
            
            duration = (datetime.now() - start_time).total_seconds()
            kwargs['ti'].log.info(f"Carga completada en {duration:.2f} segundos")
            
            # 9. Verificar conteo de registros
            count = postgres_hook.get_first("SELECT COUNT(*) FROM FILE_ENCAJE.ENCAJE;")[0]
            kwargs['ti'].log.info(f"Total de registros cargados: {count}")
            
            if count == 0:
                raise Exception("No se cargaron registros - verificar formato del archivo CSV")
                
        except Exception as e:
            kwargs['ti'].log.error(f"Error durante la carga de datos: {str(e)}")
            
            # Intentar leer el archivo para debug (ya usa 'windows-1252' segun tu codigo)
            try:
                with open(local_file_path, 'r', encoding='utf8') as f:
                    sample = f.read(1000)
                    kwargs['ti'].log.info(f"Contenido parcial del archivo:\n{sample}")
            except Exception as read_error:
                kwargs['ti'].log.error(f"No se pudo leer el archivo para debug: {str(read_error)}")
            
            raise
            
        finally:
            # Limpieza siempre se ejecuta
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                kwargs['ti'].log.info(f"Archivo temporal eliminado: {local_file_path}")
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
                kwargs['ti'].log.info(f"Directorio temporal eliminado: {temp_dir}")
                
    except Exception as e:
        kwargs['ti'].log.error(f"Error general en IMPORTAR_INSUMO: {str(e)}")
        # Registrar el error completo para diagnostico
        import traceback
        kwargs['ti'].log.error("Traceback completo:\n" + traceback.format_exc())
        raise

def ENCAJE(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ATER_STG.ENCAJE_F010 (
	DESCRIPCION VARCHAR(100),
	INSUMO VARCHAR(100),
	LUNES NUMERIC(15,2),
	MARTES NUMERIC(15,2),
	MIERCOLES NUMERIC(15,2),
	JUEVES NUMERIC(15,2),
	VIERNES NUMERIC(15,2)
    );'''
    hook.run(sql_query_deftxt)

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ater_stg.encaje_f010;'''
    hook.run(sql_query_deftxt)

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ater_stg.encaje_f010 (
	DESCRIPCION,
	INSUMO,
	LUNES,
	MARTES,
	MIERCOLES,
	JUEVES,
	VIERNES
    )
    SELECT
        ENCAJE.DESCRIPCION,
        ENCAJE.INSUMO,
        ENCAJE.LUNES,
        ENCAJE.MARTES,
        ENCAJE.MIERCOLES,
        ENCAJE.JUEVES,
        ENCAJE.VIERNES
    FROM FILE_ENCAJE.ENCAJE AS ENCAJE;'''
    hook.run(sql_query_deftxt)

def ENCAJE_TOTAL(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ater.encaje (
	descripcion	VARCHAR(100),
	insumo	VARCHAR(100),
	lunes	NUMERIC(15,2),
	martes	NUMERIC(15,2),
	miercoles	NUMERIC(15,2),
	jueves	NUMERIC(15,2),
	viernes	NUMERIC(15,2),
	formulario	VARCHAR(100),
	codigo	VARCHAR(100)
    );'''
    hook.run(sql_query_deftxt)

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ater.encaje;'''
    hook.run(sql_query_deftxt)

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ater.encaje (
	DESCRIPCION,
	INSUMO,
	LUNES,
	MARTES,
	MIERCOLES,
	JUEVES,
	VIERNES,
    FORMULARIO,
    CODIGO
    )
    SELECT
        ENCAJE_F010.DESCRIPCION,
        ENCAJE_F010.INSUMO,
        ENCAJE_F010.LUNES,
        ENCAJE_F010.MARTES,
        ENCAJE_F010.MIERCOLES,
        ENCAJE_F010.JUEVES,
        ENCAJE_F010.VIERNES,
        NULL AS FORMULARIO,
        NULL AS CODIGO
    FROM ATER_STG.ENCAJE_F010 AS ENCAJE_F010;'''
    hook.run(sql_query_deftxt)

def ALI_F010(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS ater.ali_f010 (
	clave	VARCHAR(15),
	clave_1	VARCHAR(15),
	lunes	NUMERIC(15),
	martes	NUMERIC(15),
	miercoles	NUMERIC(15),
	jueves	NUMERIC(15),
	viernes	NUMERIC(15),
	total	NUMERIC(15,1),
	promedio	NUMERIC(15)
    );'''
    hook.run(sql_query_deftxt)

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ater.ali_f010;'''
    hook.run(sql_query_deftxt)

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ater.ali_f010 (
    CLAVE, 
    CLAVE_1, 
    LUNES, 
    MARTES, 
    MIERCOLES, 
    JUEVES, 
    VIERNES, 
    TOTAL, 
    PROMEDIO
    )
    SELECT 
        TRIM(ater_parametros.clave) AS CLAVE, 
        TRIM(ater_parametros.clave_1) AS CLAVE_1,
        0,
        0,
        0,
        0,
        0,
        0,
        0
    FROM ater.ater_parametros AS ater_parametros
    WHERE ater_parametros.formulario = TRIM('F010');'''
    hook.run(sql_query_deftxt)


def ACTUALIZA_F010(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # CONSTANTE_F010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = 0, martes = 0, miercoles = 0, jueves = 0, viernes = 0, total = 0, promedio = 0
WHERE clave IN ('111030', '115000', '116030', '131140', '133140', '215990', '216000', '241010', '241090', '246040', '246990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'LUNES') 
    END,
    martes = CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'MARTES') 
    END,
    miercoles = CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'MIERCOLES') 
    END,
    jueves = CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'JUEVES') 
    END,
    viernes = CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11101' AND cb_bacom_final.dia = 'VIERNES') 
    END
WHERE ali_f010.clave = '111010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11104' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE ali_f010.clave = '111040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11202' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '112020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112033
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacent_final.cuenta = '11203203' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '11203203' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '112033';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_119000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'LUNES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'LUNES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'LUNES')
        END
    ),
    martes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'MARTES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'MARTES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'MARTES')
        END
    ),
    miercoles = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'MIERCOLES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'MIERCOLES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'MIERCOLES')
        END
    ),
    jueves = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'JUEVES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'JUEVES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'JUEVES')
        END
    ),
    viernes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'VIERNES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'VIERNES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '119' AND cb_bacom_final.dia = 'VIERNES')
        END
)
WHERE clave = '119000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12101' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12101' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12101' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12101' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12101' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '121010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12102' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12102' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12102' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12102' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12102' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12103' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12103' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12103' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12103' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12103' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12104' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12104' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12104' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12104' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12104' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '121040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12105' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12105' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12105' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12105' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12105' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12106' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12106' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12106' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12106' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12106' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '121060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12107') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12107') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12107') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12107') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12107') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('121070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12108' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12108' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12108' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12108' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12108' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121251
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125101') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125101') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125101') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125101') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125101') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('121251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125102' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125102' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125102' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125102' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125102' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121252';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125103' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125103' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125103' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125103' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125103' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121253';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125104' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125104' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125104' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125104' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12125104' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '121254';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125199') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125199') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125199') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125199') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12125199') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('121259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('122') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('122') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('122') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('122') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('122') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('122000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12201' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12201' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12201' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12201' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12201' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12202') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12202') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12202') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12202') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12202') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('122020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12203' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12203' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12203' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12203' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12203' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12204' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12204' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12204' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12204' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12204' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12205' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12205' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12205' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12205' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12205' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12206') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12206') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12206') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12206') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12206') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('122060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12207' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12207' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12207' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12207' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12207' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12208' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12208' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12208' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12208' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12208' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122251
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225101') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225101') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225101') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225101') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225101') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('122251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12225102') AND cb_bacum_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12225102') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12225102') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12225102') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12225102') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('122252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225103') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225103') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225103') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225103') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225103') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('122253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225104') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225104') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225104') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225104') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12225104') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('122254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12225199' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12225199' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12225199' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12225199' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12225199' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '122259';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('123') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('123') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('123') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('123') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('123') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('123000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12301' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12301' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12301' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12301' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12301' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '123010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12302' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12302' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12302' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12302' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12302' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '123020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12303' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12303' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12303' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12303' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '12303' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '123030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12304' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12304' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12304' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12304' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12304' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '123040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12305' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12305' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12305' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12305' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12305' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '123050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12306' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12306' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12306' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12306' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12306' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '123060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12307' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12307' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12307' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12307' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12307' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '123070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12308' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12308' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12308' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12308' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12308' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '123080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123251
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325101' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE ali_f010.clave = '123251';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325102' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '123252';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta = '12325103' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325103' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '123253';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta = '12325104' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325104' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '123254';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta = '12325199' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325199' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '123259';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12401' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12401' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12401' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12401' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12401' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '124010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12402' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12402' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12402' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12402' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12402' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '124020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124251
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425101') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425101') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425101') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425101') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425101') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('124251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425102') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425102') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425102') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425102') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425102') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('124252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425103') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425103') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425103') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425103') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425103') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('124253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12425104' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12425104' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12425104' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12425104' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '12425104' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '124254';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425199') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425199') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425199') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425199') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('12425199') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('124259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '125' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '125' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '125' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '125' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '125' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '125000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12503') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12503') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12503') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12503') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12503') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('125030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12507' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12507' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12507' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12507' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '12507' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '125070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('126') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('126') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('126') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('126') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('126') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('126000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12625') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12625') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12625') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12625') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('12625') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('126250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126251
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625101') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625101') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625101') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625101') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625101') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('126251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625102') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625102') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625102') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625102') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625102') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('126252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625103') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625103') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625103') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625103') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625103') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('126253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625104') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625104') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625104') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625104') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('12625104') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('126254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12625199' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12625199' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12625199' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12625199' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12625199' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '126259';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta = '129' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '129' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '129000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12901' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '129010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12902' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '129020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) * -1 FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'LUNES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) * -1 FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'MARTES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) * -1 FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'MIERCOLES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) * -1 FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'JUEVES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) * -1 FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'VIERNES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '131' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '131000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13104' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13105' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13106' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13107' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13108' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13114M02') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13114M02') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13114M02') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13114M02') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13114M02') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('131142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131180
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13118' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '131180';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131220
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13122' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '131220';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13125' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131250';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13128' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131280';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131290
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13129' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '131290';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13131' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '131310';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13204' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '132040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13205' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '132050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13206' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '132060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13207' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '132070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13208' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '132080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13214M02') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13214M02') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13214M02') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13214M02') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13214M02') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('132142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132180
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13218' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13218' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13218' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13218' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13218' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '132180';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132220
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13222') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13222') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13222') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13222') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13222') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('132220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13225' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13225' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13225' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13225' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13225' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '132250';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13228') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13228') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13228') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13228') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13228') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('132280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132290
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13229' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13229' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13229' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13229' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13229' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '132290';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13231') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13231') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13231') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13231') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13231') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('132310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('133') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('133') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('133') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('133') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('133') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13304' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13304' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13304' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13304' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13304' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '133040';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '13305' AND cb_bacom_final.DIA = 'LUNES'), 0),
   MARTES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '13305' AND cb_bacom_final.DIA = 'MARTES'), 0),
   MIERCOLES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '13305' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
   JUEVES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '13305' AND cb_bacom_final.DIA = 'JUEVES'), 0),
   VIERNES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '13305' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '133050';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13306' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13306' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13306' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13306' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13306' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '133060';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13307') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13307') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13307') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13307') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13307') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('133070');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13308' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13308' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13308' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13308' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13308' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '133080';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13314M02') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13314M02') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13314M02') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13314M02') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13314M02') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave IN ('133142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133180
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13318' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13318' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13318' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13318' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13318' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '133180';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133220
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13322' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13322' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13322' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13322' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13322' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '133220';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13325' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13325' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13325' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13325' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13325' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '133250';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13328' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13328' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13328' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13328' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13328' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '133280';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133290
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13329') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13329') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13329') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13329') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13329') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('133290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13331') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13331') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13331') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13331') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13331') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('133310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '134' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '134' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '134' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '134' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '134' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '134000';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13404') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13404') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13404') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13404') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13404') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('134040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13405' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13405' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13405' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13405' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13405' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '134050';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13406') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13406') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13406') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13406') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13406') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('134060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13407' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13407' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13407' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13407' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13407' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '134070';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13408' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13408' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13408' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13408' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13408' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '134080';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13414M02') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13414M02') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13414M02') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13414M02') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('13414M02') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE 
    ali_f010.clave IN ('134142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134180
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13418' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13418' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13418' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13418' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13418' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '134180';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134220
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13422') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13422') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13422') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13422') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13422') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('134220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13425') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13425') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13425') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13425') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('13425') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('134250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13428') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13428') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13428') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13428') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13428') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('134280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134290
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13429' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13429' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13429' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13429' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13429' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '134290';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13431') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13431') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13431') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13431') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('13431') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('134310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13901' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13901' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13901' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13901' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13901' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '139010';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13902' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13902' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13902' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13902' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '13902' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '139020';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13903' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13903' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13903' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13903' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13903' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '139030';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13904' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13904' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13904' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13904' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13904' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '139040';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13905' AND cb_bacom_final.dia = 'LUNES'),
  martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13905' AND cb_bacom_final.dia = 'MARTES'),
  miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13905' AND cb_bacom_final.dia = 'MIERCOLES'),
  jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13905' AND cb_bacom_final.dia = 'JUEVES'),
  viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '13905' AND cb_bacom_final.dia = 'VIERNES')
WHERE ater.ali_f010.clave = '139050';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '13906' AND cb_bacom_final.dia = 'LUNES'
  ),
  martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '13906' AND cb_bacom_final.dia = 'MARTES'
  ),
  miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '13906' AND cb_bacom_final.dia = 'MIERCOLES'
  ),
  jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '13906' AND cb_bacom_final.dia = 'JUEVES'
  ),
  viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '13906' AND cb_bacom_final.dia = 'VIERNES'
  )
WHERE ali_f010.clave = '139060';'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_141000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '141000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '14101' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '14101' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '14101' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '14101' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '14101' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '141010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141011
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '141011' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '141011' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '141011' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '141011' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '141011' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '141011';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410111
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410111' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410111' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410111' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410111' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410111' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '1410111';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410112
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410112' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410112' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410112' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410112' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '1410112' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '1410112';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410113
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '1410113' AND cb_bacom_final.DIA = 'LUNES'), 0),
    MARTES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '1410113' AND cb_bacom_final.DIA = 'MARTES'), 0),
    MIERCOLES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '1410113' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
    JUEVES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '1410113' AND cb_bacom_final.DIA = 'JUEVES'), 0),
    VIERNES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '1410113' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '1410113';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141019
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141019' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141019' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141019' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141019' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '141019' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '141019';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14201' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14201' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14201' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14201' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14201' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '142010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14202' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14202' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14202' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14202' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14202' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '142020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14203' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14203' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14203' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14203' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14203' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '142030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14204' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14204' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14204' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14204' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14204' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '142040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14205' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14205' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14205' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14205' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14205' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '142050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142051
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA IN ('14205M01') AND cb_bacom_final.DIA = 'LUNES'), 0),
  MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA IN ('14205M01') AND cb_bacom_final.DIA = 'MARTES'), 0),
  MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA IN ('14205M01') AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
  JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA IN ('14205M01') AND cb_bacom_final.DIA = 'JUEVES'), 0),
  VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA IN ('14205M01') AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE IN ('142051');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14206' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14206' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14206' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14206' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '14206' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '142060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_143000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('143') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('143') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('143') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('143') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('143') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('143000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_144000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '144' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '144' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '144' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '144' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '144' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '144000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_145000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '145' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '145' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '145' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '145' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '145' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '145000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_149000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '149' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '149' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '149' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '149' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '149' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '149000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_150000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '15' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '15' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '15' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '15' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '15' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '150000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_159000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '159' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '159' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '159' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '159' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '159' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '159000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_160000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '16' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '16' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '16' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '16' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '16' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '160000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_169000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '169' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '169' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '169' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '169' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '169' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '169000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_170000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '17' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '17' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '17' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '17' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '17' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '170000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('21105', '21106') AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave IN ('211030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211031
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211051' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '211031';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211032
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('211052', '211062') AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave IN ('211032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211033
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '211053' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '211033';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '212' AND cb_bacom_final.dia = 'LUNES'
    ),
    martes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '212' AND cb_bacom_final.dia = 'MARTES'
    ),
    miercoles = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '212' AND cb_bacom_final.dia = 'MIERCOLES'
    ),
    jueves = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '212' AND cb_bacom_final.dia = 'JUEVES'
    ),
    viernes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '212' AND cb_bacom_final.dia = 'VIERNES'
    )
WHERE clave = '212000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '21205' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '21205' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '21205' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '21205' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '21205' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21206' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21206' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21206' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21206' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21206' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21207' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21207' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21207' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21207' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21207' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21208' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21208' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21208' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21208' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21208' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212120
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21212' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21212' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21212' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21212' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21212' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212120';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212130
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21213' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21213' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21213' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21213' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21213' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212130';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212140
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('21214') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('21214') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('21214') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('21214') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('21214') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('212140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21299' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21299' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21299' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21299' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21299' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '212990';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214033
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('21402203') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('21402203') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('21402203') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('21402203') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('21402203') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('214033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21501' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21501' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21501' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21501' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21501' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21502' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21502' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21502' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21502' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21502' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21503' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21503' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21503' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21503' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21503' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21504' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21504' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21504' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21504' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21504' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21505' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21505' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21505' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21505' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21505' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21506' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21506' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21506' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21506' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21506' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21507' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21507' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21507' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21507' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21507' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215071
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215071' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215071' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215071' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215071' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215071' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215071';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150711
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150711' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150711' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150711' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150711' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150711' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150711';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150712
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150712' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150712' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150712' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150712' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150712' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150712';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150713
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150713' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150713' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150713' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150713' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150713' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150713';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215072
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215072' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215072' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215072' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215072' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215072' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215072';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150721
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150721' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150721' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150721' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150721' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150721' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150721';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150722
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150722' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150722' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150722' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150722' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '2150722' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150722';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150723
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150723' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150723' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150723' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150723' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150723' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150723';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215073
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215073' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215073' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215073' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215073' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '215073' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '215073';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150731
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final 
         WHERE cb_bacom_final.cuenta = '2150731' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final 
         WHERE cb_bacom_final.cuenta = '2150731' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final 
         WHERE cb_bacom_final.cuenta = '2150731' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final 
         WHERE cb_bacom_final.cuenta = '2150731' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final 
         WHERE cb_bacom_final.cuenta = '2150731' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150731';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150732
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '2150732' AND cb_bacom_final.DIA = 'LUNES'), 0),
    MARTES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '2150732' AND cb_bacom_final.DIA = 'MARTES'), 0),
    MIERCOLES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '2150732' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
    JUEVES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '2150732' AND cb_bacom_final.DIA = 'JUEVES'), 0),
    VIERNES = COALESCE(
    (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.CUENTA = '2150732' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '2150732';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150733
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150733' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150733' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150733' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150733' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2150733' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150733';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215074
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '215074' AND cb_bacum_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '215074' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '215074' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '215074' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '215074' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '215074';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150741
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150741' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150741' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta = '2150741' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150741' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150741' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150741';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150742
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150742' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150742' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150742' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150742' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150742' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '2150742';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150743
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150743' AND cb_bacom_final.DIA = 'LUNES'), 0),
  MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150743' AND cb_bacom_final.DIA = 'MARTES'), 0),
  MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150743' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
  JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150743' AND cb_bacom_final.DIA = 'JUEVES'), 0),
  VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150743' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '2150743';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215075
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('215075') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('215075') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta IN ('215075') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('215075') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('215075') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('215075');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150752
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150752' AND cb_bacom_final.DIA = 'LUNES'), 0),
  MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150752' AND cb_bacom_final.DIA = 'MARTES'), 0),
  MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150752' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
  JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150752' AND cb_bacom_final.DIA = 'JUEVES'), 0),
  VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.CUENTA = '2150752' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '2150752';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150753
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150753' AND cb_bacom_final.DIA = 'LUNES'), 0),
  MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150753' AND cb_bacom_final.DIA = 'MARTES'), 0),
  MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150753' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
  JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150753' AND cb_bacom_final.DIA = 'JUEVES'), 0),
  VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150753' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '2150753';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215076
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '215076' AND cb_bacom_final.DIA = 'LUNES'), 0),
  MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '215076' AND cb_bacom_final.DIA = 'MARTES'), 0),
  MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.CUENTA = '215076' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
  JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '215076' AND cb_bacom_final.DIA = 'JUEVES'), 0),
  VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '215076' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE ali_f010.CLAVE = '215076';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150761
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150761' AND cb_bacom_final.DIA = 'LUNES'), 0),
  MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150761' AND cb_bacom_final.DIA = 'MARTES'), 0),
  MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150761' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
  JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150761' AND cb_bacom_final.DIA = 'JUEVES'), 0),
  VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150761' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE CLAVE = '2150761';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150762
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150762' AND cb_bacrom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150762' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150762' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150762' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2150762' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '2150762';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150763
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    LUNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150763' AND cb_bacom_final.DIA = 'LUNES'), 0),
    MARTES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150763' AND cb_bacom_final.DIA = 'MARTES'), 0),
    MIERCOLES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150763' AND cb_bacom_final.DIA = 'MIERCOLES'), 0),
    JUEVES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150763' AND cb_bacom_final.DIA = 'JUEVES'), 0),
    VIERNES = COALESCE((SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '2150763' AND cb_bacom_final.DIA = 'VIERNES'), 0)
WHERE 
    CLAVE = '2150763';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21601' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21601' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21601' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21601' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21601' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '216010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21602' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21602' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21602' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21602' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21602' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '216020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21603' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21603' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21603' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21603' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '21603' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '216030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21699' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21699' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21699' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21699' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '21699' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '216990';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_220000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '220000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '221' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '221' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '221' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '221' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '221' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '221000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22101' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22101' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22101' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22101' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '22101' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '221010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_230000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '230000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '231010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23102' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23102' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23102' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23102' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23102' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '231020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23103' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23103' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23103' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23103' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23103' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '231030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '23104' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '23104' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '23104' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '23104' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '23104' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '231040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('23106') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('23106') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('23106') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('23106') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('23106') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('231060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23199' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23199' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23199' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23199' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23199' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '231990';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('232') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('232') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('232') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('232') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta IN ('232') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('232000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232010_232020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '23201' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '23201' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '23201' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '23201' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '23201' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE 
    ali_f010.clave IN ('232010', '232020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23299' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23299' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23299' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23299' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23299' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '232990';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '23101' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '231010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24201' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24201' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24201' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24201' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24201' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '242010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24202' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24202' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24202' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24202' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24202' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '242020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24203' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24203' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24203' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24203' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24203' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '242030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24204') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24204') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24204') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24204') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24204') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('242040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24299' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24299' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24299' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24299' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24299' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '242990';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24302' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24302' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24302' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24302' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24302' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24304' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24304' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24304' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24304' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24304' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '243040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24305' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24305' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24305' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24305' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24305' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '243050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24306' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24306' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24306' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24306' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24306' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243060';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24307' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24307' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24307' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24307' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24307' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243070';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24308' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24308' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24308' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24308' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24308' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243090
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24309' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24309' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24309' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24309' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24309' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243090';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243100
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2431' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2431' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2431' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2431' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '2431' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243100';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243120
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24312' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24312' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24312' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24312' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24312' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243120';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243130
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24313' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24313' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24313' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24313' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24313' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '243130';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24399') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24399') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24399') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24399') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24399') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('243990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24501' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24501' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24501' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24501' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24501' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '245010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24502' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24502' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24502' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24502' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24502' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '245020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24503' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24503' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24503' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24503' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24503' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '245030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24504' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24504' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24504' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24504' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24504' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '245040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24599' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24599' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24599' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24599' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '24599' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '245990';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24601' AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24601' AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24601' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24601' AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final 
     WHERE cb_bacom_final.cuenta = '24601' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave = '246010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24602') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24602') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24602') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24602') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta IN ('24602') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ater.ali_f010.clave IN ('246020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24603' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24603' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24603' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24603' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '24603' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '246030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_251000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '251' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '251' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '251' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '251' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '251' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '251000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_253000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '253' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '253' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '253' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '253' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '253' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '253000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '254' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '254' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '254' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '254' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
        (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
         FROM ater.cb_bacom_final AS cb_bacom_final
         WHERE cb_bacom_final.cuenta = '254' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '254000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '25404' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '25404' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '25404' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '25404' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '25404' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '254040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254041
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('25404M01') AND cb_bacom_final.dia = 'LUNES'), 0),
  martes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('25404M01') AND cb_bacom_final.dia = 'MARTES'), 0),
  miercoles = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('25404M01') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('25404M01') AND cb_bacom_final.dia = 'JUEVES'), 0),
  viernes = COALESCE((SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('25404M01') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave IN ('254041');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_261000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '261' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '261' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '261' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '261' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '261' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '261000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261090
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26109' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '261090';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261091
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacum_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261091' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '261091';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610911
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610911' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '2610911';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610912
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610912' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '2610912';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610913
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610913' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '2610913';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261092
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '261092' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '261092';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610921
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610921' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '2610921';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610922
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610922' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '2610922';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610923
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610923' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '2610923';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610931
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610931' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '2610931';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610932
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610932' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '2610932';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610933
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'LUNES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'MARTES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'MIERCOLES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'JUEVES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'VIERNES') 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '2610933' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '2610933';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '264' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '264' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '264' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '264' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '264' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '264000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE ali_f010.clave = '264040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264041
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '26404M01' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '264041';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '265' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '265' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '265' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '265' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '265' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '265000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '26505' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '26505' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '26505' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '26505' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '26505' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '265050';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE(
    (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
     FROM ater.cb_bacom_final AS cb_bacom_final
     WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '270000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_280000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'LUNES') 
        END, 0),
    martes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'MARTES') 
        END, 0),
    miercoles = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'MIERCOLES') 
        END, 0),
    jueves = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'JUEVES') 
        END, 0),
    viernes = COALESCE(
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '28' AND cb_bacom_final.dia = 'VIERNES') 
        END, 0)
WHERE clave = '280000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_290000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'LUNES') 
    END, 0),
    martes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacum_final.cuenta = '29' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'MARTES') 
    END, 0),
    miercoles = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'MIERCOLES') 
    END, 0),
    jueves = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'JUEVES') 
    END, 0),
    viernes = COALESCE(
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '29' AND cb_bacom_final.dia = 'VIERNES') 
    END, 0)
WHERE clave = '290000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_300000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '3' AND cb_bacom_final.dia = 'LUNES'
    ),
    martes = (
        SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '3' AND cb_bacom_final.dia = 'MARTES'
    ),
    miercoles = (
        SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '3' AND cb_bacom_final.dia = 'MIERCOLES'
    ),
    jueves = (
        SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '3' AND cb_bacom_final.dia = 'JUEVES'
    ),
    viernes = (
        SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '3' AND cb_bacom_final.dia = 'VIERNES'
    )
WHERE clave = '300000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_320000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'LUNES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'LUNES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'LUNES')
    END
),
martes = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'MARTES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'MARTES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'MARTES')
    END
),
miercoles = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'MIERCOLES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'MIERCOLES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'MIERCOLES')
    END
),
jueves = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'JUEVES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'JUEVES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'JUEVES')
    END
),
viernes = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'VIERNES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'VIERNES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '32' AND cb_bacom_final.dia = 'VIERNES')
    END
)
WHERE ali_f010.clave = '320000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_400000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '4' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '400000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_411000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '411' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '411000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_412000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '412' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '412000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_413000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '413' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '413000';'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_414000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '414' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '414000';'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '415' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '415' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '415' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '415' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '415' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ater.ali_f010.clave = '415000';'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '41506' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '41506' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '41506' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '41506' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '41506' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '415060';'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_417000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'LUNES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'LUNES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'LUNES')
    END
),
martes = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'MARTES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'MARTES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'MARTES')
    END
),
miercoles = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'MIERCOLES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'MIERCOLES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'MIERCOLES')
    END
),
jueves = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'JUEVES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'JUEVES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'JUEVES')
    END
),
viernes = (
    CASE
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'VIERNES') < 0
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'VIERNES') * (-1)
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '417' AND cb_bacom_final.dia = 'VIERNES')
    END
)
WHERE ali_f010.clave = '417000';'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_419000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '419' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '419000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_421000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '421' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '421' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '421' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '421' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '421' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '421000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_439000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '439' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '439000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_441000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'LUNES') 
        END
    ),
    martes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'MARTES') 
        END
    ),
    miercoles = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'MIERCOLES') 
        END
    ),
    jueves = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'JUEVES') 
        END
    ),
    viernes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '441' AND cb_bacom_final.dia = 'VIERNES') 
        END
    )
WHERE ali_f010.clave = '441000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_470000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'LUNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'LUNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'LUNES') 
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'MARTES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'MARTES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'MARTES') 
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'MIERCOLES') 
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'JUEVES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'JUEVES') 
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'VIERNES') < 0 
        THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
        ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '47' AND cb_bacom_final.dia = 'VIERNES') 
    END
)
WHERE ali_f010.clave = '470000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_500000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'LUNES') 
        END
    ),
    martes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'MARTES') 
        END
    ),
    miercoles = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'MIERCOLES') 
        END
    ),
    jueves = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'JUEVES') 
        END
    ),
    viernes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '5' AND cb_bacom_final.dia = 'VIERNES') 
        END
    )
WHERE clave = '500000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_511000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '511' AND cb_bacom_final.dia = 'LUNES'
    ),
    martes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '511' AND cb_bacom_final.dia = 'MARTES'
    ),
    miercoles = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '511' AND cb_bacom_final.dia = 'MIERCOLES'
    ),
    jueves = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '511' AND cb_bacom_final.dia = 'JUEVES'
    ),
    viernes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '511' AND cb_bacom_final.dia = 'VIERNES'
    )
WHERE clave = '511000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = (
    SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '512' AND cb_bacom_final.dia = 'LUNES'
  ),
  martes = (
    SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '512' AND cb_bacom_final.dia = 'MARTES'
  ),
  miercoles = (
    SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '512' AND cb_bacom_final.dia = 'MIERCOLES'
  ),
  jueves = (
    SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '512' AND cb_bacom_final.dia = 'JUEVES'
  ),
  viernes = (
    SELECT CASE WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * -1 ELSE SUM(saldo_actual) END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '512' AND cb_bacom_final.dia = 'VIERNES'
  )
WHERE ali_f010.clave = '512000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        CASE 
            WHEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'LUNES') < 0 
            THEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'LUNES') * (-1) 
            ELSE (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'LUNES') 
        END
    ),
    martes = (
        CASE 
            WHEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'MARTES') < 0 
            THEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'MARTES') * (-1) 
            ELSE (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'MARTES') 
        END
    ),
    miercoles = (
        CASE 
            WHEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'MIERCOLES') < 0 
            THEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'MIERCOLES') * (-1) 
            ELSE (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'MIERCOLES') 
        END
    ),
    jueves = (
        CASE 
            WHEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'JUEVES') < 0 
            THEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'JUEVES') * (-1) 
            ELSE (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'JUEVES') 
        END
    ),
    viernes = (
        CASE 
            WHEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'VIERNES') < 0 
            THEN (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'VIERNES') * (-1) 
            ELSE (SELECT COALESCE(SUM(saldo_actual), 0) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '51204' AND cb_bacom_final.dia = 'VIERNES') 
        END
)
WHERE clave = '512040';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_513000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '513' AND cb_bacom_final.dia = 'LUNES'
    ),
    martes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '513' AND cb_bacom_final.dia = 'MARTES'
    ),
    miercoles = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '513' AND cb_bacom_final.dia = 'MIERCOLES'
    ),
    jueves = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '513' AND cb_bacom_final.dia = 'JUEVES'
    ),
    viernes = (
        SELECT CASE 
                 WHEN SUM(saldo_actual) < 0 THEN SUM(saldo_actual) * (-1) 
                 ELSE SUM(saldo_actual) 
               END 
        FROM ater.cb_bacom_final AS cb_bacom_final
        WHERE cb_bacom_final.cuenta = '513' AND cb_bacom_final.dia = 'VIERNES'
    )
WHERE clave = '513000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_514000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = (SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '514' AND cb_bacom_final.DIA = 'LUNES'),
    MARTES = (SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '514' AND cb_bacom_final.DIA = 'MARTES'),
    MIERCOLES = (SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '514' AND cb_bacom_final.DIA = 'MIERCOLES'),
    JUEVES = (SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '514' AND cb_bacom_final.DIA = 'JUEVES'),
    VIERNES = (SELECT SALDO_ACTUAL FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '514' AND cb_bacom_final.DIA = 'VIERNES')
WHERE ali_f010.CLAVE = '514000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_519000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '519' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '519' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '519' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '519' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '519' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '519000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_530000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '53' AND cb_bacom_final.DIA = 'LUNES'),
    MARTES = (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '53' AND cb_bacom_final.DIA = 'MARTES'),
    MIERCOLES = (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '53' AND cb_bacom_final.DIA = 'MIERCOLES'),
    JUEVES = (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '53' AND cb_bacom_final.DIA = 'JUEVES'),
    VIERNES = (SELECT CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * (-1) ELSE SALDO_ACTUAL END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.CUENTA = '53' AND cb_bacom_final.DIA = 'VIERNES')
WHERE ali_f010.CLAVE = '530000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_610000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61' AND cb_bacom_final.dia = 'VIERNES')
WHERE ater.ali_f010.clave = '610000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_616012
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61601102' AND cb_bacom_final.dia = 'LUNES'),
  martes = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61601102' AND cb_bacom_final.dia = 'MARTES'),
  miercoles = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61601102' AND cb_bacom_final.dia = 'MIERCOLES'),
  jueves = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61601102' AND cb_bacom_final.dia = 'JUEVES'),
  viernes = (SELECT saldo_actual FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '61601102' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '616012';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_710000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '71' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '71' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '71' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '71' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '71' AND cb_bacom_final.dia = 'VIERNES')
WHERE clave = '710000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_711000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '711' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '711' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '711' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '711' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '711' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ater.ali_f010.clave = '711000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_712000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '712' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '712' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '712' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '712' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '712' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '712000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_713000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '713' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '713' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '713' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '713' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '713' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '713000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_718000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '718' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '718' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '718' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '718' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '718' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '718000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_720000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '72' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '72' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '72' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '72' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '72' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '720000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_730000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '73' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '73' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '73' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '73' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '73' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '730000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_740000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '74' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '74' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '74' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '74' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '74' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '740000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_750000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '75' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '75' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '75' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '75' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '75' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '750000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_760000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '76' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '76' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '76' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '76' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '76' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '760000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_770000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '77' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '77' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '77' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '77' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '77' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave = '770000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_780000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '78' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '78' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '78' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '78' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '78' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '780000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_781020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('78102') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('78102') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('78102') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('78102') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('78102') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('781020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_790000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '79' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '79' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '79' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '79' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '79' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '790000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_810000_820000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '81' AND cb_bacom_final.dia = 'LUNES'
),
martes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '81' AND cb_bacom_final.dia = 'MARTES'
),
miercoles = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '81' AND cb_bacom_final.dia = 'MIERCOLES'
),
jueves = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '81' AND cb_bacom_final.dia = 'JUEVES'
),
viernes = (
    SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END
    FROM ater.cb_bacom_final AS cb_bacom_final
    WHERE cb_bacom_final.cuenta = '81' AND cb_bacom_final.dia = 'VIERNES'
)
WHERE ali_f010.clave IN ('810000', '820000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111020_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '111020'),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '111020'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '111020'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '111020'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '111020')
WHERE clave = '111020';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_112032_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031', '112032')),
  martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031', '112032')),
  miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031', '112032')),
  jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031', '112032')),
  viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031', '112032'))
WHERE clave IN ('112031', '112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031')),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('112031'))
WHERE clave IN ('112031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112032_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '112032'),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '112032'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '112032'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '112032'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '112032')
WHERE clave = '112032';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_114000_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '114000'),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '114000'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '114000'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '114000'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '114000')
WHERE clave = '114000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214031_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214031'),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214031'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214031'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214031'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214031')
WHERE clave = '214031';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214032_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214032'),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214032'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214032'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214032'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave = '214032')
WHERE clave = '214032';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243010_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243010')),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243010')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243010')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243010')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243010'))
WHERE 
    ater.ali_f010.clave IN ('243010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243110_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243110')),
    martes = (SELECT SUM(martes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243110')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243110')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243110')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f011_b AS ali_f011_b WHERE clave IN ('243110'))
WHERE ater.ali_f010.clave IN ('243110');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_113000_FANEXOC
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexoc AS ali_fanexoc WHERE ali_fanexoc.clave IN ('110000')),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexoc AS ali_fanexoc WHERE ali_fanexoc.clave IN ('110000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexoc AS ali_fanexoc WHERE ali_fanexoc.clave IN ('110000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexoc AS ali_fanexoc WHERE ali_fanexoc.clave IN ('110000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexoc AS ali_fanexoc WHERE ali_fanexoc.clave IN ('110000'))
WHERE ali_f010.clave IN ('113000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116010_FANEXOB
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave = '116010'),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave = '116010'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave = '116010'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave = '116010'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave = '116010')
WHERE ali_f010.clave = '116010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116020_FANEXOB
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave IN ('116015')),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave IN ('116015')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave IN ('116015')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave IN ('116015')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexob AS ali_fanexob WHERE ali_fanexob.clave IN ('116015'))
WHERE ali_f010.clave IN ('116020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218010_FANEXOH
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexoh AS ali_fanexoh WHERE ali_fanexoh.clave = '218000'),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexoh AS ali_fanexoh WHERE ali_fanexoh.clave = '218000'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexoh AS ali_fanexoh WHERE ali_fanexoh.clave = '218000'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexoh AS ali_fanexoh WHERE ali_fanexoh.clave = '218000'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexoh AS ali_fanexoh WHERE ali_fanexoh.clave = '218000')
WHERE ali_f010.clave = '218010';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241020_FANEXOD
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000')),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21A000'))
WHERE ali_f010.clave IN ('241020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241030_FANEXOD
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B000')),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21B000'))
WHERE ali_f010.clave IN ('241030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241040_FANEXOD
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C000')),
    martes = (SELECT SUM(martes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_fanexod AS ali_fanexod WHERE ali_fanexod.clave IN ('21C000'))
WHERE ali_f010.clave IN ('241040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121010', '121259')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121010', '121259')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121010', '121259')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121010', '121259')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121010', '121259'))
WHERE clave IN ('121000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('121251', '121252', '121253', '121254', '121259')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('121251', '121252', '121253', '121254', '121259')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('121251', '121252', '121253', '121254', '121259')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('121251', '121252', '121253', '121254', '121259')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('121251', '121252', '121253', '121254', '121259'))
WHERE clave IN ('121250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('122251', '122252', '122253', '122254', '122259')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('122251', '122252', '122253', '122254', '122259')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('122251', '122252', '122253', '122254', '122259')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('122251', '122252', '122253', '122254', '122259')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('122251', '122252', '122253', '122254', '122259'))
WHERE clave IN ('122250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132140
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('132142')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('132142')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('132142')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('132142')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('132142'))
WHERE clave IN ('132140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134140
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('134142')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('134142')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('134142')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('134142')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('134142'))
WHERE clave IN ('134140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('142010','142020','142030','142040','142050','142060')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('142010','142020','142030','142040','142050','142060')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('142010','142020','142030','142040','142050','142060')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('142010','142020','142030','142040','142050','142060')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('142010','142020','142030','142040','142050','142060'))
WHERE clave = '142000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_140000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_1 WHERE ali_f010_1.clave IN ('141000','142000','143000','144000','145000')
) - (
    SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_2 WHERE ali_f010_2.clave IN ('149000')
),
martes = (
    SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_3 WHERE ali_f010_3.clave IN ('141000','142000','143000','144000','145000')
) - (
    SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_4 WHERE ali_f010_4.clave IN ('149000')
),
miercoles = (
    SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_5 WHERE ali_f010_5.clave IN ('141000','142000','143000','144000','145000')
) - (
    SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_6 WHERE ali_f010_6.clave IN ('149000')
),
jueves = (
    SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_7 WHERE ali_f010_7.clave IN ('141000','142000','143000','144000','145000')
) - (
    SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_8 WHERE ali_f010_8.clave IN ('149000')
),
viernes = (
    SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_9 WHERE ali_f010_9.clave IN ('141000','142000','143000','144000','145000')
) - (
    SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_10 WHERE ali_f010_10.clave IN ('149000')
)
WHERE clave IN ('140000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('214031', '214032', '214033')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('214031', '214032', '214033')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('214031', '214032', '214033')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('214031', '214032', '214033')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('214031', '214032', '214033'))
WHERE clave IN ('214030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('215010','215020','215030','215040','215050','215060','215070','215071','2150711','2150712','2150713','215072','2150721','2150722','2150723','215073','2150731','2150732','2150733','215074','2150741','2150742','2150743','215075','2150751','2150752','2150753','215076','2150761','2150762','2150763','215990')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('215010','215020','215030','215040','215050','215060','215070','215071','2150711','2150712','2150713','215072','2150721','2150722','2150723','215073','2150731','2150732','2150733','215074','2150741','2150742','2150743','215075','2150751','2150752','2150753','215076','2150761','2150762','2150763','215990')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('215010','215020','215030','215040','215050','215060','215070','215071','2150711','2150712','2150713','215072','2150721','2150722','2150723','215073','2150731','2150732','2150733','215074','2150741','2150742','2150743','215075','2150751','2150752','2150753','215076','2150761','2150762','2150763','215990')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('215010','215020','215030','215040','215050','215060','215070','215071','2150711','2150712','2150713','215072','2150721','2150722','2150723','215073','2150731','2150732','2150733','215074','2150741','2150742','2150743','215075','2150751','2150752','2150753','215076','2150761','2150762','2150763','215990')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('215010','215020','215030','215040','215050','215060','215070','215071','2150711','2150712','2150713','215072','2150721','2150722','2150723','215073','2150731','2150732','2150733','215074','2150741','2150742','2150743','215075','2150751','2150752','2150753','215076','2150761','2150762','2150763','215990'))
WHERE clave = '215000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('218010')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('218010')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('218010')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('218010')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('218010'))
WHERE clave IN ('218000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('231010','231990','231020','231030','231040','231060')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('231010','231990','231020','231030','231040','231060')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('231010','231990','231020','231030','231040','231060')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('231010','231990','231020','231030','231040','231060')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('231010','231990','231020','231030','231040','231060'))
WHERE ali_f010.clave IN ('231000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('242010','242020','242030','242040','242990')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('242010','242020','242030','242040','242990')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('242010','242020','242030','242040','242990')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('242010','242020','242030','242040','242990')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('242010','242020','242030','242040','242990'))
WHERE clave IN ('242000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_244000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('244010','244990','244020','244030','244040','244050','244060')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('244010','244990','244020','244030','244040','244050','244060')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('244010','244990','244020','244030','244040','244050','244060')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('244010','244990','244020','244030','244040','244050','244060')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('244010','244990','244020','244030','244040','244050','244060'))
WHERE clave IN ('244000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('245010','245020','245030','245040','245990')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('245010','245020','245030','245040','245990')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('245010','245020','245030','245040','245990')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('245010','245020','245030','245040','245990')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('245010','245020','245030','245040','245990'))
WHERE clave IN ('245000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('246040')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('246040')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('246040')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('246040')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('246040'))
WHERE clave IN ('246000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_250000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('251000','254040','253000','254000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('251000','254040','253000','254000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('251000','254040','253000','254000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('251000','254040','253000','254000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('251000','254040','253000','254000'))
WHERE clave IN ('250000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('111010','111020','111030','111040')) +
            (SELECT COALESCE(SUM(CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END), 0) 
             FROM ater.cb_bacom_final AS cb_bacom_final 
             WHERE cb_bacom_final.cuenta = '11106' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('111010','111020','111030','111040')) +
             (SELECT COALESCE(SUM(CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END), 0) 
              FROM ater.cb_bacom_final AS cb_bacom_final 
              WHERE cb_bacom_final.cuenta = '11106' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('111010','111020','111030','111040')) +
                (SELECT COALESCE(SUM(CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END), 0) 
                 FROM ater.cb_bacom_final AS cb_bacom_final 
                 WHERE cb_bacom_final.cuenta = '11106' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('111010','111020','111030','111040')) +
             (SELECT COALESCE(SUM(CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END), 0) 
              FROM ater.cb_bacom_final AS cb_bacom_final 
              WHERE cb_bacom_final.cuenta = '11106' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('111010','111020','111030','111040')) +
              (SELECT COALESCE(SUM(CASE WHEN saldo_actual < 0 THEN saldo_actual * -1 ELSE saldo_actual END), 0) 
               FROM ater.cb_bacom_final AS cb_bacom_final 
               WHERE cb_bacom_final.cuenta = '11106' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '111000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.encaje AS encaje WHERE encaje.insumo IN ('SALDO BCV - ENCAJE M/N')), 0),
    martes = COALESCE((SELECT SUM(martes) FROM ater.encaje AS encaje WHERE encaje.insumo IN ('SALDO BCV - ENCAJE M/N')), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.encaje AS encaje WHERE encaje.insumo IN ('SALDO BCV - ENCAJE M/N')), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM ater.encaje AS encaje WHERE encaje.insumo IN ('SALDO BCV - ENCAJE M/N')), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM ater.encaje AS encaje WHERE encaje.insumo IN ('SALDO BCV - ENCAJE M/N')), 0)
WHERE ali_f010.clave IN ('112990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.encaje AS encaje WHERE encaje.descripcion IN ('ENCAJE M/N')), 0),
    martes = COALESCE((SELECT SUM(martes) FROM ater.encaje AS encaje WHERE encaje.descripcion IN ('ENCAJE M/N')), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.encaje AS encaje WHERE encaje.descripcion IN ('ENCAJE M/N')), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM ater.encaje AS encaje WHERE encaje.descripcion IN ('ENCAJE M/N')), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM ater.encaje AS encaje WHERE encaje.descripcion IN ('ENCAJE M/N')), 0)
WHERE clave IN ('112010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.CLAVE IN ('112031', '112032')),
    MARTES = (SELECT SUM(MARTES) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.CLAVE IN ('112031', '112032')),
    MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.CLAVE IN ('112031', '112032')),
    JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.CLAVE IN ('112031', '112032')),
    VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.CLAVE IN ('112031', '112032'))
WHERE ali_f010.CLAVE IN ('112030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('112010','112020','112030','112990')),
    MARTES = (SELECT SUM(MARTES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('112010','112020','112030','112990')),
    MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('112010','112020','112030','112990')),
    JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('112010','112020','112030','112990')),
    VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('112010','112020','112030','112990'))
WHERE CLAVE IN ('112000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('116010','116020','116030')) +  
           (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('11699') AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('116010','116020','116030')) +  
           (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('11699') AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('116010','116020','116030')) +  
           (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('11699') AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('116010','116020','116030')) +  
           (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('11699') AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('116010','116020','116030')) +  
           (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('11699') AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave IN ('116000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '12325' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE clave = '123250';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('124010','124020','124250','124251','124252','124253','124254','124259')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('124010','124020','124250','124251','124252','124253','124254','124259')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('124010','124020','124250','124251','124252','124253','124254','124259')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('124010','124020','124250','124251','124252','124253','124254','124259')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('124010','124020','124250','124251','124252','124253','124254','124259'))
WHERE clave IN ('124000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_120000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121000','122000','123000','124000','125000','126000')), 0),
    martes = COALESCE((SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121000','122000','123000','124000','125000','126000')), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121000','122000','123000','124000','125000','126000')), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121000','122000','123000','124000','125000','126000')), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('121000','122000','123000','124000','125000','126000')), 0)
WHERE clave IN ('120000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132000 ***
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '132' AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '132' AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '132' AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '132' AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '132' AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave = '132000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '133' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '133' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '133' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '133' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '133' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '133000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_110000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_1 WHERE ali_f010_1.clave IN ('111000','112000','113000','114000','115000','116000')) -
            (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_2 WHERE ali_f010_2.clave IN ('119000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_3 WHERE ali_f010_3.clave IN ('111000','112000','113000','114000','115000','116000')) -
             (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_4 WHERE ali_f010_4.clave IN ('119000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_5 WHERE ali_f010_5.clave IN ('111000','112000','113000','114000','115000','116000')) -
                (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_6 WHERE ali_f010_6.clave IN ('119000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_7 WHERE ali_f010_7.clave IN ('111000','112000','113000','114000','115000','116000')) -
             (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_8 WHERE ali_f010_8.clave IN ('119000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_9 WHERE ali_f010_9.clave IN ('111000','112000','113000','114000','115000','116000')) -
              (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_10 WHERE ali_f010_10.clave IN ('119000'))
WHERE clave IN ('110000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        SELECT SUM(lunes) 
        FROM ater.ali_f010 AS ali_f010_sum 
        WHERE clave IN ('139010', '139020', '139030', '139040', '139050', '139060')
    ) + COALESCE((
        SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '13907' AND cb_bacom_final.dia = 'LUNES'
    ), 0),
    martes = (
        SELECT SUM(martes) 
        FROM ater.ali_f010 AS ali_f010_sum 
        WHERE clave IN ('139010', '139020', '139030', '139040', '139050', '139060')
    ) + COALESCE((
        SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '13907' AND cb_bacom_final.dia = 'MARTES'
    ), 0),
    miercoles = (
        SELECT SUM(miercoles) 
        FROM ater.ali_f010 AS ali_f010_sum 
        WHERE clave IN ('139010', '139020', '139030', '139040', '139050', '139060')
    ) + COALESCE((
        SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '13907' AND cb_bacom_final.dia = 'MIERCOLES'
    ), 0),
    jueves = (
        SELECT SUM(jueves) 
        FROM ater.ali_f010 AS ali_f010_sum 
        WHERE clave IN ('139010', '139020', '139030', '139040', '139050', '139060')
    ) + COALESCE((
        SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '13907' AND cb_bacom_final.dia = 'JUEVES'
    ), 0),
    viernes = (
        SELECT SUM(viernes) 
        FROM ater.ali_f010 AS ali_f010_sum 
        WHERE clave IN ('139010', '139020', '139030', '139040', '139050', '139060')
    ) + COALESCE((
        SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END 
        FROM ater.cb_bacom_final AS cb_bacom_final 
        WHERE cb_bacom_final.cuenta = '13907' AND cb_bacom_final.dia = 'VIERNES'
    ), 0)
WHERE clave = '139000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_130000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('131000','132000','133000','134000')) - (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('139000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('131000','132000','133000','134000')) - (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('139000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('131000','132000','133000','134000')) - (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('139000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('131000','132000','133000','134000')) - (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('139000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('131000','132000','133000','134000')) - (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sum WHERE ali_f010_sum.clave IN ('139000'))
WHERE clave IN ('130000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_214010
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS target
SET 
  target.lunes = COALESCE((SELECT SUM(source.lunes) FROM ater.ahresgen_ins_varios AS source WHERE source.descripcion IN ('DIFERENCIA')), 0),
  target.martes = COALESCE((SELECT SUM(source.martes) FROM ater.ahresgen_ins_varios AS source WHERE source.descripcion IN ('DIFERENCIA')), 0),
  target.miercoles = COALESCE((SELECT SUM(source.miercoles) FROM ater.ahresgen_ins_varios AS source WHERE source.descripcion IN ('DIFERENCIA')), 0),
  target.jueves = COALESCE((SELECT SUM(source.jueves) FROM ater.ahresgen_ins_varios AS source WHERE source.descripcion IN ('DIFERENCIA')), 0),
  target.viernes = COALESCE((SELECT SUM(source.viernes) FROM ater.ahresgen_ins_varios AS source WHERE source.descripcion IN ('DIFERENCIA')), 0)
WHERE target.clave IN ('214010');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_217000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
  lunes = COALESCE((SELECT SUM(lunes_cc) FROM ater.tmp_f010_217) + (SELECT valor_ah FROM ater.tmp_f010_217_ah WHERE fecha = 'LUNES'), 0),
  martes = COALESCE((SELECT SUM(martes_cc) FROM ater.tmp_f010_217) + (SELECT valor_ah FROM ater.tmp_f010_217_ah WHERE fecha = 'MARTES'), 0),
  miercoles = COALESCE((SELECT SUM(miercoles_cc) FROM ater.tmp_f010_217) + (SELECT valor_ah FROM ater.tmp_f010_217_ah WHERE fecha = 'MIERCOLES'), 0),
  jueves = COALESCE((SELECT SUM(jueves_cc) FROM ater.tmp_f010_217) + (SELECT valor_ah FROM ater.tmp_f010_217_ah WHERE fecha = 'JUEVES'), 0),
  viernes = COALESCE((SELECT SUM(viernes_cc) FROM ater.tmp_f010_217) + (SELECT valor_ah FROM ater.tmp_f010_217_ah WHERE fecha = 'VIERNES'), 0)
WHERE clave IN ('217000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = COALESCE((SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('214010', '214030')), 0),
    martes = COALESCE((SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('214010', '214030')), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('214010', '214030')), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('214010', '214030')), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('214010', '214030')), 0)
WHERE 
    ali_f010.clave IN ('214000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_100000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('110000','120000','130000','140000','150000','160000','170000')) +
            (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '18' AND cb_bacom_final.dia = 'LUNES'),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('110000','120000','130000','140000','150000','160000','170000')) +
             (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '18' AND cb_bacom_final.dia = 'MARTES'),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('110000','120000','130000','140000','150000','160000','170000')) +
                (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '18' AND cb_bacom_final.dia = 'MIERCOLES'),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('110000','120000','130000','140000','150000','160000','170000')) +
             (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '18' AND cb_bacom_final.dia = 'JUEVES'),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('110000','120000','130000','140000','150000','160000','170000')) +
              (SELECT CASE WHEN saldo_actual < 0 THEN saldo_actual * (-1) ELSE saldo_actual END FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '18' AND cb_bacom_final.dia = 'VIERNES')
WHERE ali_f010.clave = '100000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM op_mesa_24108), 0),
    martes = COALESCE((SELECT SUM(martes) FROM op_mesa_24108), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM op_mesa_24108), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM op_mesa_24108), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM op_mesa_24108), 0)
WHERE clave = '241080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('241010','241020','241030','241040','241050','241060','241070','241080','241090','241990')), 0),
    martes = COALESCE((SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('241010','241020','241030','241040','241050','241060','241070','241080','241090','241990')), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('241010','241020','241030','241040','241050','241060','241070','241080','241090','241990')), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('241010','241020','241030','241040','241050','241060','241070','241080','241090','241990')), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('241010','241020','241030','241040','241050','241060','241070','241080','241090','241990')), 0)
WHERE ali_f010.clave IN ('241000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE
        WHEN '{ULT_DIA_HABIL}' = TO_CHAR(TO_DATE('{varFechaInicio}', 'DD/MM/YY'), 'DD/MM/YY') THEN
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'LUNES'
            ), 0)
        ELSE
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'LUNES'
            ), 0) +
            COALESCE((SELECT SUM(LUNES) FROM ater.ccdia_totales WHERE ID = '2C'), 0) +
            COALESCE((SELECT SUM(LUNES) FROM ater.ccdia_totales WHERE ID = '3S'), 0)
    END
),
martes = (
    CASE
        WHEN '{ULT_DIA_HABIL}' = TO_CHAR(TO_DATE('{varFechaInicio}', 'DD/MM/YY') + 1, 'DD/MM/YY') THEN
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'MARTES'
            ), 0)
        ELSE
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'MARTES'
            ), 0) +
            COALESCE((SELECT SUM(MARTES) FROM ater.ccdia_totales WHERE ID = '2C'), 0) +
            COALESCE((SELECT SUM(MARTES) FROM ater.ccdia_totales WHERE ID = '3S'), 0)
    END
),
miercoles = (
    CASE
        WHEN '{ULT_DIA_HABIL}' = TO_CHAR(TO_DATE('{varFechaInicio}', 'DD/MM/YY') + 2, 'DD/MM/YY') THEN
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'MIERCOLES'
            ), 0)
        ELSE
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'MIERCOLES'
            ), 0) +
            COALESCE((SELECT SUM(MIERCOLES) FROM ater.ccdia_totales WHERE ID = '2C'), 0) +
            COALESCE((SELECT SUM(MIERCOLES) FROM ater.ccdia_totales WHERE ID = '3S'), 0)
    END
),
jueves = (
    CASE
        WHEN '{ULT_DIA_HABIL}' = TO_CHAR(TO_DATE('{varFechaInicio}', 'DD/MM/YY') + 3, 'DD/MM/YY') THEN
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'JUEVES'
            ), 0)
        ELSE
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'JUEVES'
            ), 0) +
            COALESCE((SELECT SUM(JUEVES) FROM ater.ccdia_totales WHERE ID = '2C'), 0) +
            COALESCE((SELECT SUM(JUEVES) FROM ater.ccdia_totales WHERE ID = '3S'), 0)
    END
),
viernes = (
    CASE
        WHEN '{ULT_DIA_HABIL}' = TO_CHAR(TO_DATE('{varFechaInicio}', 'DD/MM/YY') + 4, 'DD/MM/YY') THEN
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'VIERNES'
            ), 0)
        ELSE
            COALESCE((
                SELECT SUM(CASE WHEN SALDO_ACTUAL < 0 THEN SALDO_ACTUAL * -1 ELSE SALDO_ACTUAL END)
                FROM ater.cb_bacom_final
                WHERE CUENTA = '24303' AND DIA = 'VIERNES'
            ), 0) +
            COALESCE((SELECT SUM(VIERNES) FROM ater.ccdia_totales WHERE ID = '2C'), 0) +
            COALESCE((SELECT SUM(VIERNES) FROM ater.ccdia_totales WHERE ID = '3S'), 0)
    END
)
WHERE clave = '243030';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('261','261','26109','261091','2610911','2610912','2610913','261092','2610921','2610922','2610923','261093','2610931','2610932','2610933','264','26404','26404M01','265') AND cb_bacom_final.dia = 'LUNES'), 0),
    martes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('261','261','26109','261091','2610911','2610912','2610913','261092','2610921','2610922','2610923','261093','2610931','2610932','2610933','264','26404','26404M01','265') AND cb_bacom_final.dia = 'MARTES'), 0),
    miercoles = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('261','261','26109','261091','2610911','2610912','2610913','261092','2610921','2610922','2610923','261093','2610931','2610932','2610933','264','26404','26404M01','265') AND cb_bacom_final.dia = 'MIERCOLES'), 0),
    jueves = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('261','261','26109','261091','2610911','2610912','2610913','261092','2610921','2610922','2610923','261093','2610931','2610932','2610933','264','26404','26404M01','265') AND cb_bacom_final.dia = 'JUEVES'), 0),
    viernes = COALESCE((SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta IN ('261','261','26109','261091','2610911','2610912','2610913','261092','2610921','2610922','2610923','261093','2610931','2610932','2610933','264','26404','26404M01','265') AND cb_bacom_final.dia = 'VIERNES'), 0)
WHERE ali_f010.clave IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET lunes = COALESCE((CASE WHEN (SELECT SUM(lunes) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) < 0 THEN (SELECT SUM(lunes) * (-1) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) ELSE (SELECT SUM(lunes) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) END), 0) + COALESCE((SELECT SUM(lunes) FROM ater.ccresgen_ins_varios AS cc WHERE cc.descripcion IN ('DIFERENCIA')), 0) + COALESCE((CASE WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'LUNES') < 0 THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'LUNES') ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'LUNES') END), 0),
   martes = COALESCE((CASE WHEN (SELECT SUM(martes) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) < 0 THEN (SELECT SUM(martes) * (-1) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) ELSE (SELECT SUM(martes) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) END), 0) + COALESCE((SELECT SUM(martes) FROM ater.ccresgen_ins_varios AS cc WHERE cc.descripcion IN ('DIFERENCIA')), 0) + COALESCE((CASE WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'MARTES') < 0 THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'MARTES') ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'MARTES') END), 0),
   miercoles = COALESCE((CASE WHEN (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) < 0 THEN (SELECT SUM(miercoles) * (-1) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) ELSE (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) END), 0) + COALESCE((SELECT SUM(miercoles) FROM ater.ccresgen_ins_varios AS cc WHERE cc.descripcion IN ('DIFERENCIA')), 0) + COALESCE((CASE WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'MIERCOLES') < 0 THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'MIERCOLES') ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'MIERCOLES') END), 0),
   jueves = COALESCE((CASE WHEN (SELECT SUM(jueves) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) < 0 THEN (SELECT SUM(jueves) * (-1) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) ELSE (SELECT SUM(jueves) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) END), 0) + COALESCE((SELECT SUM(jueves) FROM ater.ccresgen_ins_varios AS cc WHERE cc.descripcion IN ('DIFERENCIA')), 0) + COALESCE((CASE WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'JUEVES') < 0 THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'JUEVES') ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'JUEVES') END), 0),
   viernes = COALESCE((CASE WHEN (SELECT SUM(viernes) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) < 0 THEN (SELECT SUM(viernes) * (-1) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) ELSE (SELECT SUM(viernes) FROM ater.ali_f010 AS ali2 WHERE ali2.clave IN ('211030')) END), 0) + COALESCE((SELECT SUM(viernes) FROM ater.ccresgen_ins_varios AS cc WHERE cc.descripcion IN ('DIFERENCIA')), 0) + COALESCE((CASE WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'VIERNES') < 0 THEN (SELECT SUM(saldo_actual) * (-1) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'VIERNES') ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb WHERE cb.cuenta IN ('21104') AND cb.dia = 'VIERNES') END), 0)
WHERE ali.clave IN ('211000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_210000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('211000','212000','213000','214000','215000','216000','217000','218000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('211000','212000','213000','214000','215000','216000','217000','218000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('211000','212000','213000','214000','215000','216000','217000','218000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('211000','212000','213000','214000','215000','216000','217000','218000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('211000','212000','213000','214000','215000','216000','217000','218000'))
WHERE clave IN ('210000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('261000', '261090', '261091', '2610911', '2610912', '2610913', '261092', '2610921', '2610922', '2610923', '2610930', '2610931', '2610932', '2610933', '264000', '264040', '264041', '265000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('261000', '261090', '261091', '2610911', '2610912', '2610913', '261092', '2610921', '2610922', '2610923', '2610930', '2610931', '2610932', '2610933', '264000', '264040', '264041', '265000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('261000', '261090', '261091', '2610911', '2610912', '2610913', '261092', '2610921', '2610922', '2610923', '2610930', '2610931', '2610932', '2610933', '264000', '264040', '264041', '265000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('261000', '261090', '261091', '2610911', '2610912', '2610913', '261092', '2610921', '2610922', '2610923', '2610930', '2610931', '2610932', '2610933', '264000', '264040', '264041', '265000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('261000', '261090', '261091', '2610911', '2610912', '2610913', '261092', '2610921', '2610922', '2610923', '2610930', '2610931', '2610932', '2610933', '264000', '264040', '264041', '265000'))
WHERE clave = '260000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET 
    lunes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'LUNES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'LUNES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'LUNES')
        END
    ),
    martes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MARTES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MARTES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MARTES')
        END
    ),
    miercoles = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MIERCOLES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MIERCOLES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'MIERCOLES')
        END
    ),
    jueves = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'JUEVES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'JUEVES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'JUEVES')
        END
    ),
    viernes = (
        CASE 
            WHEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'VIERNES') < 0
            THEN (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'VIERNES') * (-1)
            ELSE (SELECT SUM(saldo_actual) FROM ater.cb_bacom_final AS cb_bacom_final WHERE cb_bacom_final.cuenta = '27' AND cb_bacom_final.dia = 'VIERNES')
        END
    )
WHERE clave = '270000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_410000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('411000','412000','413000','414000','415000','415060','417000','419000')),
    MARTES = (SELECT SUM(MARTES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('411000','412000','413000','414000','415000','415060','417000','419000')),
    MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('411000','412000','413000','414000','415000','415060','417000','419000')),
    JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('411000','412000','413000','414000','415000','415060','417000','419000')),
    VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('411000','412000','413000','414000','415000','415060','417000','419000'))
WHERE ali_f010.CLAVE IN ('410000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = (SELECT SUM(LUNES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('243010','243020','243030','243040','243050','243060','243070','243080','243090','243100','243110','243120','243130','243990')),
    MARTES = (SELECT SUM(MARTES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('243010','243020','243030','243040','243050','243060','243070','243080','243090','243100','243110','243120','243130','243990')),
    MIERCOLES = (SELECT SUM(MIERCOLES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('243010','243020','243030','243040','243050','243060','243070','243080','243090','243100','243110','243120','243130','243990')),
    JUEVES = (SELECT SUM(JUEVES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('243010','243020','243030','243040','243050','243060','243070','243080','243090','243100','243110','243120','243130','243990')),
    VIERNES = (SELECT SUM(VIERNES) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.CLAVE IN ('243010','243020','243030','243040','243050','243060','243070','243080','243090','243100','243110','243120','243130','243990'))
WHERE CLAVE IN ('243000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_240000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('241000','242000','243000','244000','245000','246000')), 0),
    martes = COALESCE((SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('241000','242000','243000','244000','245000','246000')), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('241000','242000','243000','244000','245000','246000')), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('241000','242000','243000','244000','245000','246000')), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('241000','242000','243000','244000','245000','246000')), 0)
WHERE clave IN ('240000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_200000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (
    CASE 
        WHEN (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) < 0
        THEN (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) * (-1)
        ELSE (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000'))
    END
),
martes = (
    CASE 
        WHEN (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) < 0
        THEN (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) * (-1)
        ELSE (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000'))
    END
),
miercoles = (
    CASE 
        WHEN (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) < 0
        THEN (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) * (-1)
        ELSE (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000'))
    END
),
jueves = (
    CASE 
        WHEN (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) < 0
        THEN (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) * (-1)
        ELSE (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000'))
    END
),
viernes = (
    CASE 
        WHEN (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) < 0
        THEN (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000')) * (-1)
        ELSE (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010 WHERE ali_f010.clave IN ('210000','220000','230000','240000','250000','260000','270000','280000','290000'))
    END
)
WHERE clave IN ('200000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_180000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET 
    lunes = (
        SELECT ABS(saldo_actual) 
        FROM ater.cb_bacom_final AS cb
        WHERE cb.cuenta = '18' AND cb.dia = 'LUNES'
    ) + (
        CASE 
            WHEN (
                (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '100000') - 
                (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '200000') - 
                (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '300000') + 
                (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '400000') - 
                (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '500000')
            ) < 0 THEN 
                -1 * (
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '500000')
                )
            ELSE 
                (
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(lunes) FROM ater.ali_f010 WHERE clave = '500000')
                )
        END
    ),
    martes = (
        SELECT ABS(saldo_actual) 
        FROM ater.cb_bacom_final AS cb
        WHERE cb.cuenta = '18' AND cb.dia = 'MARTES'
    ) + (
        CASE 
            WHEN (
                (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '100000') - 
                (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '200000') - 
                (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '300000') + 
                (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '400000') - 
                (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '500000')
            ) < 0 THEN 
                -1 * (
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '500000')
                )
            ELSE 
                (
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(martes) FROM ater.ali_f010 WHERE clave = '500000')
                )
        END
    ),
    miercoles = (
        SELECT ABS(saldo_actual) 
        FROM ater.cb_bacom_final AS cb
        WHERE cb.cuenta = '18' AND cb.dia = 'MIERCOLES'
    ) + (
        CASE 
            WHEN (
                (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '100000') - 
                (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '200000') - 
                (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '300000') + 
                (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '400000') - 
                (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '500000')
            ) < 0 THEN 
                -1 * (
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '500000')
                )
            ELSE 
                (
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(miercoles) FROM ater.ali_f010 WHERE clave = '500000')
                )
        END
    ),
    jueves = (
        SELECT ABS(saldo_actual) 
        FROM ater.cb_bacom_final AS cb
        WHERE cb.cuenta = '18' AND cb.dia = 'JUEVES'
    ) + (
        CASE 
            WHEN (
                (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '100000') - 
                (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '200000') - 
                (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '300000') + 
                (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '400000') - 
                (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '500000')
            ) < 0 THEN 
                -1 * (
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '500000')
                )
            ELSE 
                (
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(jueves) FROM ater.ali_f010 WHERE clave = '500000')
                )
        END
    ),
    viernes = (
        SELECT ABS(saldo_actual) 
        FROM ater.cb_bacom_final AS cb
        WHERE cb.cuenta = '18' AND cb.dia = 'VIERNES'
    ) + (
        CASE 
            WHEN (
                (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '100000') - 
                (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '200000') - 
                (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '300000') + 
                (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '400000') - 
                (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '500000')
            ) < 0 THEN 
                -1 * (
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '500000')
                )
            ELSE 
                (
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '100000') - 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '200000') - 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '300000') + 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '400000') - 
                    (SELECT ABS(viernes) FROM ater.ali_f010 WHERE clave = '500000')
                )
        END
    )
WHERE ali.clave = '180000';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_100000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('110000','120000','130000','140000','150000','160000','170000','180000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('110000','120000','130000','140000','150000','160000','170000','180000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('110000','120000','130000','140000','150000','160000','170000','180000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('110000','120000','130000','140000','150000','160000','170000','180000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 AS ali_f010_sub WHERE ali_f010_sub.clave IN ('110000','120000','130000','140000','150000','160000','170000','180000'))
WHERE clave IN ('100000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_510000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET lunes = (SELECT SUM(lunes) FROM ater.ali_f010 WHERE clave IN ('511000','512000','513000','514000','519000')),
    martes = (SELECT SUM(martes) FROM ater.ali_f010 WHERE clave IN ('511000','512000','513000','514000','519000')),
    miercoles = (SELECT SUM(miercoles) FROM ater.ali_f010 WHERE clave IN ('511000','512000','513000','514000','519000')),
    jueves = (SELECT SUM(jueves) FROM ater.ali_f010 WHERE clave IN ('511000','512000','513000','514000','519000')),
    viernes = (SELECT SUM(viernes) FROM ater.ali_f010 WHERE clave IN ('511000','512000','513000','514000','519000'))
WHERE clave IN ('510000');'''
    hook.run(sql_query_deftxt)

def ACTUALIZA_F010_MN(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # CONSTANTE_F010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = 0, martes = 0, miercoles = 0, jueves = 0, viernes = 0, total = 0, promedio = 0
WHERE clave IN ('111030', '115000', '116030', '131140', '133140', '215990', '216000', '241010', '241090', '246040', '246990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('111010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('111040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112033_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_119000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('119000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('121040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('121060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121251_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121252_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121253_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121254_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121259_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122251_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122252_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122253_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122254_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122259_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123251_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123252_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123253_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123254_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123259_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124251_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124252_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124253_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124254_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124259_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('125000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('125030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('125070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126251_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126252_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126253_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126254_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126259_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('126259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('129000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('129010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('129020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131142_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131180_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131180');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131220_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131220');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131280_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('131280');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131290_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131290');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131310_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('131310');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132142_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132180_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132180');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132220_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132220');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132280_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132280');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132290_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132290');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132310_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132310');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132310_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('133080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133142_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133180_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133180');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133220_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133220');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133280_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133280');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133290_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133290');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133310_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133310');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134142_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134180_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134180');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134220_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134220');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134280_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134280');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134290_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134290');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134310_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134310');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('141000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('141010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141011_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('141011');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410111_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('1410111');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410112_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('1410112');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410113_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('1410113');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141019_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('141019');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('142020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142051_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142051');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_143000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('143000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_144000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('144000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_145000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('145000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_149000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('149000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_150000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('150000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_159000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('159000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_160000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('160000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_169000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('169000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_170000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('170000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_180000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * 1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * 1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * 1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * 1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * 1 ELSE viernes END
WHERE clave IN ('180000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('211030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211031_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('211031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211032_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('211032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211033_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('211033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212120_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('212120');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212130_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212130');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212140_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('212990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214033_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('214033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215071_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215071');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150711_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('2150711');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150712_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150712');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150713_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150713');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215072_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('215072');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150721_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150721');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150722_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150722');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150723_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('2150723');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215073_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215073');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150731_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('2150731');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150732_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('2150732');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150733_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150733');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215074_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('215074');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150741_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('2150741');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150742_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('2150742');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150743_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150743');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215075_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215075');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150752_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2150752');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150753_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2150753');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215076_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('215076');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150761_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2150761');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150762_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('2150762');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150763_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2150763');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('216010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('216020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('216030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('216990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_220000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('220000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('221000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('221010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_230000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('230000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('231010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('231020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('231030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('231040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('231060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('231990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('232000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232010_232020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('232010', '232020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * -1 ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * -1 ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * -1 ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * -1 ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * -1 ELSE viernes END
WHERE clave IN ('232990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('231010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('242010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('242020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('242030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('242040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('242990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243070_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('243080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243090_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('24309');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243100_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243100');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243120_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('243120');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243130_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('243130');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('245010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('245020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('245030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('245040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245990_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('245990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('246010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('246020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('246030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_251000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('251000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_253000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('253000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('254000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('254040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254041_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('254041');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_261000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('261000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261090_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('261090');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261091_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('261091');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610911_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2610911');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610912_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2610912');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610913_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2610913');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261092_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('261092');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610921_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2610921');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610922_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2610922');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610923_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2610923');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610931_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2610931');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610932_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('2610932');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610933_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('2610933');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('264000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('264040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264041_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('264041');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('265000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265050_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('265050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('270000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_280000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('280000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_290000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('290000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_300000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('300000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_320000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('320000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_400000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('400000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_411000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('411000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_412000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('412000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_413000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('413000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_414000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('414000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('415000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415060_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('415060');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_417000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('417000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_419000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('419000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_421000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('421000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_439000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('439000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_441000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('441000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_470000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('470000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_500000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('500000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_511000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('511000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('512000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('512040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_513000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('513000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_514000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('514000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_519000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('519000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_530000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('530000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_610000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('610000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_616012_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('616012');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_710000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('710000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_711000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('711000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_712000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('712000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_713000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('713000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_718000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('718000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_720000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('720000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_730000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('730000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_740000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('740000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_750000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('750000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_760000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('760000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_770000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('770000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_780000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('780000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_781020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('781020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_790000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('790000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_810000_820000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('810000', '820000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('111020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_112032_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('112031', '112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112032_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_114000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('114000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214031_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('214031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214032_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('214032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243110_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('243110');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_113000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('113000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('116010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('116020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('218010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241020_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('241020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('241030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241040_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('241040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('121000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('121250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('122250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132140_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('132140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134140_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('134140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('142000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_140000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('140000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('214030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('215000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('218000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('231000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('242000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_244000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('244000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('245000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('246000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_250000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('250000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('111000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('112030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('116000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_120000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('120000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123250_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('123250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('124000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * -1 ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * -1 ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * -1 ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * -1 ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * -1 ELSE VIERNES END
WHERE CLAVE IN ('132000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_130000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('130000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_110000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('110000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_100000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('100000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('139000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_214010_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('214010');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_217000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('217000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('214000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_200000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('200000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('241000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241080_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = COALESCE((SELECT SUM(lunes) FROM op_mesa_24108), 0),
    martes = COALESCE((SELECT SUM(martes) FROM op_mesa_24108), 0),
    miercoles = COALESCE((SELECT SUM(miercoles) FROM op_mesa_24108), 0),
    jueves = COALESCE((SELECT SUM(jueves) FROM op_mesa_24108), 0),
    viernes = COALESCE((SELECT SUM(viernes) FROM op_mesa_24108), 0)
WHERE clave = '241080';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243030_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('243030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('270000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_410000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('410000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('243000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_240000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('240000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_510000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET LUNES = CASE WHEN LUNES < 0 THEN LUNES * (-1) ELSE LUNES END,
    MARTES = CASE WHEN MARTES < 0 THEN MARTES * (-1) ELSE MARTES END,
    MIERCOLES = CASE WHEN MIERCOLES < 0 THEN MIERCOLES * (-1) ELSE MIERCOLES END,
    JUEVES = CASE WHEN JUEVES < 0 THEN JUEVES * (-1) ELSE JUEVES END,
    VIERNES = CASE WHEN VIERNES < 0 THEN VIERNES * (-1) ELSE VIERNES END
WHERE CLAVE IN ('510000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211000_MN
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = CASE WHEN lunes < 0 THEN lunes * (-1) ELSE lunes END,
    martes = CASE WHEN martes < 0 THEN martes * (-1) ELSE martes END,
    miercoles = CASE WHEN miercoles < 0 THEN miercoles * (-1) ELSE miercoles END,
    jueves = CASE WHEN jueves < 0 THEN jueves * (-1) ELSE jueves END,
    viernes = CASE WHEN viernes < 0 THEN viernes * (-1) ELSE viernes END
WHERE clave IN ('211000');'''
    hook.run(sql_query_deftxt)

def ACTUALIZA_F010_FERIADOS_0(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # ACTUALIZA_F010_FERIADOS_0
    sql_query_deftxt = f'''BEGIN;

UPDATE ater.ali_f010 SET lunes = 0 WHERE EXISTS (SELECT 1 FROM ater.ater_dias_feriados AS feriados WHERE feriados.dia = 'LUNES');
UPDATE ater.ali_f010 SET martes = 0 WHERE EXISTS (SELECT 1 FROM ater.ater_dias_feriados AS feriados WHERE feriados.dia = 'MARTES');
UPDATE ater.ali_f010 SET miercoles = 0 WHERE EXISTS (SELECT 1 FROM ater.ater_dias_feriados AS feriados WHERE feriados.dia = 'MIERCOLES');
UPDATE ater.ali_f010 SET jueves = 0 WHERE EXISTS (SELECT 1 FROM ater.ater_dias_feriados AS feriados WHERE feriados.dia = 'JUEVES');
UPDATE ater.ali_f010 SET viernes = 0 WHERE EXISTS (SELECT 1 FROM ater.ater_dias_feriados AS feriados WHERE feriados.dia = 'VIERNES');

COMMIT;'''
    hook.run(sql_query_deftxt)

def ACTUALIZA_F010_TOTALES(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # CONSTANTE_F010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = 0, martes = 0, miercoles = 0, jueves = 0, viernes = 0, total = 0, promedio = 0
WHERE clave IN ('111030', '115000', '116030', '131140', '133140', '215990', '216000', '241010', '241090', '246040', '246990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('111010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('111040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('112020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112033
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('112033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_119000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('119000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('121010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('121020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('121030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('121040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('121050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('121060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('121070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('121080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121251
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE ali_f010.clave IN ('121251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('121252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('121253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('121254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('121259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('122000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('122010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('122020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('122030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('122040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('122050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('122060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('122070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('122080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('122251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('122252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122253
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('122253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122254
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('122254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('122259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('123000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('123010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('123020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('123030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('123040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('123050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('123060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('123070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('123080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE = '123251';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE = '123252';'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123253
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('123253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('123254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('123259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('124010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('124020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('124251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('124252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('124253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124254
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('124254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('124259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('125000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('125030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('125070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('126000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('126250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126251
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('126251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('126252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('126253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126254
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('126254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('126259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('129000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('129010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('129020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('131000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('131040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('131050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('131060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('131070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('131080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131142
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali_f010 SET total = (lunes + martes + miercoles + jueves + viernes) WHERE clave IN ('131142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131180
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('131180');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131220
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('131220');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('131250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('131280');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131290
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('131290');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('131310');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('132060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132180
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('132180');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132220
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132290
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132310
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('132310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132310
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('133040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('133050');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('133060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('133070');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('133080');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('133142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133180
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('133180');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133220
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('133220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('133250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('133280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133290
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('133290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('133310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('134000');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('134040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('134050');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('134060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('134070');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('134080');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134142
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('134142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134180
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('134180');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134220
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('134220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('134250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134280
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('134280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134290
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE CLAVE IN ('134290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('134310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('139010');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('139020');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE CLAVE IN ('139030');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('139040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('139050');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139060
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('139060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_141000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('141000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('141010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141011
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('141011');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410111
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('1410111');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410112
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('1410112');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410113
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('1410113');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141019
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('141019');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('142010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('142020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('142030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('142040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('142050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142051
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('142051');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('142060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_143000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('143000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_144000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('144000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_145000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('145000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_149000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('149000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_150000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('150000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_159000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('159000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_160000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('160000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_169000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('169000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_170000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('170000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_180000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('180000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('211030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211031
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('211031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211032
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('211032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211033
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('211033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('212000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('212050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('212060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('212070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('212080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212120
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('212120');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212130
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('212130');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212140
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('212140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('212990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214033
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('214033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215010
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('215010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('215020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('215030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('215040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('215050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('215060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('215070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215071
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('215071');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150711
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150711');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150712
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150712');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150713
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150713');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215072
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('215072');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150721
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150721');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150722
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150722');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150723
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150723');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215073
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('215073');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150731
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150731');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150732
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150732');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150733
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150733');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215074
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('215074');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150741
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150741');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150742
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150742');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150743
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150743');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215075
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('215075');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150752
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2150752');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150753
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('2150753');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215076
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('215076');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150761
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2150761');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150762
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('2150762');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150763
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('2150763');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('216010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216020
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('216020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('216030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('216990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_220000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('220000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('221000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221010
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('221010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_230000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('230000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('231010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('231020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('231030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('231040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('231060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('231990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('232000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232010_232020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('232010', '232020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('232990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('231010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('242010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('242020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('242030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('242040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('242990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('243040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('243060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243070
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('243080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243090
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('24309');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243100
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243100');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243120
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('243120');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243130
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243130');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('243990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('245010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('245020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245030
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('245030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('245040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245990
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('245990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('246010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('246020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('246030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_251000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('251000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_253000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('253000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('254000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('254040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254041
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('254041');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_261000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('261000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261090
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('261090');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261091
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('261091');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610911
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2610911');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610912
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2610912');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610913
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('2610913');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261092
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('261092');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610921
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2610921');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610922
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('2610922');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610923
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('2610923');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610931
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('2610931');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610932
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('2610932');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610933
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('2610933');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('264000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('264040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264041
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('264041');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('265000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('265050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('270000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_280000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('280000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_290000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('290000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_300000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('300000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_320000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('320000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_400000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('400000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_411000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('411000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_412000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('412000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_413000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('413000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_414000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('414000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('415000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415060
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('415060');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_417000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('417000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_419000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('419000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_421000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('421000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_439000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('439000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_441000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('441000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_470000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('470000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_500000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('500000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_511000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('511000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('512000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('512040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_513000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('513000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_514000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('514000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_519000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('519000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_530000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('530000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_610000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('610000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_616012
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('616012');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_710000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('710000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_711000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('711000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_712000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('712000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_713000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('713000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_718000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('718000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_720000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('720000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_730000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('730000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_740000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('740000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_750000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('750000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_760000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('760000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_770000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('770000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_780000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('780000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_781020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('781020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_790000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('790000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_810000_820000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('810000', '820000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111020_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('111020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_112032_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('112031', '112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('112031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112032_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_114000_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('114000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214031_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('214031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214032_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali_f010 SET total = (lunes + martes + miercoles + jueves + viernes) WHERE ali_f010.clave IN ('214032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243010_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243110_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243110');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_113000_FANEXOC
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('113000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116010_FANEXOB
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('116010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116020_FANEXOB
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('116020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218010_FANEXOH
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('218010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241020_FANEXOD
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('241020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241030_FANEXOD
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('241030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241040_FANEXOD
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('241040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('121000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('121250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122250
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('122250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132140
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('132140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134140
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('134140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('142000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_140000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('140000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214030
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('214030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('215000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('218000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('231000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('242000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_244000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('244000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('245000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('246000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_250000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('250000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('111000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('112990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('112010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('112030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('112000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('116000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('123250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('124000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_120000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('120000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('132000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_110000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET total = (ali.lunes + ali.martes + ali.miercoles + ali.jueves + ali.viernes)
WHERE ali.clave IN ('110000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_100000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('100000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('139000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_130000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('130000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_214010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('214010');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_217000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('217000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('214000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_210000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('210000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_200000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('200000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('241000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('241080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243030
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES) WHERE ali_f010.CLAVE IN ('270000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_410000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (ali_f010.LUNES + ali_f010.MARTES + ali_f010.MIERCOLES + ali_f010.JUEVES + ali_f010.VIERNES)
WHERE ali_f010.CLAVE IN ('410000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('243000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_240000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('240000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_510000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET total = (lunes + martes + miercoles + jueves + viernes)
WHERE clave IN ('510000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET TOTAL = (LUNES + MARTES + MIERCOLES + JUEVES + VIERNES)
WHERE ali_f010.CLAVE IN ('211000');'''
    hook.run(sql_query_deftxt)

def ACTUALIZA_F010_PROMEDIO(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    varCantDiasHabiles = Variable.get('varCantDiasHabiles')

    # CONSTANTE_F010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET lunes = 0, martes = 0, miercoles = 0, jueves = 0, viernes = 0, total = 0, promedio = 0
WHERE clave IN ('111030', '115000', '116030', '131140', '133140', '215990', '216000', '241010', '241090', '246040', '246990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('111010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('111040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112033
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_119000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('119000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('121020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('121040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('121060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('121070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('121080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121253
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('121254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122253
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('122253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122254
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('122254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123080
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('123080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123252
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('123252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123253
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('123253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123254
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123259
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('123259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('124010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124253
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124254
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('125000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('125030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_125070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('125070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('126000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('126250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126251
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('126251');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126252
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('126252');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126253
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('126253');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126254
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('126254');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_126259
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('126259');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('129000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('129010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_129020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('129020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('131000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131142
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131142');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131180
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('131180');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131220
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('131220');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131280
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('131280');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131290
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('131290');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_131310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('131310');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('132040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132050
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('132050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('132060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132142
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132180
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132180');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132220
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132280
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132290
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132310
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('132310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_132310
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133050');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133070');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133080');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133142
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133180
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133180');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133220
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133280
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('133280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133290
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_133310
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('133310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134000');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('134040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134050');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('134060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134070');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('134080');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134142
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134142');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134180
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134180');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134220
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134220');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134250');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134280
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134280');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134290
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134290');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_134310
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134310');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('139010');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('139020');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('139030');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('139040');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('139050');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_139060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('139060');'''
    hook.run(sql_query_deftxt)

    # FORMULARIO_F010_141000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('141000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('141010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141011
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('141011');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410111
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('1410111');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410112
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE CLAVE IN ('1410112');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_1410113
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('1410113');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_141019
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('141019');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('142010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('142020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('142030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('142040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('142050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142051
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('142051');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('142060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_143000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('143000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_144000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('144000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_145000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('145000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_149000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('149000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_150000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('150000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_159000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('159000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_160000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('160000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_169000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('169000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_170000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('170000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_180000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('180000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('211030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211031
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('211031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211032
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('211032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211033
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('211033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('212000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('212050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('212060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('212070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('212080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212120
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('212120');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212130
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('212130');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212140
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('212140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_212990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('212990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214033
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('214033');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('215030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215071
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215071');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150711
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150711');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150712
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150712');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150713
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150713');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215072
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215072');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150721
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150721');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150722
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('2150722');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150723
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150723');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215073
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215073');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150731
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150731');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150732
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('2150732');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150733
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150733');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215074
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215074');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150741
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150741');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150742
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('2150742');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150743
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150743');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215075
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('215075');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150752
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150752');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150753
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150753');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215076
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('215076');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150761
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150761');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150762
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150762');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2150763
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2150763');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('216010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('216020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('216030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_216990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('216990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_220000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('220000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('221000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_221010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('221010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_230000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('230000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('231010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('231020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('231030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('231040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('231060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('231990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('232000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232010_232020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('232010', '232020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_232990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('232990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('231010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('242010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('242020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('242030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('242040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('242990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243060');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243070
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243070');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243090
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('24309');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243100
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243100');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243120
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243120');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243130
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243130');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245010
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('245010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245020
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('245020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('245030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('245040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('245990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('246010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('246020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('246030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_251000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('251000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_253000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('253000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('254000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('254040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_254041
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('254041');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_261000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('261000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261090
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('261090');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261091
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('261091');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610911
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610911');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610912
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610912');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610913
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('2610913');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_261092
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('261092');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610921
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610921');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610922
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('2610922');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610923
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610923');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610931
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610931');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610932
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610932');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_2610933
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('2610933');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('264000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264040
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('264040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_264041
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('264041');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('265000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_265050
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('265050');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('270000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_280000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('280000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_290000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('290000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_300000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('300000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_320000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('320000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_400000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('400000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_411000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('411000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_412000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('412000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_413000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('413000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_414000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('414000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('415000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_415060
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('415060');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_417000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('417000');'''
    hook.run(sql_query_deftxt)

    # FOMRULA_F010_419000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('419000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_421000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('421000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_439000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('439000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_441000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('441000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_470000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('470000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_500000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('500000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_511000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('511000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('512000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_512040
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('512040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_513000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('513000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_514000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('514000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_519000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('519000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_530000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('530000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_610000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('610000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_616012
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('616012');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_710000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('710000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_711000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('711000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_712000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('712000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_713000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('713000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_718000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('718000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_720000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('720000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_730000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('730000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_740000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('740000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_750000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('750000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_760000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('760000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_770000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('770000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_780000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('780000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_781020
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010 SET PROMEDIO = (TOTAL) / {varCantDiasHabiles} WHERE ali_f010.CLAVE IN ('781020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_790000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('790000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_810000_820000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('810000', '820000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111020_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('111020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_112032_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112031', '112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112031_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('112031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112032_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('112032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_114000_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('114000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214031_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('214031');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214032_F011$B
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('214032');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243010_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('243010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243110_F011$B
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('243110');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_113000_FANEXOC
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('113000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116010_FANEXOB
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('116010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116020_FANEXOB
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('116020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218010_FANEXOH
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('218010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241020_FANEXOD
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('241020');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241030_FANEXOD
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('241030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241040_FANEXOD
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('241040');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('121000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_121250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('121250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_122250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('122250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132140
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_134140
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('134140');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_142000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('142000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_140000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('140000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('214030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_215000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('215000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_218000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('218000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_231000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('231000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_242000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = (total) / {varCantDiasHabiles}
WHERE ali.clave IN ('242000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_244000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('244000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_245000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('245000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_246000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('246000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_250000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('250000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_111000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('111000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112990
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112990');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112010');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_112000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('112000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_116000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('116000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_123250
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('123250');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_124000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('124000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_120000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('120000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_132000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('132000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_133000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('133000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_110000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('110000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_100000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('100000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_139000
    sql_query_deftxt = f'''UPDATE ater.ali_f010 AS ali
SET promedio = total / {varCantDiasHabiles}
WHERE ali.clave IN ('139000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_130000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('130000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_214010
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('214010');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_217000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('217000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_214000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('214000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_210000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('210000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_200000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('200000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = total / {varCantDiasHabiles}
WHERE clave IN ('241000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_241080
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('241080');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243030
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243030');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_260000
    sql_query_deftxt = f'''UPDATE ater.ali_f010
SET promedio = (total) / {varCantDiasHabiles}
WHERE clave IN ('260000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_270000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('270000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_410000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('410000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_243000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('243000');'''
    hook.run(sql_query_deftxt)

    # FROMULA_F010_240000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('240000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_510000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali.CLAVE IN ('510000');'''
    hook.run(sql_query_deftxt)

    # FORMULA_F010_211000
    sql_query_deftxt = f'''UPDATE ATER.ALI_F010 AS ali_f010
SET PROMEDIO = (TOTAL) / {varCantDiasHabiles}
WHERE ali_f010.CLAVE IN ('211000');'''
    hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='F010',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IMPORTAR_INSUMO_task = PythonOperator(
    task_id='IMPORTAR_INSUMO_task',
    python_callable=IMPORTAR_INSUMO,
    dag=dag
)

ENCAJE_task = PythonOperator(
    task_id='ENCAJE_task',
    python_callable=ENCAJE,
    provide_context=True,
    dag=dag
)

ENCAJE_TOTAL_task = PythonOperator(
    task_id='ENCAJE_TOTAL_task',
    python_callable=ENCAJE_TOTAL,
    provide_context=True,
    dag=dag
)

ALI_F010_task = PythonOperator(
    task_id='ALI_F010_task',
    python_callable=ALI_F010,
    provide_context=True,
    dag=dag
)

ACTUALIZA_F010_task = PythonOperator(
    task_id='ACTUALIZA_F010_task',
    python_callable=ACTUALIZA_F010,
    provide_context=True,
    dag=dag
)

ACTUALIZA_F010_MN_task = PythonOperator(
    task_id='ACTUALIZA_F010_MN_task',
    python_callable=ACTUALIZA_F010_MN,
    provide_context=True,
    dag=dag
)

ACTUALIZA_F010_FERIADOS_0_task = PythonOperator(
    task_id='ACTUALIZA_F010_FERIADOS_0_task',
    python_callable=ACTUALIZA_F010_FERIADOS_0,
    provide_context=True,
    dag=dag
)

ACTUALIZA_F010_TOTALES_task = PythonOperator(
    task_id='ACTUALIZA_F010_TOTALES_task',
    python_callable=ACTUALIZA_F010_TOTALES,
    provide_context=True,
    dag=dag
)

ACTUALIZA_F010_PROMEDIO_task = PythonOperator(
    task_id='ACTUALIZA_F010_PROMEDIO_task',
    python_callable=ACTUALIZA_F010_PROMEDIO,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IMPORTAR_INSUMO_task >> ENCAJE_task >> ENCAJE_TOTAL_task >> ALI_F010_task >> ACTUALIZA_F010_task >> ACTUALIZA_F010_MN_task >> ACTUALIZA_F010_FERIADOS_0_task >> ACTUALIZA_F010_TOTALES_task >> ACTUALIZA_F010_PROMEDIO_task
