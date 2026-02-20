from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
import time
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import json
import os
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator


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

def AT26_ATSUDEBAN_TOFILE(**kwargs):
	
	# Conexion a la bd at26
	hook = PostgresHook(postgres_conn_id='repodataprd')
	
	sql_query_deftxt = '''
	CREATE TABLE IF NOT EXISTS AT_STG.ATS_TH_AT26 (
		NROFRAUDE              VARCHAR(20),
		CAUSAFRAUDE            VARCHAR(20),
		TIPOINSTRUMENTO        VARCHAR(2),
		CANALFRAUDE            VARCHAR(255),
		TIPOFRAUDE             VARCHAR(2),
		TIPOFRANQUICIA         VARCHAR(255),
		MONTOFRAUDE            VARCHAR(15),
		AFECTACIONMONETARIA    VARCHAR(2),
		CODIGOCONTABLE         VARCHAR(20),
		TIPOPERSONA            VARCHAR(50),
		IDCLIENTE              VARCHAR(50),
		GENERO                 VARCHAR(50),
		RANGOEDAD              VARCHAR(50),
		PENSIONADOIVSS         VARCHAR(50),
		NRORECLAMO             VARCHAR(20),
		FECHAFRAUDE            VARCHAR(8),
		CODIGOPARROQUIA        VARCHAR(8),
		NOMBRECLIENTE          VARCHAR(255),
		TIPOOPERACION          VARCHAR(50),
		MONTOFRAUDEINTERNO     VARCHAR(15),
		MONTOFRAUDEEXTERNO     VARCHAR(15),
		RED                    VARCHAR(50)
	); '''
	
	hook.run(sql_query_deftxt)
	
	# vaciar la tabla antes de cargar
	sql_query_deftxt = '''TRUNCATE TABLE AT_STG.ATS_TH_AT26;'''
	hook.run(sql_query_deftxt)

	
	# Insertar los registros en la tabla de destino
	sql_query_deftxt = '''
	INSERT INTO AT_STG.ATS_TH_AT26 (
		NROFRAUDE,
		CAUSAFRAUDE,
		TIPOINSTRUMENTO,
		CANALFRAUDE,
		TIPOFRAUDE,
		TIPOFRANQUICIA,
		MONTOFRAUDE,
		AFECTACIONMONETARIA,
		CODIGOCONTABLE,
		TIPOPERSONA,
		IDCLIENTE,
		GENERO,
		RANGOEDAD,
		PENSIONADOIVSS,
		NRORECLAMO,
		FECHAFRAUDE,
		CODIGOPARROQUIA,
		NOMBRECLIENTE,
		TIPOOPERACION,
		MONTOFRAUDEINTERNO,
		MONTOFRAUDEEXTERNO,
		RED
	) 
	SELECT
		SUBSTRING(NROFRAUDE FROM 1 FOR 20) AS NROFRAUDE,
		CAUSAFRAUDE,
		TIPOINSTRUMENTO,
		CANALFRAUDE,
		TIPOFRAUDE,
		TIPOFRANQUICIA,
		CASE 
			WHEN MONTOFRAUDE = 0 THEN '0,00'
			ELSE REPLACE(TO_CHAR(MONTOFRAUDE, 'FM9999999999999.00'), '.', ',')
		END AS MONTOFRAUDE,
		AFECTACIONMONETARIA,
		CODIGOCONTABLE,
		TIPOPERSONA,
		SUBSTRING(IDCLIENTE FROM 1 FOR 19) AS IDCLIENTE,
		GENERO,
		RANGOEDAD,
		PENSIONADOIVSS,
		NRORECLAMO,
		FECHAFRAUDE,
		LPAD(CODIGOPARROQUIA, 6, '0') AS CODIGOPARROQUIA,
		SUBSTRING(NOMBRECLIENTE FROM 1 FOR 100) AS NOMBRECLIENTE,
		TIPOOPERACION,
		CASE 
			WHEN MONTOFRAUDEINTERNO = 0 THEN '0,00'
			ELSE REPLACE(TO_CHAR(MONTOFRAUDEINTERNO, 'FM9999999999999.00'), '.', ',')
		END AS MONTOFRAUDEINTERNO,
		CASE 
			WHEN MONTOFRAUDEEXTERNO = 0 THEN '0,00'
			ELSE REPLACE(TO_CHAR(MONTOFRAUDEEXTERNO, 'FM9999999999999.00'), '.', ',')
		END AS MONTOFRAUDEEXTERNO,
		RED
	FROM ATSUDEBAN.AT26_TH_BC
	ORDER BY NROFRAUDE, TIPOINSTRUMENTO, CANALFRAUDE asc; '''

	hook.run(sql_query_deftxt)

def ATS_TH_AT26_TOTXT(**kwargs):
    # Conexion a la bd repodataprd
    hook = PostgresHook(postgres_conn_id='repodataprd')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook

    # Recuperar las variables definidas en las tareas previas
    FileAT = get_variable('FileAT_at26')
    FileCodSupervisado = get_variable('FileCodSupervisado')
    FechaFile = get_variable('FechaFile')

    # Generar txt desde la base de datos
    logger.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records("SELECT * FROM AT_STG.ATS_TH_AT26 ORDER BY NROFRAUDE, TIPOINSTRUMENTO, CANALFRAUDE ASC;")
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Definir la ruta del archivo de salida en GCS
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT26/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFile}.txt"
    
    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal
    local_file_path = os.path.join(temp_dir, f"{FileAT}{FileCodSupervisado}{FechaFile}.txt") # Ruta del archivo temporal local

    try:
        logger.info(f"Escribiendo datos a archivo temporal local: {local_file_path}")
        # Escribir los registros en el archivo de texto temporal local
        with open(local_file_path, 'w', encoding='utf-8') as f:
            for row in registros:
                # Convertimos cada fila (tupla) a una cadena separada por tildes y aseguramos que los valores None se traten como cadenas vaci­as
                linea = "~".join(str(valor) if valor is not None else "" for valor in row)
                f.write(linea + "\n")
        
        logger.info(f"Archivo temporal local generado correctamente. Subiendo a GCS: gs://{gcs_bucket}/{gcs_object_path}")
        
        # Subir el archivo temporal local a GCS
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=gcs_object_path,
            filename=local_file_path,
            #mime_type='text/plain' # Opcional: especificar el tipo MIME
        )
        logger.info(f"Archivo generado y subido a GCS: gs://{gcs_bucket}/{gcs_object_path}")

    except Exception as e:
        logger.error(f"Error durante la generacion o subida del archivo: {str(e)}")
        import traceback
        logger.error("Traceback completo:\n" + traceback.format_exc())
        raise

    finally:
        # Limpieza: Asegurarse de eliminar el archivo temporal y el directorio
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            logger.info(f"Archivo temporal eliminado: {local_file_path}")
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
            logger.info(f"Directorio temporal eliminado: {temp_dir}")


###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT26_TO_FILE', default_args=default_args, schedule=None, catchup=False)


AT26_ATSUDEBAN_TOFILE_task = PythonOperator(
	task_id='AT26_ATSUDEBAN_TOFILE_task',
	python_callable=AT26_ATSUDEBAN_TOFILE,
	dag=dag
)

ATS_TH_AT26_TOTXT_task = PythonOperator(
	task_id='ATS_TH_AT26_TOTXT_task',
	python_callable=ATS_TH_AT26_TOTXT,
	dag=dag
)

def enviar_correo_task_callable(**kwargs):
    """Envía email usando send_email() - Convertido desde EmailOperator"""
    from airflow.utils.email import send_email
    send_email(
        to="daniel.figueroa@kreadata.com",
        subject="AT26 - ARCHIVOS DE TRANSMISION (SIIF) DWH EN LA NUBE GCP (Airflow) - GALIPAN TECNOLOGICO",
        html_content="""

        <h3>¡Buen dia!</h3>
        <p>Se ha generado de forma automatica el correspondiente.<b>'AT26'</b> El mismo se encuentra en la ruta 'data/AT26/SALIDAS' del bucket. El nombre del reporte es: {{ var.value.FileAT_at26 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>
    
        """
    )

enviar_correo_task = PythonOperator(
    task_id="enviar_correo",
    python_callable=enviar_correo_task_callable,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT26_ATSUDEBAN_TOFILE_task >> ATS_TH_AT26_TOTXT_task >> enviar_correo_task