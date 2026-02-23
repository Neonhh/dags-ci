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
from io import StringIO
import time
import os
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook



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

def FileName(**kwargs):
    value = '#ODI_AT_SUDEBAN_DEV.FileName'
    Variable.set('FileName', serialize_value(value))

def AT10_ATSUDEBAN_TOFILE(**kwargs):
    #Inicializa los hooks
    hook = PostgresHook(postgres_conn_id='repodataprd')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    # Recuperar las variables definidas en las tareas previas
    FileAT = get_variable("FileAT", default_var="AT10")
    FileCodSupervisado = get_variable('FileCodSupervisado')
    FechaFile = get_variable('FechaFile')

    # Generar txt desde la base de datos
    logger.info("Obteniendo registros de la base de datos...")
    query_string = '''
        SELECT 
            OFICINA, CODINSTRUMENTO, TIPOSISTEMA, CODCTACONTABLE, IDINSTRUMENTO, CODEMISOR, PAISEMISOR, CODCUSTODIO, PAISCUSTODIO, EMPRESARIESGOCUSTODIO,
            CALIFRIESGOCUSTODIO, MONEDA, FECHAEMISION, FECHAADQUISICION, FECHAVCTO, ULTFECHATRANS, NROTRANSFERENCIAS,
            CASE WHEN VALORNOMINAL = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(VALORNOMINAL, 'FM99999999999990D00000000'),'.',',') END AS VALORNOMINAL,
            CASE WHEN COSTOADQUISICION = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(COSTOADQUISICION, 'FM99999999999990D00000000'),'.',',') END AS COSTOADQUISICION,
            CASE WHEN VALORMERCADOTRANS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORMERCADOTRANS, 'FM99999999999990D00'),'.',',') END AS VALORMERCADOTRANS,
            CASE WHEN PORCENTAJECOMPRA = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(PORCENTAJECOMPRA, 'FM99999999999990D00000000'),'.',',') END AS PORCENTAJECOMPRA,
            CASE WHEN PORCENTAJEVALORMERCADO = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(PORCENTAJEVALORMERCADO, 'FM99999999999990D00000000'),'.',',') END AS PORCENTAJEVALORMERCADO,
            CASE WHEN VALORMERCADO = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(VALORMERCADO, 'FM99999999999990D00000000'),'.',',') END AS VALORMERCADO,
            CASE WHEN VALORPRESENTE = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(VALORPRESENTE, 'FM99999999999990D00000000'),'.',',') END AS VALORPRESENTE,
            CASE WHEN TIPOCAMBIOCOMPRA = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(TIPOCAMBIOCOMPRA, 'FM99999999999990D00000000'),'.',',') END AS TIPOCAMBIOCOMPRA,
            CASE WHEN TIPOCAMBIOCIERRE = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(TIPOCAMBIOCIERRE, 'FM99999999999990D00000000'),'.',',') END AS TIPOCAMBIOCIERRE,
            CASE WHEN GANOPERDIFCAMBIARIO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(GANOPERDIFCAMBIARIO, 'FM99999999999990D00'),'.',',') END AS GANOPERDIFCAMBIARIO,
            CASE WHEN GANOPERDNOREALIZADA = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(GANOPERDNOREALIZADA, 'FM99999999999990D00000000'),'.',',') END AS GANOPERDNOREALIZADA,
            CASE WHEN PRIMAODESCUENTO = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(PRIMAODESCUENTO, 'FM99999999999990D00000000'),'.',',') END AS PRIMAODESCUENTO,
            CASE WHEN COSTOAMORTIZADO = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(COSTOAMORTIZADO, 'FM99999999999990D00000000'),'.',',') END AS COSTOAMORTIZADO,
            CASE WHEN VALORENLIBROS = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(VALORENLIBROS, 'FM99999999999990D00000000'),'.',',') END AS VALORENLIBROS,
            CASE WHEN TASADESCTO = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASADESCTO, 'FM99999999999990D0000'),'.',',') END AS TASADESCTO,
            NROACCIONES,
            CASE WHEN PRECIOMERCADOACCIONES = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(PRECIOMERCADOACCIONES, 'FM99999999999990D00'),'.',',') END AS PRECIOMERCADOACCIONES,
            CASE WHEN PARTCPATRIMONIAL = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(PARTCPATRIMONIAL, 'FM99999999999990D0000'),'.',',') END AS PARTCPATRIMONIAL,
            CASE WHEN MONTOPROVISION = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(MONTOPROVISION, 'FM99999999999990D00'),'.',',') END AS MONTOPROVISION,
            NRODECRETO, NROEMISION, SERIEINSTR,
            CASE WHEN TASAINTCUPON = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASAINTCUPON, 'FM99999999999990D0000'),'.',',') END AS TASAINTCUPON,
            CASE WHEN TASAINTERESES = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASAINTERESES, 'FM99999999999990D0000'),'.',',') END AS TASAINTERESES,
            FECHAINICIOCP, FECHAVTOCP,
            CASE WHEN INTERESESCOBRADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(INTERESESCOBRADOS, 'FM99999999999990D00'),'.',',') END AS INTERESESCOBRADOS,
            CASE WHEN INTERESESDEVENGADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(INTERESESDEVENGADOS, 'FM99999999999990D00'),'.',',') END AS INTERESESDEVENGADOS,
            CASE WHEN DIVIDENDOSCOBRADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(DIVIDENDOSCOBRADOS, 'FM99999999999990D00'),'.',',') END AS DIVIDENDOSCOBRADOS,
            CASE WHEN RENDPORCOBRAR = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(RENDPORCOBRAR, 'FM99999999999990D00'),'.',',') END AS RENDPORCOBRAR,
            PERIOPAGOINT, FECHACOBRO,
            CASE WHEN DIVDECRNOCOBRADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(DIVDECRNOCOBRADOS, 'FM99999999999990D00'),'.',',') END AS DIVDECRNOCOBRADOS,
            CTACBLEORITRANS,
            CASE WHEN MONTOCANJE = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(MONTOCANJE, 'FM99999999999990D00'),'.',',') END AS MONTOCANJE,
            FECHACANJE, CONTRAPARTECANJE, CODINSTRENTREGADO,
            CASE WHEN PATRIEMPRESA = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(PATRIEMPRESA, 'FM99999999999990D00'),'.',',') END AS PATRIEMPRESA,
            FECHAEDOFINANCIERO,
            CASE WHEN MONTOCAPSOCIAL = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(MONTOCAPSOCIAL, 'FM99999999999990D00'),'.',',') END AS MONTOCAPSOCIAL,
            CAST(NROACCIONESCIRC AS INT) AS NROACCIONESCIRC,
            CANTIDADACCIONES AS CANTIDADACCIONES,
            ACTIVOSUBYACENTE AS ACTIVOSUBYACENTE,
            CASE WHEN VALORNOMACTIVO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORNOMACTIVO, 'FM99999999999990D00'),'.',',') END AS VALORNOMACTIVO,
            MONEDAACTIVO,
            CASE WHEN TIPOCAMCIERRESUB = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TIPOCAMCIERRESUB, 'FM99999999999990D0000'),'.',',') END AS TIPOCAMCIERRESUB,
            CODEMISORSUB, CODCUSTODIOSUB,
            CASE WHEN TASACUPONSUB = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASACUPONSUB, 'FM99999999999990D0000'),'.',',') END AS TASACUPONSUB,
            FECHAVCTOSUB,
            CASE WHEN RESULTADONETO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(RESULTADONETO, 'FM99999999999990D00'),'.',',') END AS RESULTADONETO,
            CASE WHEN GANOPERPARTPATRIM = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(GANOPERPARTPATRIM, 'FM99999999999990D00'),'.',',') END AS GANOPERPARTPATRIM,
            BASECALCULO
        FROM ATSUDEBAN.ATS_TH_AT10
        WHERE (PORCION IN ('LA'))
        UNION ALL
        SELECT 
            OFICINA, CODINSTRUMENTO, TIPOSISTEMA, CODCTACONTABLE, IDINSTRUMENTO, CODEMISOR, PAISEMISOR, CODCUSTODIO, PAISCUSTODIO, EMPRESARIESGOCUSTODIO,
            CALIFRIESGOCUSTODIO, MONEDA, FECHAEMISION, FECHAADQUISICION, FECHAVCTO, ULTFECHATRANS, NROTRANSFERENCIAS,
            CASE WHEN VALORNOMINAL = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORNOMINAL, 'FM99999999999990D00'),'.',',') END AS VALORNOMINAL,
            CASE WHEN COSTOADQUISICION = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(COSTOADQUISICION, 'FM99999999999990D0000'),'.',',') END AS COSTOADQUISICION,
            CASE WHEN VALORMERCADOTRANS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORMERCADOTRANS, 'FM99999999999990D00'),'.',',') END AS VALORMERCADOTRANS,
            CASE WHEN PORCENTAJECOMPRA = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(PORCENTAJECOMPRA, 'FM99999999999990D00000000'),'.',',') END AS PORCENTAJECOMPRA,
            CASE WHEN PORCENTAJEVALORMERCADO = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(PORCENTAJEVALORMERCADO, 'FM99999999999990D0000'),'.',',') END AS PORCENTAJEVALORMERCADO,
            CASE WHEN VALORMERCADO = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(VALORMERCADO, 'FM99999999999990D00000000'),'.',',') END AS VALORMERCADO,
            CASE WHEN VALORPRESENTE = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORPRESENTE, 'FM99999999999990D00'),'.',',') END AS VALORPRESENTE,
            CASE WHEN TIPOCAMBIOCOMPRA = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(TIPOCAMBIOCOMPRA, 'FM99999999999990D00000000'),'.',',') END AS TIPOCAMBIOCOMPRA,
            CASE WHEN TIPOCAMBIOCIERRE = 0 THEN '0,00000000' ELSE REPLACE(TO_CHAR(TIPOCAMBIOCIERRE, 'FM99999999999990D00000000'),'.',',') END AS TIPOCAMBIOCIERRE,
            CASE WHEN GANOPERDIFCAMBIARIO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(GANOPERDIFCAMBIARIO, 'FM99999999999990D00'),'.',',') END AS GANOPERDIFCAMBIARIO,
            CASE WHEN GANOPERDNOREALIZADA = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(GANOPERDNOREALIZADA, 'FM99999999999990D00'),'.',',') END AS GANOPERDNOREALIZADA,
            CASE WHEN PRIMAODESCUENTO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(PRIMAODESCUENTO, 'FM99999999999990D00'),'.',',') END AS PRIMAODESCUENTO,
            CASE WHEN COSTOAMORTIZADO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(COSTOAMORTIZADO, 'FM99999999999990D00'),'.',',') END AS COSTOAMORTIZADO,
            CASE WHEN VALORENLIBROS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORENLIBROS, 'FM99999999999990D00'),'.',',') END AS VALORENLIBROS,
            CASE WHEN TASADESCTO = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASADESCTO, 'FM99999999999990D0000'),'.',',') END AS TASADESCTO,
            NROACCIONES,
            CASE WHEN PRECIOMERCADOACCIONES = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(PRECIOMERCADOACCIONES, 'FM99999999999990D00'),'.',',') END AS PRECIOMERCADOACCIONES,
            CASE WHEN PARTCPATRIMONIAL = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(PARTCPATRIMONIAL, 'FM99999999999990D0000'),'.',',') END AS PARTCPATRIMONIAL,
            CASE WHEN MONTOPROVISION = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(MONTOPROVISION, 'FM99999999999990D00'),'.',',') END AS MONTOPROVISION,
            NRODECRETO, LPAD(NROEMISION, 4, '0') AS NROEMISION, LPAD(SERIEINSTR, 6, '0') AS SERIEINSTR,
            CASE WHEN TASAINTCUPON = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASAINTCUPON, 'FM99999999999990D0000'),'.',',') END AS TASAINTCUPON,
            CASE WHEN TASAINTERESES = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASAINTERESES, 'FM99999999999990D0000'),'.',',') END AS TASAINTERESES,
            FECHAINICIOCP, FECHAVTOCP,
            CASE WHEN INTERESESCOBRADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(INTERESESCOBRADOS, 'FM99999999999990D00'),'.',',') END AS INTERESESCOBRADOS,
            CASE WHEN INTERESESDEVENGADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(INTERESESDEVENGADOS, 'FM99999999999990D00'),'.',',') END AS INTERESESDEVENGADOS,
            CASE WHEN DIVIDENDOSCOBRADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(DIVIDENDOSCOBRADOS, 'FM99999999999990D00'),'.',',') END AS DIVIDENDOSCOBRADOS,
            CASE WHEN RENDPORCOBRAR = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(RENDPORCOBRAR, 'FM9999999999990D00'),'.',',') END AS RENDPORCOBRAR,
            PERIOPAGOINT, FECHACOBRO,
            CASE WHEN DIVDECRNOCOBRADOS = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(DIVDECRNOCOBRADOS, 'FM99999999999990D00'),'.',',') END AS DIVDECRNOCOBRADOS,
            CTACBLEORITRANS,
            CASE WHEN MONTOCANJE = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(MONTOCANJE, 'FM99999999999990D00'),'.',',') END AS MONTOCANJE,
            FECHACANJE, CONTRAPARTECANJE, CODINSTRENTREGADO,
            CASE WHEN PATRIEMPRESA = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(PATRIEMPRESA, 'FM99999999999990D00'),'.',',') END AS PATRIEMPRESA,
            FECHAEDOFINANCIERO,
            CASE WHEN MONTOCAPSOCIAL = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(MONTOCAPSOCIAL, 'FM99999999999990D00'),'.',',') END AS MONTOCAPSOCIAL,
            CAST(NROACCIONESCIRC AS INT) AS NROACCIONESCIRC,
            CANTIDADACCIONES AS CANTIDADACCIONES,
            ACTIVOSUBYACENTE AS ACTIVOSUBYACENTE,
            CASE WHEN VALORNOMACTIVO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(VALORNOMACTIVO, 'FM99999999999990D00'),'.',',') END AS VALORNOMACTIVO,
            MONEDAACTIVO,
            CASE WHEN TIPOCAMCIERRESUB = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TIPOCAMCIERRESUB, 'FM99999999999990D0000'),'.',',') END AS TIPOCAMCIERRESUB,
            CODEMISORSUB, CODCUSTODIOSUB,
            CASE WHEN TASACUPONSUB = 0 THEN '0,0000' ELSE REPLACE(TO_CHAR(TASACUPONSUB, 'FM99999999999990D0000'),'.',',') END AS TASACUPONSUB,
            FECHAVCTOSUB,
            CASE WHEN RESULTADONETO = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(RESULTADONETO, 'FM99999999999990D00'),'.',',') END AS RESULTADONETO,
            CASE WHEN GANOPERPARTPATRIM = 0 THEN '0,00' ELSE REPLACE(TO_CHAR(GANOPERPARTPATRIM, 'FM99999999999990D00'),'.',',') END AS GANOPERPARTPATRIM,
            BASECALCULO
        FROM ATSUDEBAN.ATS_TH_AT10
        WHERE (PORCION IN ('FIDEICOMISO'))
    '''
    registros = hook.get_records(query_string)
    logger.info(f"Se obtuvieron {len(registros)} registros.")

    # Definir la ruta del archivo de salida en GCS
    gcs_bucket = 'airflow-dags-data'
    gcs_object_path = f"data/AT10/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFile}.txt"

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

dag = DAG(dag_id='AT10_ATSUDEBAN_TOFILE',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False,
          tags=['ATs', 'AT10','Unificado','To_File'])

FileName_task = PythonOperator(
    task_id='FileName_task',
    python_callable=FileName,
    dag=dag
)

AT10_ATSUDEBAN_TOFILE_task = PythonOperator(
    task_id='AT10_ATSUDEBAN_TOFILE_task',
    python_callable=AT10_ATSUDEBAN_TOFILE,
    dag=dag
)

def enviar_correo_task_callable(**kwargs):
    """Envía email usando send_email() - Convertido desde EmailOperator"""
    from airflow.utils.email import send_email
    send_email(
        to="daniel.figueroa@kreadata.com",
        subject="AT10 UNIFICADO - ARCHIVOS DE TRANSMISION (SIIF) DWH EN LA NUBE GCP (Airflow) - GALIPAN TECNOLOGICO",
        html_content="""

        <h3>¡Buen día!</h3>
        <p>Se ha generado de forma automática el <b>'AT10 Unificado'</b>, el mismo se encuentra en la ruta 'data/AT10/SALIDAS' del bucket. El nombre del reporte es: {{ var.value.FileAT }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>
    
        """
    )

enviar_correo_task = PythonOperator(
    task_id="enviar_correo",
    python_callable=enviar_correo_task_callable,
    dag=dag
)
###### SECUENCIA DE EJECUCION ######
FileName_task >> AT10_ATSUDEBAN_TOFILE_task >> enviar_correo_task
