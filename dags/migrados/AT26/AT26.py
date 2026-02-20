import os
import tempfile 
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
from decimal import Decimal
from airflow.providers.google.cloud.hooks.gcs import GCSHook 



logger = logging.getLogger(__name__)
def IMPORTAR_INSUMO(**kwargs):
    try:
        # Define la informacion del bucket y el objeto en GCS
        gcs_bucket = 'airflow-dags-data'
        gcs_object = 'data/AT26/INSUMOS/AT26_FRAUDE.csv'
        
        # Inicializa los hooks
        postgres_hook = PostgresHook(postgres_conn_id='repodataprd')
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
        logger.info("Truncando la tabla AT_STG.AT26_FRAUDE_FILE...")
        postgres_hook.run("TRUNCATE TABLE AT_STG.AT26_FRAUDE_FILE;")
        logger.info("Tabla AT_STG.AT26_FRAUDE_FILE truncada exitosamente.")

        # 4. Preparar consulta COPY
        sql_copy = """
        COPY AT_STG.AT26_FRAUDE_FILE (
            FRAUDE, CAUSA, T_INSTRUMENTO, CANAL, TIPO_FRAUDE, FRANQUICIA, MTO, 
            AFECTACION, CTA_CONTABLE, T_PERSONA, ID_CLIENTE, GENERO, EDAD, 
            IVSS, RECLAMO, FECHA_FRAUDE, PARROQUIA, NOMBRE, OPERACION, 
            MTO_FRAUDE, MTO_FRAUDE_2, RED
        )
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');
        """
        
        # 5. Descargar archivo temporal y crea directorio temporal
        temp_dir = tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, 'AT26_FRAUDE.csv')
        
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
            
            # 7. Mostrar primeras li­neas para debug
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
            count = postgres_hook.get_first("SELECT COUNT(*) FROM AT_STG.AT26_FRAUDE_FILE;")[0]
            logger.info(f"Total de registros cargados desde el insumo csv (AT26_FRAUDE): {count}")
            
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

def AT26_FRAUDE(**kwargs):
    # Conectar a la bd at26
    hook = PostgresHook(postgres_conn_id='repodataprd')
    
    # Creamos la tabla destino
    sql_query_deftxt = '''CREATE TABLE IF NOT EXISTS AT_STG.AT26_FRAUDE (
    FRAUDE        VARCHAR(50)      NULL,
    CAUSA         VARCHAR(2)       NULL,
    T_INSTRUMENTO VARCHAR(2)       NULL,
    CANAL         VARCHAR(2)       NULL,
    TIPO_FRAUDE   VARCHAR(2)       NULL,
    FRANQUICIA    VARCHAR(2)       NULL,
    MTO           NUMERIC(15,2)    NULL,
    AFECTACION    VARCHAR(2)       NULL,
    CTA_CONTABLE  VARCHAR(15)      NULL,
    T_PERSONA     VARCHAR(2)       NULL,
    ID_CLIENTE    VARCHAR(50)      NULL,
    GENERO        VARCHAR(2)       NULL,
    EDAD          VARCHAR(2)       NULL,
    IVSS          VARCHAR(2)       NULL,
    RECLAMO       VARCHAR(50)      NULL,
    FECHA_FRAUDE  VARCHAR(10)      NULL,
    PARROQUIA     VARCHAR(50)      NULL,
    NOMBRE        VARCHAR(200)     NULL,
    OPERACION     VARCHAR(50)      NULL,
    MTO_FRAUDE    NUMERIC(15,2)    NULL,
    MTO_FRAUDE_2  NUMERIC(15,2)    NULL,
    RED           VARCHAR(2)       NULL
    );'''

    logger.info("Creando/verificando tabla AT_STG.AT26_FRAUDE...")
    hook.run(sql_query_deftxt)
    logger.info("Tabla AT_STG.AT26_FRAUDE verificada/creada.")

    # Vaciamos la tabla (para no duplicar datos)
    logger.info("Truncando la tabla AT_STG.AT26_FRAUDE...")
    sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT26_FRAUDE;'''
    hook.run(sql_query_deftxt)
    logger.info("Tabla AT_STG.AT26_FRAUDE truncada exitosamente.")

    # Query de insercion en la tabla destino de AT_STG
    logger.info("Insertando datos en AT_STG.AT26_FRAUDE desde AT_STG.AT26_FRAUDE_FILE...")
    sql_query_deftxt = '''
    INSERT INTO AT_STG.AT26_FRAUDE (
        FRAUDE, 
        CAUSA, 
        T_INSTRUMENTO, 
        CANAL, 
        TIPO_FRAUDE, 
        FRANQUICIA, 
        MTO, 
        AFECTACION,
        CTA_CONTABLE, 
        T_PERSONA, 
        ID_CLIENTE, 
        GENERO, 
        EDAD, 
        IVSS, 
        RECLAMO, 
        FECHA_FRAUDE, 
        PARROQUIA, 
        NOMBRE, 
        OPERACION, 
        MTO_FRAUDE, 
        MTO_FRAUDE_2, 
        RED
    )
    SELECT
        FRAUDE, 
        CAUSA, 
        T_INSTRUMENTO, 
        CANAL, 
        TIPO_FRAUDE, 
        FRANQUICIA, 
        CAST(REPLACE(REPLACE(MTO, '.', ''), ',', '.') AS NUMERIC) AS MTO, 
        AFECTACION,
        CTA_CONTABLE, 
        T_PERSONA, 
        ID_CLIENTE, 
        GENERO, 
        EDAD, 
        IVSS, 
        RECLAMO, 
        FECHA_FRAUDE, 
        PARROQUIA, 
        NOMBRE, 
        OPERACION, 
        CAST(REPLACE(REPLACE(MTO_FRAUDE, '.', ''), ',', '.') AS NUMERIC) AS MTO_FRAUDE, 
        CAST(REPLACE(REPLACE(MTO_FRAUDE_2, '.', ''), ',', '.') AS NUMERIC) AS MTO_FRAUDE_2, 
        RED
    FROM AT_STG.AT26_FRAUDE_FILE;'''
    hook.run(sql_query_deftxt)
    logger.info("Datos insertados exitosamente en AT_STG.AT26_FRAUDE.")

def AT26_UNION(**kwargs):

    # Conexion a la bd at26
    hook = PostgresHook(postgres_conn_id='repodataprd')
    
    # Creamos la tabla destino
    sql_query_deftxt = '''
    CREATE TABLE IF NOT EXISTS AT_STG.AT26_UNION (
        FRAUDE          VARCHAR(50),
        CAUSA           VARCHAR(2),
        T_INSTRUMENTO   VARCHAR(2),
        CANAL           VARCHAR(2),
        TIPO_FRAUDE     VARCHAR(2),
        FRANQUICIA      VARCHAR(2),
        MTO             NUMERIC(15,2),
        AFECTACION      VARCHAR(2),
        CTA_CONTABLE    VARCHAR(20),
        T_PERSONA       VARCHAR(2),
        ID_CLIENTE      VARCHAR(50),
        GENERO          VARCHAR(3),
        EDAD            VARCHAR(50),
        IVSS            VARCHAR(100),
        RECLAMO         VARCHAR(50),
        FECHA_FRAUDE    VARCHAR(10),
        PARROQUIA       VARCHAR(50),
        NOMBRE          VARCHAR(200),
        OPERACION       VARCHAR(50),
        MTO_FRAUDE      NUMERIC(15,2),
        MTO_FRAUDE_2    NUMERIC(15,2),
        RED             VARCHAR(2)
    );'''
    logger.info("Creando/verificando tabla AT_STG.AT26_UNION...")
    hook.run(sql_query_deftxt)
    logger.info("Tabla AT_STG.AT26_UNION verificada/creada.")
    
    # Vaciamos la tabla
    logger.info("Truncando la tabla AT_STG.AT26_UNION...")
    sql_query_deftxt = '''TRUNCATE TABLE AT_STG.AT26_UNION;'''
    hook.run(sql_query_deftxt)
    logger.info("Tabla AT_STG.AT26_UNION truncada exitosamente.")
    
    # Insertamos en AT_STG.AT26_UNION
    logger.info("Insertando datos en AT_STG.AT26_UNION...")
    sql_query_deftxt = '''
    INSERT INTO AT_STG.AT26_UNION ( 
        FRAUDE, 
        CAUSA, 
        T_INSTRUMENTO, 
        CANAL, 
        TIPO_FRAUDE, 
        FRANQUICIA, 
        MTO, 
        AFECTACION, 
        CTA_CONTABLE, 
        T_PERSONA, 
        ID_CLIENTE, 
        GENERO,
        EDAD,
        IVSS,
        RECLAMO, 
        FECHA_FRAUDE, 
        PARROQUIA,
        NOMBRE, 
        OPERACION, 
        MTO_FRAUDE, 
        MTO_FRAUDE_2, 
        RED
    )
    SELECT 
        f.FRAUDE,
        f.CAUSA,
        f.T_INSTRUMENTO,
        f.CANAL,
        f.TIPO_FRAUDE,
        f.FRANQUICIA,
        f.MTO,
        f.AFECTACION,
        f.CTA_CONTABLE,
        f.T_PERSONA,
        f.ID_CLIENTE,
        c.GENERO,
        ROUND(EXTRACT(YEAR FROM age(current_date, to_date(c.FECHANACIMIENTO, 'YYYYMMDD')))) AS EDAD,
        c.OFICINA AS IVSS,
        f.RECLAMO,
        f.FECHA_FRAUDE,
        c.CODPARROQUIA AS PARROQUIA,
        f.NOMBRE,
        f.OPERACION,
        f.MTO_FRAUDE,
        f.MTO_FRAUDE_2,
        f.RED
    FROM AT_STG.AT26_FRAUDE f INNER JOIN ATSUDEBAN.ATS_TM_CLIENTES c ON f.ID_CLIENTE = c.IDENTIFICACIONCLIENTE;'''

    hook.run(sql_query_deftxt)
    logger.info("Datos insertados exitosamente en AT_STG.AT26_UNION.")
    
def ATS_TH_AT26(**kwargs):
    # Conexion a la bd at26
    hook = PostgresHook(postgres_conn_id='repodataprd')

    # Creamos la tabla de destino ATSUDEBAN.ATS_TH_AT26 
    sql_query_deftxt = '''
    CREATE TABLE IF NOT EXISTS ATSUDEBAN.ATS_TH_AT26 (
        NROFRAUDE         VARCHAR(20),
        CAUSAFRAUDE       VARCHAR(20),
        TIPOINSTRUMENTO   VARCHAR(2),
        CANALFRAUDE       VARCHAR(255),
        TIPOFRAUDE        VARCHAR(2),
        TIPOFRANQUICIA    VARCHAR(255),
        MONTOFRAUDE       NUMERIC(15,2),
        AFECTACIONMONETARIA VARCHAR(2),
        CODIGOCONTABLE    VARCHAR(20),
        TIPOPERSONA       VARCHAR(50),
        IDCLIENTE         VARCHAR(50),
        GENERO            VARCHAR(50),
        RANGOEDAD         VARCHAR(50),
        PENSIONADOIVSS    VARCHAR(50),
        NRORECLAMO        VARCHAR(20),
        FECHAFRAUDE       VARCHAR(8),
        CODIGOPARROQUIA   VARCHAR(8),
        NOMBRECLIENTE     VARCHAR(255),
        TIPOOPERACION     VARCHAR(50),
        MONTOFRAUDEINTERNO NUMERIC(15,2),
        MONTOFRAUDEEXTERNO NUMERIC(15,2),
        RED               VARCHAR(50)
    );'''
    logger.info("Creando/verificando tabla ATSUDEBAN.ATS_TH_AT26...")
    hook.run(sql_query_deftxt)
    logger.info("Tabla ATSUDEBAN.ATS_TH_AT26 verificada/creada.")

    # Vaciamos la tabla destino antes de la carga
    logger.info("Truncando la tabla ATSUDEBAN.ATS_TH_AT26...")
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.ATS_TH_AT26;'''
    hook.run(sql_query_deftxt)
    logger.info("Tabla ATSUDEBAN.ATS_TH_AT26 truncada exitosamente.")

    # Inserción en la tabla ATSUDEBAN.ATS_TH_AT26
    logger.info("Insertando datos en ATSUDEBAN.ATS_TH_AT26 desde AT_STG.AT26_UNION...")
    sql_query_deftxt = '''
    INSERT INTO ATSUDEBAN.ATS_TH_AT26 (
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
    SELECT DISTINCT
        FRAUDE,
        CAUSA,
        T_INSTRUMENTO,
        CANAL,
        TIPO_FRAUDE,
        FRANQUICIA,
        MTO,
        AFECTACION,
        CTA_CONTABLE,
        T_PERSONA,
        ID_CLIENTE,
        GENERO,
        CASE
            WHEN CAST(EDAD AS numeric) <= 15 THEN '1'
            WHEN CAST(EDAD AS numeric) <= 30 THEN '2'
            WHEN CAST(EDAD AS numeric) <= 45 THEN '3'
            WHEN CAST(EDAD AS numeric) <= 60 THEN '4'
            WHEN CAST(EDAD AS numeric) <= 75 THEN '5'
            WHEN CAST(EDAD AS numeric) > 75  THEN '6'
            ELSE '0'
        END AS RANGOEDAD,
        CASE
            WHEN IVSS IN ('169','179') THEN '2'
            ELSE '1'
        END AS PENSIONADOIVSS,
        RECLAMO,
        FECHA_FRAUDE,
        CASE
            WHEN LENGTH(PARROQUIA) < 6 THEN LPAD(PARROQUIA, 6, '0')
            ELSE PARROQUIA
        END AS CODIGOPARROQUIA,
        NOMBRE,
        OPERACION,
        MTO_FRAUDE,
        MTO_FRAUDE_2,
        RED
    FROM AT_STG.AT26_UNION;'''

    hook.run(sql_query_deftxt)
    logger.info("Datos insertados exitosamente en ATSUDEBAN.ATS_TH_AT26.")

def ATS_TH_AT26_DATA_CHECK_TO_AT26_TH_BC(**kwargs):

    # Conexion a la bd at26
    hook = PostgresHook(postgres_conn_id='repodataprd')

    # Creamos la tabla de destino ATSUDEBAN.AT26_TH_BC 
    sql_query_deftxt = '''
    CREATE TABLE IF NOT EXISTS ATSUDEBAN.AT26_TH_BC (
        NROFRAUDE         VARCHAR(20),
        CAUSAFRAUDE       VARCHAR(20),
        TIPOINSTRUMENTO   VARCHAR(2),
        CANALFRAUDE       VARCHAR(255),
        TIPOFRAUDE        VARCHAR(2),
        TIPOFRANQUICIA    VARCHAR(255),
        MONTOFRAUDE       NUMERIC(15,2),
        AFECTACIONMONETARIA VARCHAR(2),
        CODIGOCONTABLE    VARCHAR(20),
        TIPOPERSONA       VARCHAR(50),
        IDCLIENTE         VARCHAR(50),
        GENERO            VARCHAR(50),
        RANGOEDAD         VARCHAR(50),
        PENSIONADOIVSS    VARCHAR(50),
        NRORECLAMO        VARCHAR(20),
        FECHAFRAUDE       VARCHAR(8),
        CODIGOPARROQUIA   VARCHAR(8),
        NOMBRECLIENTE     VARCHAR(255),
        TIPOOPERACION     VARCHAR(50),
        MONTOFRAUDEINTERNO NUMERIC(15,2),
        MONTOFRAUDEEXTERNO NUMERIC(15,2),
        RED               VARCHAR(50)
    );
    '''
    logger.info("Creando/verificando tabla ATSUDEBAN.AT26_TH_BC...")
    hook.run(sql_query_deftxt)
    logger.info("Tabla ATSUDEBAN.AT26_TH_BC verificada/creada.")

    # Vaciamos la tabla destino antes de la carga
    logger.info("Truncando la tabla ATSUDEBAN.AT26_TH_BC...")
    sql_query_deftxt = '''TRUNCATE TABLE ATSUDEBAN.AT26_TH_BC;'''
    hook.run(sql_query_deftxt)
    logger.info("Tabla ATSUDEBAN.AT26_TH_BC truncada exitosamente.")

    # Query de inserción en la tabla ATSUDEBAN.AT26_TH_BC usando las condiciones definidas
    logger.info("Insertando datos en ATSUDEBAN.AT26_TH_BC desde ATSUDEBAN.ATS_TH_AT26...")
    sql_query_deftxt = '''
    INSERT INTO ATSUDEBAN.AT26_TH_BC (
        NROFRAUDE,CAUSAFRAUDE,TIPOINSTRUMENTO,CANALFRAUDE,TIPOFRAUDE,TIPOFRANQUICIA,MONTOFRAUDE,AFECTACIONMONETARIA,CODIGOCONTABLE,TIPOPERSONA,IDCLIENTE,GENERO,RANGOEDAD,PENSIONADOIVSS,NRORECLAMO,FECHAFRAUDE,CODIGOPARROQUIA,NOMBRECLIENTE,TIPOOPERACION,MONTOFRAUDEINTERNO,MONTOFRAUDEEXTERNO,RED
    )
    SELECT DISTINCT
        NROFRAUDE,CAUSAFRAUDE,TIPOINSTRUMENTO,CANALFRAUDE,TIPOFRAUDE,TIPOFRANQUICIA,MONTOFRAUDE,AFECTACIONMONETARIA,CODIGOCONTABLE,TIPOPERSONA,IDCLIENTE,GENERO,RANGOEDAD,PENSIONADOIVSS,NRORECLAMO,FECHAFRAUDE,CODIGOPARROQUIA,NOMBRECLIENTE,TIPOOPERACION,MONTOFRAUDEINTERNO,MONTOFRAUDEEXTERNO,RED
    FROM ATSUDEBAN.ATS_TH_AT26
    WHERE (
        (
            (TIPOINSTRUMENTO IN ('30','31','32') AND CANALFRAUDE NOT IN ('5'))
            OR (TIPOINSTRUMENTO IN ('33') AND CANALFRAUDE NOT IN ('3','5','10'))
            OR (TIPOINSTRUMENTO IN ('34') AND CANALFRAUDE NOT IN ('2','3','5'))
            OR (TIPOINSTRUMENTO IN ('35') AND CANALFRAUDE NOT IN ('1','2','3','4','5','6','7','9','10'))
            OR (TIPOINSTRUMENTO IN ('36','37') AND CANALFRAUDE NOT IN ('1','2','3','5','7','9','10'))
            OR (TIPOINSTRUMENTO IN ('38','39') AND CANALFRAUDE NOT IN ('1','2','5','7','9','10'))
            OR (TIPOINSTRUMENTO IN ('40','41') AND CANALFRAUDE NOT IN ('1','2','10'))
            OR (TIPOINSTRUMENTO IN ('42','43','44','45','46','47','48','49') AND CANALFRAUDE NOT IN ('1','2','5','7','9','10'))
        )
        AND CODIGOCONTABLE != '0'
        AND CODIGOPARROQUIA != '0'
        AND FECHAFRAUDE > '20101231'
        AND (
            (TIPOPERSONA IN ('E','P','V') AND GENERO IN ('1','2'))
            OR (TIPOPERSONA NOT IN ('E','P','V') AND GENERO IN ('0'))
            OR (AFECTACIONMONETARIA IN ('3') AND GENERO IN ('1','2'))
        )
        AND (
            (TIPOPERSONA = 'V' AND CAST(IDCLIENTE AS INTEGER) BETWEEN 1 AND 40000000)
            OR (TIPOPERSONA = 'E' AND (CAST(IDCLIENTE AS INTEGER) BETWEEN 1 AND 1500000 OR CAST(IDCLIENTE AS INTEGER) BETWEEN 80000000 AND 100000000))
            OR (TIPOPERSONA IN ('J','G') AND CAST(IDCLIENTE AS INTEGER) > 0)
        )
        AND MONTOFRAUDE > 0
        AND (
            (AFECTACIONMONETARIA IN ('2','3') AND MONTOFRAUDEEXTERNO > 0)
            OR (AFECTACIONMONETARIA IN ('1') AND MONTOFRAUDEEXTERNO = 0)
        )
        AND (
            (AFECTACIONMONETARIA IN ('1','3') AND MONTOFRAUDEINTERNO > 0)
            OR (AFECTACIONMONETARIA IN ('2') AND MONTOFRAUDEINTERNO = 0)
        )
        AND NROFRAUDE IS NOT NULL
        AND (
            (AFECTACIONMONETARIA IN ('1') AND NRORECLAMO IN ('0'))
            OR (AFECTACIONMONETARIA IN ('2','3') AND NRORECLAMO NOT IN ('0'))
        )
        AND (
            (TIPOPERSONA IN ('E','P','V') AND PENSIONADOIVSS NOT IN ('0'))
            OR (TIPOPERSONA NOT IN ('E','P','V') AND PENSIONADOIVSS IN ('0'))
        )
        AND (
            (TIPOPERSONA IN ('E','P','V') AND GENERO NOT IN ('0'))
            OR (TIPOPERSONA NOT IN ('E','P','V') AND GENERO IN ('0'))
        )
        AND (
            (TIPOINSTRUMENTO IN ('30','31','32') AND RED != '0')
            OR (TIPOINSTRUMENTO NOT IN ('30','31','32') AND RED = '0')
        )
        AND (
            (TIPOINSTRUMENTO IN ('30','32') AND TIPOFRANQUICIA NOT IN ('0'))
            OR (TIPOINSTRUMENTO NOT IN ('30','32') AND TIPOFRANQUICIA IN ('0'))
        )
        AND (
            (AFECTACIONMONETARIA IN ('1') AND TIPOPERSONA IN ('J','G'))
            OR (AFECTACIONMONETARIA IN ('3') AND TIPOPERSONA IN ('0'))
            OR AFECTACIONMONETARIA NOT IN ('0')
        )
    )
    ORDER BY NROFRAUDE, TIPOFRAUDE, CANALFRAUDE  asc;
    '''

    hook.run(sql_query_deftxt)
    logger.info("Datos insertados exitosamente en ATSUDEBAN.AT26_TH_BC.")


###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT26', default_args=default_args, schedule=None, catchup=False)

IMPORTAR_INSUMO_task = PythonOperator(
    task_id='IMPORTAR_INSUMO_task',
    python_callable=IMPORTAR_INSUMO,
    dag=dag
)

AT26_FRAUDE_task = PythonOperator(
    task_id='AT26_FRAUDE_task',
    python_callable=AT26_FRAUDE,
    dag=dag
)

AT26_UNION_task = PythonOperator(
    task_id='AT26_UNION_task',
    python_callable=AT26_UNION,
    dag=dag
)

ATS_TH_AT26_task = PythonOperator(
    task_id='ATS_TH_AT26_task',
    python_callable=ATS_TH_AT26,
    dag=dag
)

ATS_TH_AT26_DATA_CHECK_TO_AT26_TH_BC_task = PythonOperator(
    task_id='ATS_TH_AT26_DATA_CHECK_TO_AT26_TH_BC_task',
    python_callable=ATS_TH_AT26_DATA_CHECK_TO_AT26_TH_BC,
    dag=dag
)


###### SECUENCIA DE EJECUCION ######
IMPORTAR_INSUMO_task >> AT26_FRAUDE_task >> AT26_UNION_task >> ATS_TH_AT26_task >> ATS_TH_AT26_DATA_CHECK_TO_AT26_TH_BC_task