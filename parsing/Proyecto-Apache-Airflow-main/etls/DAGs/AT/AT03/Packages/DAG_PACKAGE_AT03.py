from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import time
import tempfile
import os
import logging
from decimal import Decimal
import json

logger = logging.getLogger(__name__)

def serialize_value(value):
    """
    Helper para serializar valores de PostgreSQL a JSON.
    Convierte Decimal, date, datetime a string para preservar precision.
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
    
    Para conversiones especificas:
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

def AT03_AGRUPACION_CONTABLE_BANCARIBE(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
    COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS C1_OFICINA,
    SUBSTR(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0'), 1, 12) AS C2_CUENTA,
    CB_HIST_SALDO_AT03.HI_SALDO AS C3_SALDO,
    CB_HIST_SALDO_AT03.HI_EMPRESA AS C4_EMPRESA,
    CB_CUENTA.CU_CATEGORIA AS C5_CATEGORIA
FROM
    ODS.CB_CUENTA AS CB_CUENTA,
    ODS.CB_RELOFI AS CB_RELOFI,
    ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03
WHERE
    1 = 1
    AND CB_HIST_SALDO_AT03.HI_PERIODO = {AT03_Max_Periodo}
    AND CB_HIST_SALDO_AT03.HI_CORTE = {AT03_Max_Corte}
    AND CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700, 701, 702, 703, 703, 704, 705, 708, 709, 710, 711, 713, 714, 715)
    AND CB_HIST_SALDO_AT03.HI_AREA >= 0
    AND CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9'
    AND CB_CUENTA.CU_CATEGORIA IS NOT NULL
    AND CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA
    AND CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN
    AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA;'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0AGRUPACION_BANCARIBE" (C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA) VALUES (:C1_OFICINA, :C2_CUENTA, :C3_SALDO, :C4_EMPRESA, :C5_CATEGORIA);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS "AGRUPACION_BANCARIBE" (
    OFICINA NUMERIC(10),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    EMPRESA NUMERIC(3),
    CATEGORIA VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM agrupacion_bancariber;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "AGRUPACION_BANCARIBE" (OFICINA, CUENTA, SALDO, EMPRESA, CATEGORIA)
SELECT DISTINCT C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA
FROM "%COL_PRF0AGRUPACION_BANCARIBE"
WHERE (1 = 1);'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_V001_BANCARIBE (OFICINA VARCHAR(4), CUENTA VARCHAR(12), SALDO NUMERIC(32, 7), CONSTANTE VARCHAR(1));'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_V001_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_V001_BANCARIBE (CUENTA, SALDO, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, 'V001', '1'
FROM (SELECT AT03_OFICINA_FINAL.CUENTA AS CUENTA, SUM(AT03_OFICINA_FINAL.SALDO) AS SALDO
      FROM AT03_OFICINA_FINAL
      WHERE (1 = 1)
      GROUP BY AT03_OFICINA_FINAL.CUENTA) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo}
    AND CB_HIST_SALDO.hi_empresa = 1
    AND CB_HIST_SALDO.hi_corte = {AT03_Max_Corte};'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "%COL_PRF0CB_HIST_SALDO_AT03" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM %COL_PRF0CB_HIST_SALDO_AT03 WHERE (1 = 1)) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo}
    AND CB_HIST_SALDO.hi_empresa = 2
    AND CB_HIST_SALDO.hi_corte = {AT03_Max_Corte};'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME
      FROM "%COL_PRF0CB_HIST_SALDO_AT") AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def AT_AGRUPACION_V001_BANCARIBE(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
    COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS C1_OFICINA,
    SUBSTR(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0'), 1, 12) AS C2_CUENTA,
    CB_HIST_SALDO_AT03.HI_SALDO AS C3_SALDO,
    CB_HIST_SALDO_AT03.HI_EMPRESA AS C4_EMPRESA,
    CB_CUENTA.CU_CATEGORIA AS C5_CATEGORIA
FROM
    ODS.CB_CUENTA AS CB_CUENTA
INNER JOIN
    ODS.CB_RELOFI AS CB_RELOFI
    ON CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA
INNER JOIN
    ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03
    ON CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA
WHERE
    1 = 1
    AND CB_HIST_SALDO_AT03.HI_PERIODO = {AT03_Max_Periodo}
    AND CB_HIST_SALDO_AT03.HI_CORTE = {AT03_Max_Corte}
    AND CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700, 701, 702, 703, 703, 704, 705, 708, 709, 710, 711, 713, 714, 715)
    AND CB_HIST_SALDO_AT03.HI_AREA >= 0
    AND CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9'
    AND CB_CUENTA.CU_CATEGORIA IS NOT NULL;'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0AGRUPACION_BANCARIBE" (C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA) VALUES (:C1_OFICINA, :C2_CUENTA, :C3_SALDO, :C4_EMPRESA, :C5_CATEGORIA);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS "AGRUPACION_BANCARIBE" (
    "OFICINA" NUMERIC(10),
    "CUENTA" VARCHAR(12),
    "SALDO" NUMERIC(32, 7),
    "EMPRESA" NUMERIC(3),
    "CATEGORIA" VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM agrupacion_bancariber;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "AGRUPACION_BANCARIBE" (OFICINA, CUENTA, SALDO, EMPRESA, CATEGORIA)
SELECT DISTINCT C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA
FROM "%COL_PRF0AGRUPACION_BANCARIBE" AS "ATSUDEBAN"
WHERE (1 = 1);'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_V001_BANCARIBE (
    OFICINA VARCHAR(4),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    CONSTANTE VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_V001_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_V001_BANCARIBE (CUENTA, SALDO, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, 'V001', '1'
FROM (SELECT AT03_OFICINA_FINAL.CUENTA AS CUENTA, SUM(AT03_OFICINA_FINAL.SALDO) AS SALDO
      FROM AT03_OFICINA_FINAL
      WHERE (1 = 1)
      GROUP BY AT03_OFICINA_FINAL.CUENTA) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 1)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT03" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM "%COL_PRF0CB_HIST_SALDO_AT03") AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo}
    AND CB_HIST_SALDO.hi_empresa = 2
    AND CB_HIST_SALDO.hi_corte = {AT03_Max_Corte};'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "%COL_PRF0CB_HIST_SALDO_AT" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ODS.CB_HIST_SALDO_AT (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM ODS.%COL_PRF0CB_HIST_SALDO_AT_W) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
    COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS C1_OFICINA,
    SUBSTR(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0'), 1, 12) AS C2_CUENTA,
    CB_HIST_SALDO_AT03.HI_SALDO AS C3_SALDO,
    CB_HIST_SALDO_AT03.HI_EMPRESA AS C4_EMPRESA,
    CB_CUENTA.CU_CATEGORIA AS C5_CATEGORIA
FROM
    ODS.CB_CUENTA AS CB_CUENTA
INNER JOIN
    ODS.CB_RELOFI AS CB_RELOFI
    ON CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA
INNER JOIN
    ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03
    ON CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA
WHERE
    CB_HIST_SALDO_AT03.HI_PERIODO = {AT03_Max_Periodo}
    AND CB_HIST_SALDO_AT03.HI_CORTE = {AT03_Max_Corte}
    AND CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700, 701, 702, 703, 704, 705, 708, 709, 710, 711, 713, 714, 715)
    AND CB_HIST_SALDO_AT03.HI_AREA >= 0
    AND CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9'
    AND CB_CUENTA.CU_CATEGORIA IS NOT NULL;'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0AGRUPACION_BANCARIBE" (C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA) VALUES (:C1_OFICINA, :C2_CUENTA, :C3_SALDO, :C4_EMPRESA, :C5_CATEGORIA);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AGRUPACION_BANCARIBE (
    OFICINA NUMERIC(10),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    EMPRESA NUMERIC(3),
    CATEGORIA VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "AGRUPACION_BANCARIBE" (OFICINA, CUENTA, SALDO, EMPRESA, CATEGORIA)
SELECT DISTINCT
    C1_OFICINA,
    C2_CUENTA,
    C3_SALDO,
    C4_EMPRESA,
    C5_CATEGORIA
FROM "%COL_PRF0AGRUPACION_BANCARIBE"
WHERE (1 = 1);'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_V001_BANCARIBE (OFICINA VARCHAR(4), CUENTA VARCHAR(12), SALDO NUMERIC(32, 7), CONSTANTE VARCHAR(1));'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_V001_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_V001_BANCARIBE (CUENTA, SALDO, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, 'V001', '1'
FROM (
    SELECT AT03_OFICINA_FINAL.CUENTA AS CUENTA, SUM(AT03_OFICINA_FINAL.SALDO) AS SALDO
    FROM AT03_OFICINA_FINAL
    WHERE (1 = 1)
    GROUP BY AT03_OFICINA_FINAL.CUENTA
) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    cb_hist_saldo AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 1)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT03" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME
      FROM "%COL_PRF0CB_HIST_SALDO_AT03") AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo}
    AND CB_HIST_SALDO.hi_empresa = 2
    AND CB_HIST_SALDO.hi_corte = {AT03_Max_Corte};'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ODS.CB_HIST_SALDO_AT (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (
    SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME
    FROM ODS.%COL_PRF0CB_HIST_SALDO_AT
    WHERE (1 = 1)
) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def IT_CB_HIST_SALDO_ODS_BANGENTE_AT03(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
    COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS C1_OFICINA,
    SUBSTR(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0'), 1, 12) AS C2_CUENTA,
    CB_HIST_SALDO_AT03.HI_SALDO AS C3_SALDO,
    CB_HIST_SALDO_AT03.HI_EMPRESA AS C4_EMPRESA,
    CB_CUENTA.CU_CATEGORIA AS C5_CATEGORIA
FROM
    ODS.CB_CUENTA AS CB_CUENTA,
    ODS.CB_RELOFI AS CB_RELOFI,
    ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03
WHERE
    1 = 1
    AND CB_HIST_SALDO_AT03.HI_PERIODO = {AT03_Max_Periodo}
    AND CB_HIST_SALDO_AT03.HI_CORTE = {AT03_Max_Corte}
    AND CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700, 701, 702, 703, 703, 704, 705, 708, 709, 710, 711, 713, 714, 715)
    AND CB_HIST_SALDO_AT03.HI_AREA >= 0
    AND CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9'
    AND CB_CUENTA.CU_CATEGORIA IS NOT NULL
    AND CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA
    AND CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN
    AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA;'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0AGRUPACION_BANCARIBE" (C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA) VALUES (:C1_OFICINA, :C2_CUENTA, :C3_SALDO, :C4_EMPRESA, :C5_CATEGORIA);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AGRUPACION_BANCARIBE (
    OFICINA NUMERIC(10),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    EMPRESA NUMERIC(3),
    CATEGORIA VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM agrupacion_bancariber;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "AGRUPACION_BANCARIBE" (OFICINA, CUENTA, SALDO, EMPRESA, CATEGORIA)
SELECT DISTINCT
    C1_OFICINA,
    C2_CUENTA,
    C3_SALDO,
    C4_EMPRESA,
    C5_CATEGORIA
FROM "%COL_PRF0AGRUPACION_BANCARIBE"
WHERE (1 = 1);'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_V001_BANCARIBE (
    OFICINA VARCHAR(4) NULL,
    CUENTA VARCHAR(12) NULL,
    SALDO NUMERIC(32, 7) NULL,
    CONSTANTE VARCHAR(1) NULL
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_V001_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_V001_BANCARIBE (CUENTA, SALDO, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, 'V001', '1'
FROM (SELECT AT03_OFICINA_FINAL.CUENTA AS CUENTA, SUM(AT03_OFICINA_FINAL.SALDO) AS SALDO
      FROM AT03_OFICINA_FINAL
      GROUP BY AT03_OFICINA_FINAL.CUENTA) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo}
    AND CB_HIST_SALDO.hi_empresa = 1
    AND CB_HIST_SALDO.hi_corte = {AT03_Max_Corte};'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT03" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, SYSDATE, SYSDATE
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM %COL_PRF0CB_HIST_SALDO_AT03 WHERE (1 = 1)) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 2)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE "CB_HIST_SALDO_AT";'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ODS.CB_HIST_SALDO_AT (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, SYSDATE, SYSDATE
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM ODS.%COL_PRF0CB_HIST_SALDO_AT WHERE (1 = 1)) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def AT03_Max_Periodo(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''SELECT CASE
    WHEN EXTRACT(MONTH FROM CURRENT_DATE) = '01' THEN '2' || (EXTRACT(YEAR FROM CURRENT_DATE)::text || '01')::int - 1
    WHEN EXTRACT(MONTH FROM CURRENT_DATE) < '08' THEN '1' || EXTRACT(YEAR FROM CURRENT_DATE)::text
    ELSE '2' || EXTRACT(YEAR FROM CURRENT_DATE)::text
END;'''
    result = hook.get_records(sql_query)

    Variable.set('AT03_Max_Periodo', serialize_value(result[0][0]))

def AT03_Max_Corte(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
    COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS C1_OFICINA,
    SUBSTR(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0'), 1, 12) AS C2_CUENTA,
    CB_HIST_SALDO_AT03.HI_SALDO AS C3_SALDO,
    CB_HIST_SALDO_AT03.HI_EMPRESA AS C4_EMPRESA,
    CB_CUENTA.CU_CATEGORIA AS C5_CATEGORIA
FROM
    ODS.CB_CUENTA AS CB_CUENTA,
    ODS.CB_RELOFI AS CB_RELOFI,
    ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03
WHERE
    1 = 1
    AND CB_HIST_SALDO_AT03.HI_PERIODO = {AT03_Max_Periodo}
    AND CB_HIST_SALDO_AT03.HI_CORTE = {AT03_Max_Corte}
    AND CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700, 701, 702, 703, 703, 704, 705, 708, 709, 710, 711, 713, 714, 715)
    AND CB_HIST_SALDO_AT03.HI_AREA >= 0
    AND CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9'
    AND CB_CUENTA.CU_CATEGORIA IS NOT NULL
    AND CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA
    AND CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN
    AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA;'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0AGRUPACION_BANCARIBE" (C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA) VALUES (:C1_OFICINA, :C2_CUENTA, :C3_SALDO, :C4_EMPRESA, :C5_CATEGORIA);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS "AGRUPACION_BANCARIBE" (
    "OFICINA" NUMERIC(10),
    "CUENTA" VARCHAR(12),
    "SALDO" NUMERIC(32, 7),
    "EMPRESA" NUMERIC(3),
    "CATEGORIA" VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM "AGRUPACION_BANCARIBE";'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "AGRUPACION_BANCARIBE" (OFICINA, CUENTA, SALDO, EMPRESA, CATEGORIA)
SELECT DISTINCT
    C1_OFICINA,
    C2_CUENTA,
    C3_SALDO,
    C4_EMPRESA,
    C5_CATEGORIA
FROM "%COL_PRF0AGRUPACION_BANCARIBE"
WHERE (1 = 1);'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_V001_BANCARIBE (
    OFICINA VARCHAR(4),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    CONSTANTE VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_V001_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_V001_BANCARIBE (CUENTA, SALDO, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, 'V001', '1'
FROM (SELECT AT03_OFICINA_FINAL.CUENTA AS CUENTA, SUM(AT03_OFICINA_FINAL.SALDO) AS SALDO
      FROM AT03_OFICINA_FINAL
      GROUP BY AT03_OFICINA_FINAL.CUENTA) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 1)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT03" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM "CB_HIST_SALDO_AT03";'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM %COL_PRF0CB_HIST_SALDO_AT03 WHERE (1 = 1)) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 2)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, CURRENT_DATE, CURRENT_DATE
FROM (SELECT C1_HI_EMPRESA AS HI_EMPRESA, C2_HI_CUENTA AS HI_CUENTA, C3_HI_OFICINA AS HI_OFICINA, C4_HI_AREA AS HI_AREA, C5_HI_CORTE AS HI_CORTE, C6_HI_PERIODO AS HI_PERIODO, C7_HI_SALDO AS HI_SALDO, C8_HI_SALDO_ME AS HI_SALDO_ME FROM %COL_PRF0CB_HIST_SALDO_AT WHERE (1 = 1)) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    sql_query = f'''```sql
SELECT MAX(co_corte)
FROM cb_corte
WHERE TO_CHAR(co_periodo) = '{AT03_Max_Periodo}'
  AND TO_CHAR(co_fecha_fin, 'yyyymmdd') = TO_CHAR(LAST_DAY(CURRENT_DATE - INTERVAL '28 days'), 'yyyymmdd')
GROUP BY co_periodo;
```'''
    result = hook.get_records(sql_query)

    Variable.set('AT03_Max_Corte', serialize_value(result[0][0]))

def IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_DEBITO, HI_CREDITO, HI_DEBITO_ME, HI_CREDITO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, LTRIM(RTRIM(HI_CUENTA)), HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_DEBITO, HI_CREDITO, HI_DEBITO_ME, HI_CREDITO_ME, SYSDATE, SYSDATE
FROM CB_HIST_SALDO
WHERE HI_PERIODO = {AT03_Max_Periodo}
  AND HI_CORTE = {AT03_Max_Corte}
  AND HI_EMPRESA = '1';'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def AT03_AGRUPACION_CONTABLE_BANCARIBE(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM AGRUPACION_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO "AGRUPACION_BANCARIBE" (OFICINA, CUENTA, SALDO, EMPRESA, CATEGORIA)
SELECT DISTINCT C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA
FROM "%COL_PRF0AGRUPACION_BANCARIBE"
WHERE (1 = 1);'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 

    # Load data
    sql_query_coltxt = f'''SELECT DISTINCT
    COALESCE(CB_RELOFI.RE_OFCONTA, CB_HIST_SALDO_AT03.HI_OFICINA) AS C1_OFICINA,
    SUBSTR(RPAD(CB_HIST_SALDO_AT03.HI_CUENTA, 20, '0'), 1, 12) AS C2_CUENTA,
    CB_HIST_SALDO_AT03.HI_SALDO AS C3_SALDO,
    CB_HIST_SALDO_AT03.HI_EMPRESA AS C4_EMPRESA,
    CB_CUENTA.CU_CATEGORIA AS C5_CATEGORIA
FROM
    ODS.CB_CUENTA AS CB_CUENTA,
    ODS.CB_RELOFI AS CB_RELOFI,
    ODS.CB_HIST_SALDO_AT03 AS CB_HIST_SALDO_AT03
WHERE
    1 = 1
    AND CB_HIST_SALDO_AT03.HI_PERIODO = {AT03_Max_Periodo}
    AND CB_HIST_SALDO_AT03.HI_CORTE = {AT03_Max_Corte}
    AND CB_HIST_SALDO_AT03.HI_OFICINA NOT IN (700, 701, 702, 703, 704, 705, 708, 709, 710, 711, 713, 714, 715)
    AND CB_HIST_SALDO_AT03.HI_AREA >= 0
    AND CB_HIST_SALDO_AT03.HI_CUENTA BETWEEN '1' AND '9'
    AND CB_CUENTA.CU_CATEGORIA IS NOT NULL
    AND CB_HIST_SALDO_AT03.HI_CUENTA = CB_CUENTA.CU_CUENTA
    AND CB_HIST_SALDO_AT03.HI_OFICINA = CB_RELOFI.RE_OFADMIN
    AND CB_HIST_SALDO_AT03.HI_EMPRESA = CB_RELOFI.RE_EMPRESA;'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0AGRUPACION_BANCARIBE" (C1_OFICINA, C2_CUENTA, C3_SALDO, C4_EMPRESA, C5_CATEGORIA) VALUES (:C1_OFICINA, :C2_CUENTA, :C3_SALDO, :C4_EMPRESA, :C5_CATEGORIA);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS "AGRUPACION_BANCARIBE" (
    OFICINA NUMERIC(10),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    EMPRESA NUMERIC(3),
    CATEGORIA VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 


def Actualizar_cuenta_formato_Sudeban(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Actualizar cuenta 714001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '714001000000'
WHERE CUENTA = '714011000000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 714001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 714001000000, ejecutada exitosamente") 

    # Actualizar cuenta 751001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '751001000000'
WHERE CUENTA = '751011000000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 751001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 751001000000, ejecutada exitosamente") 

    # Actualizar cuenta 712002000000
    sql_query_deftxt = f'''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
SET CUENTA = '712002000000'
WHERE CUENTA = '712012010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 712002000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 712002000000, ejecutada exitosamente") 

    # Actualizar cuenta 717001000000
    sql_query_deftxt = f'''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
SET CUENTA = '717001000000'
WHERE CUENTA = '717011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 717001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 717001000000, ejecutada exitosamente") 

    # Actualizar cuenta 722001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '722001000000'
WHERE CUENTA = '722011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 722001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 722001000000, ejecutada exitosamente") 

    # Actualizar cuenta 732001000000
    sql_query_deftxt = f'''UPDATE ATSUDEBAN.AGRUPACION_BANCARIBE
SET CUENTA = '732001000000'
WHERE CUENTA = '732011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 732001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 732001000000, ejecutada exitosamente") 

    # Actualizar cuenta 743001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '743001000000'
WHERE CUENTA = '743011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 743001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 743001000000, ejecutada exitosamente") 

    # Actualizar cuenta 744001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '744001000000'
WHERE CUENTA = '744010000000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 744001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 744001000000, ejecutada exitosamente") 

    # Actualizar cuenta 753001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '753001000000'
WHERE CUENTA = '753011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 753001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 753001000000, ejecutada exitosamente") 

    # Actualizar cuenta 712001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '712001000000'
WHERE CUENTA = '712011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 712001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 712001000000, ejecutada exitosamente") 

    # Actualizar cuenta 734001000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '734001000000'
WHERE CUENTA = '734011010000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 734001000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 734001000000, ejecutada exitosamente") 

    # Actualizar cuenta 714002000000
    sql_query_deftxt = f''';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 714002000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 714002000000, ejecutada exitosamente") 

    # Actualizar cuenta 722002000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET "CUENTA" = '722002000000'
WHERE "CUENTA" = '722012000000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 722002000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 722002000000, ejecutada exitosamente") 

    # Actualizar cuenta 751002000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '751002000000'
WHERE CUENTA = '751012000000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 751002000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 751002000000, ejecutada exitosamente") 

    # Actualizar cuenta 744002000000
    sql_query_deftxt = f'''UPDATE "ATSUDEBAN"."AGRUPACION_BANCARIBE"
SET CUENTA = '744002000000'
WHERE CUENTA = '744020000000';'''
    logger.info("Accion a ejecutarse: Actualizar cuenta 744002000000") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar cuenta 744002000000, ejecutada exitosamente") 


def AT03_AGRUPACION_OFICINAS_BANCARIBE(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_OFICINA (CUENTA VARCHAR(12), OFICINA VARCHAR(4), SALDO NUMERIC(32, 7), CONSTANTE VARCHAR(1), EMPRESA NUMERIC(3), CATEGORIA VARCHAR(1));'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_OFICINA;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_OFICINA (CUENTA, OFICINA, SALDO, EMPRESA, CATEGORIA, CONSTANTE)
SELECT
    CUENTA,
    LPAD(OFICINA, 4, '0'),
    SUM(SALDO),
    EMPRESA,
    CATEGORIA,
    '1'
FROM (
    SELECT
        AGRUPACION_BANCARIBE.CUENTA AS CUENTA,
        AGRUPACION_BANCARIBE.OFICINA AS OFICINA,
        AGRUPACION_BANCARIBE.SALDO AS SALDO,
        AGRUPACION_BANCARIBE.EMPRESA AS EMPRESA,
        AGRUPACION_BANCARIBE.CATEGORIA AS CATEGORIA
    FROM AGRUPACION_BANCARIBE, SIIF_AT03_CUENTACONTABLE
    WHERE TRIM(SIIF_AT03_CUENTACONTABLE.CUENTACONTABLE) = TRIM(AGRUPACION_BANCARIBE.CUENTA)
      AND TRIM(AGRUPACION_BANCARIBE.EMPRESA) = TRIM(SIIF_AT03_CUENTACONTABLE.IDBANCO)
      AND AGRUPACION_BANCARIBE.OFICINA NOT IN ('157', '306')
    GROUP BY AGRUPACION_BANCARIBE.CUENTA,
             AGRUPACION_BANCARIBE.OFICINA,
             AGRUPACION_BANCARIBE.EMPRESA,
             AGRUPACION_BANCARIBE.CATEGORIA
) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_OFICINA_SOBREG (CUENTA VARCHAR(12), OFICINA VARCHAR(4), SALDO NUMERIC(32, 7), CONSTANTE VARCHAR(1), EMPRESA NUMERIC(3));'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_OFICINA_SOBREG;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_OFICINA_SOBREG (CUENTA, SALDO, EMPRESA, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, EMPRESA, '0147', '1'
FROM (SELECT AT03_OFICINA.CUENTA AS CUENTA, SUM(AT03_OFICINA.SALDO) AS SALDO, AT03_OFICINA.EMPRESA AS EMPRESA
      FROM AT03_OFICINA
      WHERE AT03_OFICINA.SALDO < 0
        AND AT03_OFICINA.CATEGORIA = 'D'
      GROUP BY AT03_OFICINA.CUENTA, AT03_OFICINA.EMPRESA) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_OFICINA_FINAL (CUENTA VARCHAR(12), OFICINA VARCHAR(4), SALDO NUMERIC(32, 7), CONSTANTE VARCHAR(1), EMPRESA NUMERIC(3));'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_OFICINA_FINAL;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_OFICINA_FINAL (CUENTA, OFICINA, SALDO, EMPRESA, CONSTANTE)
SELECT CUENTA, OFICINA, SALDO, EMPRESA, '1'
FROM (SELECT AT03_OFICINA.CUENTA AS CUENTA, AT03_OFICINA.OFICINA AS OFICINA, AT03_OFICINA.SALDO AS SALDO, AT03_OFICINA.EMPRESA AS EMPRESA
      FROM AT03_OFICINA
      WHERE AT03_OFICINA.SALDO <> 0) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Actualizar Saldo Negativos
    sql_query_deftxt = f''';'''
    logger.info("Accion a ejecutarse: Actualizar Saldo Negativos") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar Saldo Negativos, ejecutada exitosamente") 

    # Actualizar Cuentas de la 0147
    sql_query_deftxt = f''';'''
    logger.info("Accion a ejecutarse: Actualizar Cuentas de la 0147") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Actualizar Cuentas de la 0147, ejecutada exitosamente") 


def AT_AGRUPACION_V001_BANCARIBE(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Create target table
    sql_query_deftxt = f'''CREATE TABLE IF NOT EXISTS AT03_V001_BANCARIBE (
    OFICINA VARCHAR(4),
    CUENTA VARCHAR(12),
    SALDO NUMERIC(32, 7),
    CONSTANTE VARCHAR(1)
);'''
    logger.info("Accion a ejecutarse: Create target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Create target table, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE AT03_V001_BANCARIBE;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO AT03_V001_BANCARIBE (CUENTA, SALDO, OFICINA, CONSTANTE)
SELECT CUENTA, SALDO, 'V001', '1'
FROM (SELECT AT03_OFICINA_FINAL.CUENTA AS CUENTA, SUM(AT03_OFICINA_FINAL.SALDO) AS SALDO
      FROM AT03_OFICINA_FINAL
      GROUP BY AT03_OFICINA_FINAL.CUENTA) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def ATS_TT_AT03(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE ATS_TH_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM ats_th_at03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO ATS_TH_AT03 (OFICINA, CODIGO_CONTABLE, SALDO, CONSECUTIVO)
SELECT
    OFICINA,
    CODIGO_CONTABLE,
    SALDO,
    CONSECUTIVO
FROM (
    SELECT
        OFICINA,
        SUBSTR(CUENTA, 1, 10) AS CODIGO_CONTABLE,
        SALDO,
        CONSTANTE AS CONSECUTIVO
    FROM AT03_OFICINA_FINAL
    WHERE SALDO <> 0
    UNION
    SELECT
        OFICINA,
        SUBSTR(CUENTA, 1, 10) AS CODIGO_CONTABLE,
        SALDO,
        CONSTANTE AS CONSECUTIVO
    FROM AT03_V001_BANCARIBE
    WHERE SALDO <> 0
) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def IT_CB_HIST_SALDO_ODS_BANGENTE_AT03(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM
    SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 2)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "%COL_PRF0CB_HIST_SALDO_AT" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM "CB_HIST_SALDO_AT";'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME
      FROM %COL_PRF0CB_HIST_SALDO_AT) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


def IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03(**kwargs):
    hook = PostgresHook(postgres_conn_id='nombre_conexion')

    # Load data
    sql_query_coltxt = f'''SELECT
    CB_HIST_SALDO.hi_empresa AS C1_HI_EMPRESA,
    LTRIM(RTRIM(CB_HIST_SALDO.hi_cuenta)) AS C2_HI_CUENTA,
    CB_HIST_SALDO.hi_oficina AS C3_HI_OFICINA,
    CB_HIST_SALDO.hi_area AS C4_HI_AREA,
    CB_HIST_SALDO.hi_corte AS C5_HI_CORTE,
    CB_HIST_SALDO.hi_periodo AS C6_HI_PERIODO,
    CB_HIST_SALDO.hi_saldo AS C7_HI_SALDO,
    CB_HIST_SALDO.hi_saldo_me AS C8_HI_SALDO_ME
FROM SOURCE_COB_CONTA_HIS.CB_HIST_SALDO AS CB_HIST_SALDO
WHERE
    (1 = 1)
    AND (CB_HIST_SALDO.hi_periodo = {AT03_Max_Periodo})
    AND (CB_HIST_SALDO.hi_empresa = 1)
    AND (CB_HIST_SALDO.hi_corte = {AT03_Max_Corte});'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_coltxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Load data
    sql_query_deftxt = f'''INSERT INTO "W"."%COL_PRF0CB_HIST_SALDO_AT03" (C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME) VALUES (:C1_HI_EMPRESA, :C2_HI_CUENTA, :C3_HI_OFICINA, :C4_HI_AREA, :C5_HI_CORTE, :C6_HI_PERIODO, :C7_HI_SALDO, :C8_HI_SALDO_ME);'''
    logger.info("Accion a ejecutarse: Load data") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Load data, ejecutada exitosamente") 

    # Truncate target table
    sql_query_deftxt = f'''TRUNCATE TABLE CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Truncate target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Truncate target table, ejecutada exitosamente") 

    # Delete target table
    sql_query_deftxt = f'''DELETE FROM CB_HIST_SALDO_AT03;'''
    logger.info("Accion a ejecutarse: Delete target table") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Delete target table, ejecutada exitosamente") 

    # Insert new rows
    sql_query_deftxt = f'''INSERT INTO CB_HIST_SALDO_AT03 (HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, HI_FECHA_INI, HI_FECHA_FIN)
SELECT HI_EMPRESA, HI_CUENTA, HI_OFICINA, HI_AREA, HI_CORTE, HI_PERIODO, HI_SALDO, HI_SALDO_ME, NOW(), NOW()
FROM (SELECT C1_HI_EMPRESA, C2_HI_CUENTA, C3_HI_OFICINA, C4_HI_AREA, C5_HI_CORTE, C6_HI_PERIODO, C7_HI_SALDO, C8_HI_SALDO_ME
      FROM %COL_PRF0CB_HIST_SALDO_AT03) AS ODI_GET_FROM;'''
    logger.info("Accion a ejecutarse: Insert new rows") 
    hook.run(sql_query_deftxt)
    logger.info("Accion: Insert new rows, ejecutada exitosamente") 


###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='AT03',
          default_args=default_args,
          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

AT03_AGRUPACION_CONTABLE_BANCARIBE_task = PythonOperator(
    task_id='AT03_AGRUPACION_CONTABLE_BANCARIBE_task',
    python_callable=AT03_AGRUPACION_CONTABLE_BANCARIBE,
    dag=dag
)

AT_AGRUPACION_V001_BANCARIBE_task = PythonOperator(
    task_id='AT_AGRUPACION_V001_BANCARIBE_task',
    python_callable=AT_AGRUPACION_V001_BANCARIBE,
    dag=dag
)

IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_task = PythonOperator(
    task_id='IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_task',
    python_callable=IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03,
    dag=dag
)

IT_CB_HIST_SALDO_ODS_BANGENTE_AT03_task = PythonOperator(
    task_id='IT_CB_HIST_SALDO_ODS_BANGENTE_AT03_task',
    python_callable=IT_CB_HIST_SALDO_ODS_BANGENTE_AT03,
    dag=dag
)

AT03_Max_Periodo_task = PythonOperator(
    task_id='AT03_Max_Periodo_task',
    python_callable=AT03_Max_Periodo,
    dag=dag
)

AT03_Max_Corte_task = PythonOperator(
    task_id='AT03_Max_Corte_task',
    python_callable=AT03_Max_Corte,
    dag=dag
)

IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task = PythonOperator(
    task_id='IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task',
    python_callable=IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS,
    dag=dag
)

AT03_AGRUPACION_CONTABLE_BANCARIBE_task = PythonOperator(
    task_id='AT03_AGRUPACION_CONTABLE_BANCARIBE_task',
    python_callable=AT03_AGRUPACION_CONTABLE_BANCARIBE,
    dag=dag
)

Actualizar_cuenta_formato_Sudeban_task = PythonOperator(
    task_id='Actualizar_cuenta_formato_Sudeban_task',
    python_callable=Actualizar_cuenta_formato_Sudeban,
    dag=dag
)

AT03_AGRUPACION_OFICINAS_BANCARIBE_task = PythonOperator(
    task_id='AT03_AGRUPACION_OFICINAS_BANCARIBE_task',
    python_callable=AT03_AGRUPACION_OFICINAS_BANCARIBE,
    dag=dag
)

AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task = PythonOperator(
    task_id='AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task',
    python_callable=AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA,
    dag=dag
)

AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task = PythonOperator(
    task_id='AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task',
    python_callable=AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL,
    dag=dag
)

ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task = PythonOperator(
    task_id='ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task',
    python_callable=ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO,
    dag=dag
)

AT_AGRUPACION_V001_BANCARIBE_task = PythonOperator(
    task_id='AT_AGRUPACION_V001_BANCARIBE_task',
    python_callable=AT_AGRUPACION_V001_BANCARIBE,
    dag=dag
)

ATS_TT_AT03_task = PythonOperator(
    task_id='ATS_TT_AT03_task',
    python_callable=ATS_TT_AT03,
    dag=dag
)

IT_CB_HIST_SALDO_ODS_BANGENTE_AT03_task = PythonOperator(
    task_id='IT_CB_HIST_SALDO_ODS_BANGENTE_AT03_task',
    python_callable=IT_CB_HIST_SALDO_ODS_BANGENTE_AT03,
    dag=dag
)

IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_task = PythonOperator(
    task_id='IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_task',
    python_callable=IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT03_Max_Periodo_task >> AT03_Max_Corte_task
AT03_Max_Corte_task >> IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task
IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task >> AT03_AGRUPACION_CONTABLE_BANCARIBE_task
AT03_AGRUPACION_CONTABLE_BANCARIBE_task >> Actualizar_cuenta_formato_Sudeban_task
Actualizar_cuenta_formato_Sudeban_task >> AT03_AGRUPACION_OFICINAS_BANCARIBE_task
AT03_AGRUPACION_OFICINAS_BANCARIBE_task >> AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task
AT03_AGRUPACION_OFICINAS_BANCARIBE_SOBREGIRADA_task >> AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task
AT03_AGRUPACION_OFICINAS_BANCARIBE_FINAL_task >> ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task
ACTUALIZACION_DE_LA_CUENTA_0147_POR_SOBREGIRO_task >> AT_AGRUPACION_V001_BANCARIBE_task
AT_AGRUPACION_V001_BANCARIBE_task >> ATS_TT_AT03_task
IT_CB_HIST_SALDO_ODS_BANGENTE_AT03_task >> IT_CB_HIST_SALDO_ODS_BANCARIBE_AT03_ODS_task
