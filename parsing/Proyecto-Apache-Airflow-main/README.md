# DIAGRAMA DE FLUJO DEL PROCESO DE MIGRACIÓN DE ODI A APACHE AIRFLOW

`https://drive.google.com/file/d/1Om-C420ELN0UIyZsQD-8eqAaffDvwjsa/view?usp=sharing`

Ingresar al link >> Abrir con draw.io

####################################################################################
####################################################################################

# PASOS PARA EJECUTAR EL PARSER

## Secuencia de ejecución del parser: 
## XML >> JSON (ORIGINAL) >> JSON (LIMPIO) >> DAG

- Paso 0:

Extraer cada `PAQUETE` que contenga el ETL en ODI. (Nota: no extraer la carpeta, sino cada paquete) 

####################################################################################

- Paso 1: 

En los XML, verificar y eliminar los objetos `SnpScenStep` junto con sus `SnpScenTask` que estén duplicados (en caso de estarlo). Siempre el primer objeto `SnpScenStep` que debe aparecer en el archivo es aquel cuyo field `Nno` es igual a 0. En caso de que el primer `SnpScenStep` en el archivo tenga el `Nno` en 1 debe ser eliminado, hasta conseguir el objeto `SnpScenStep` con `Nno` igual a 0. **Es importante hacer esta eliminación desde el XML, no desde el JSON.**  Luego se hace el paso 2.

####################################################################################

- Paso 2:

Ejecutar en la terminal el archivo `extraer_info_xml_elementos.py` que realiza el parser del XML a un archivo JSON.

**Uso:** 

En la carpeta llamada etls ejecutar: `python3 <ruta_del_script.py> <ruta_del_archivo_xml> <nombre_del_archivo_json>`

Ejemplo de uso: `python3 procesamientoXML/parseo/extraer_info_xml_elementos.py XMLs/AT/AT26/PACKAGE_AT26_TO_FILE.xml JSON/AT/AT26/PACKAGE_AT26_TO_FILE.json`

####################################################################################

- Paso 3:

Ejecutar en la terminal el archivo `filtrar_tareas_json.py` que limpia las consultas realizadas sobre las tablas temporales de ODI.

**Uso:**

En la carpeta llamada etls ejecutar: `python3 <ruta_del_script.py> <ruta_del_archivo_json_original> <nombre_del_archivo_json_limpio>`

Ejemplo de uso: `python3 procesamientoXML/parseo/filtrar_tareas_json.py JSON/AT/AT26/PACKAGE_AT26_TO_FILE.json JSON/AT/AT26/JSON_FILTRADOS/FILTRADO_PACKAGE_AT26_TO_FILE.json`

####################################################################################

- Paso 4:

Establecer variable de entorno: `AMBIENTE=produccion o AMBIENTE=desarrollo`

Si AMBIENTE=produccion, el generador de dags utiliza la api paga del agente traductor.

Si AMBIENTE=desarrollo, el generador de dags utiliza la api gratis del agente traductor.

**Uso:** 

En linux:

`export AMBIENTE=produccion`

Comprobar valor de la variable de entorno: `echo $AMBIENTE`

En Windows:

`set AMBIENTE=produccion`

Comprobar valor de la variable de entorno: `echo %AMBIENTE%`

####################################################################################

- Paso 5:

Ejecutar en la terminal el archivo `generador_de_dags.py` que genera el dag correspondiente al JSON y lo coloca en un archivo .py de salida (Dag de Airflow).

**Uso:** 

En la carpeta llamada etls ejecutar: `python3 generador_de_dags.py <ruta_al_json> <ruta_al_output_py>`

Ejemplo de uso: `python3 procesamientoXML/parseo/generador_de_dags.py JSON/AT/AT26/JSON_FILTRADOS/FILTRADO_PACKAGE_AT26_TO_FILE.json DAGs/AT/AT26/DAG_AT26_TO_FILE.py`

####################################################################################

- Paso 6:

Revisión y depuración del archivo DAG resultante. Recomendable utilizar la herramienta `Cursor` para hacer las siguientes depuraciones automáticamente:

(En el apartado `MODIFICACIONES MANUALES LUEGO DE LA EJECUCIÓN DEL PARSER` más abajo del readme.)

Descargar programa ejecutable .exe en: https://www.cursor.com/

####################################################################################

# Notas adicionales (Importante)

Es importante colocar la siguiente sintaxis en los nombres de los archivos 

- XML:

<AT#>_<tipo_paquete>.xml

Ejemplo: `AT26_PRINCIPAL.xml`

- JSON:

<AT#>_<tipo_paquete>.json

Ejemplo: `AT26_PRINCIPAL.json`

- .PY (DAGS):

DAG _ <AT#>_<tipo_paquete>.py

Ejemplo: `DAG_AT26_PRINCIPAL.py`


### <tipo_paquete> puede ser: 
- _PRINCIPAL 
- _TO_FILE
- _TO_FILE_ERROR

####################################################################################

# MODIFICACIONES MANUALES LUEGO DE LA EJECUCIÓN DEL PARSER

- Para los datos que origen que provengan de archivos CSV, agregar su función correspondiente `IMPORTAR_INSUMO` junto con su operador y paso en la secuencia de ejecución.
- Para cada paso de transformación del DAG, colocar la consulta del `SELECT` debajo del `INSERT INTO` generando una sola consulta ya que estas vienen separadas (Esto solo se aplica para algunos AT). Dentro de cada paso de transformación, ordenar la consulta `INSERT INTO / SELECT` luego de la consulta `TRUNCATE TABLE`. El orden de las consultas en cada paso: `CREATE TABLE -> TRUNCATE TABLE -> INSERT INTO`
- Para los pasos del DataCheck, juntar las consultas de cada `SELECT/FROM` para obtener, debajo del `INSERT INTO`, una sola consulta `SELECT/FROM` agrupando las condiciones del `WHERE` con `AND`.
- En cada paso, ajustar el nombre de las conexiones en el PostgresHook. `hook = PostgresHook(postgres_conn_id='nombre_conexion')`
- En el operador `wait_for_file`, colocar la ruta del csv de origen.
- En el operador `holiday_sensor`, colocar el tiempo de espera. (10 seg para la prueba)
- En cada paso, verificar y eliminar las consultas SQL generadas por ODI por defecto que no son utilizadas o requeridas en el ETL.
- En los pasos `GENERAR_REPORTE_TXT`, colocar la ruta en donde se guardará el archivo TXT. En la línea de código: `registros = hook.get_records("SELECT * FROM tabla_final;")` colocar el nombre de la tabla final a ser exportada como archivo TXT.
- En el operador de `Enviar_Email`, colocar el correo correspondiente a usar.
- En el caso de que el ETL tenga ramificaciones, colocar los parametros `trigger_rule=TriggerRule.ONE_FAILED` y `trigger_rule=TriggerRule.ALL_DONE` dentro de los operadores de las tareas que corresponda.

`trigger_rule=TriggerRule.ONE_FAILED` para el paso que activa la ramificacion.
`trigger_rule=TriggerRule.ALL_DONE` para el ultimo paso de la secuencia.


####################################################################################
####################################################################################

# PASOS PARA EJECUTAR EL PARSER DE EXTRACCIÓN DE DATOS DE SYBASE -> POSTGRES

- Aplicar paso 0 y 1 del parser anterior

####################################################################################

- Paso 2:

Ejecutar en la terminal el archivo `extraer_info_xml_elementos_extraccion.py` que realiza el parser del XML a un archivo JSON.

**Uso:** 

En la carpeta llamada etls_extraccion ejecutar: `python3 <ruta_del_script.py> <ruta_del_archivo_xml> <nombre_del_archivo_json>`

Ejemplo de uso: `python3 procesamientoXML/parseo_extraccion/extraer_info_xml_elementos_extraccion.py XMLs/AT/AT03/CB_RELOFI_COB_CONTA.xml JSON/AT/AT03/CB_RELOFI_COB_CONTA.json`

####################################################################################

- Paso 3:

Ejecutar en la terminal el archivo `generador_de_dags_extraccion.py` que genera el dag correspondiente al JSON y lo coloca en un archivo .py de salida (Dag de Airflow).

**Uso:** 

En la carpeta llamada etls_extraccion ejecutar: `python3 <ruta_del_script.py> <ruta_al_json> <ruta_al_output_py>`

Ejemplo de uso: `python3 procesamientoXML/parseo_extraccion/generador_de_dags_extraccion.py JSON/AT/AT03/CB_RELOFI_COB_CONTA.json DAGs/AT/AT03/CB_RELOFI_COB_CONTA.py`

####################################################################################

- Paso 4:

Revisión y depuración del archivo de salida.

- En cada paso, verificar y eliminar las consultas SQL generadas por ODI por defecto que no son utilizadas o requeridas en el ETL.


####################################################################################
####################################################################################


# AUTOMATIZACIÓN EN LA EJECUCIÓN DE LOS COMANDOS DE CADA PARSER

Recorre todos los XML que encuentre en la carpeta XMLs generando por cada uno sus archivos JSON y DAGs correspondientes, luego los guarda en sus carpetas/subcarpetas respectivas de JSON y DAGs.

Dentro de la carpeta llamada etls ejecutar en la terminal: `python3 procesar_parseo.py`

o

Dentro de la carpeta llamada etls_extraccion ejecutar en la terminal: `python3 procesar_parseo_extraccion.py`