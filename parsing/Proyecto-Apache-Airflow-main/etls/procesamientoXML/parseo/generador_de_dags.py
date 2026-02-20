import os
import json
import sys
from parser_sql_prod import parser_sql_prod
from parser_sql_dev import parser_sql_dev


# AGENTE PARA TRANSFORMAR ORACLE SQL A POSTGRES SQL
# Leemos la variable de entorno AMBIENTE; por defecto 'desarrollo'
AMBIENTE = os.getenv('AMBIENTE', 'desarrollo').lower()

# Asignamos la funcion parseo_sql al parser adecuado
if AMBIENTE == 'produccion':
	parseo_sql = parser_sql_prod
elif AMBIENTE == 'desarrollo':
	parseo_sql = parser_sql_dev
else:
	raise ValueError(f"AMBIENTE desconocido: {AMBIENTE!r}. Usa 'desarrollo' o 'produccion'.")


# FUNCION PARA GENERAR EL CODIGO DEL DAG A PARTIR DEL JSON DE UN PACKAGE
def generate_dag_code_package(json_file):
	# Cargamos el JSON
	with open(json_file, 'r') as f:
		data = json.load(f)
	
	# Extraemos el paquete y la lista de pasos
	package_list = data.get("SnpPackage", [])
	package = package_list[0]
	dag_id = package.get("PackName", "")
	scensteps = data.get("SnpScenStep", [])
	
	# Iniciamos la lista de lineas de codigo
	code_lines = []
	
	# ENCABEZADO: IMPORTS Y DEFINICION DEL AGENTE
	code_lines.append("from airflow import DAG")
	code_lines.append("from airflow.operators.python import PythonOperator, BranchPythonOperator")
	code_lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
	code_lines.append("from airflow.models import Variable")
	code_lines.append("from airflow.operators.trigger_dagrun import TriggerDagRunOperator")
	code_lines.append("from airflow.sensors.base import BaseSensorOperator")
	code_lines.append("from airflow.operators.email import EmailOperator")
	code_lines.append("from airflow.providers.google.cloud.hooks.gcs import GCSHook")
	code_lines.append("from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor ")
	code_lines.append("from airflow.utils.trigger_rule import TriggerRule")
	code_lines.append("from datetime import datetime, timedelta")
	code_lines.append("import time")
	code_lines.append("import tempfile")
	code_lines.append("import os")
	code_lines.append("import logging")
	code_lines.append("from decimal import Decimal")
	code_lines.append("import json")
	code_lines.append("")
	code_lines.append("logger = logging.getLogger(__name__)")
	code_lines.append("")
	code_lines.append("def serialize_value(value):")
	code_lines.append("    \"\"\"")
	code_lines.append("    Helper para serializar valores de PostgreSQL a JSON.")
	code_lines.append("    Convierte Decimal, date, datetime a string para preservar precision.")
	code_lines.append("    \"\"\"")
	code_lines.append("    if value is None:")
	code_lines.append("        return \"\"")
	code_lines.append("    if isinstance(value, str):")
	code_lines.append("        return value  # Ya es string")
	code_lines.append("    if isinstance(value, datetime):")
	code_lines.append("        return value.isoformat()  # Formato ISO 8601")
	code_lines.append("    if isinstance(value, (int, float, Decimal)):")
	code_lines.append("        return str(value)  # Tipos numericos")
	code_lines.append("    return json.dumps(value)  # Otros tipos")
	code_lines.append("")
	code_lines.append("def get_variable(key, default_var=\"\"):")
	code_lines.append("    \"\"\"")
	code_lines.append("    Helper para obtener variables de Airflow y deserializarlas.")
	code_lines.append("    Retorna el valor como string (compatible con SQL queries).")
	code_lines.append("    ")
	code_lines.append("    Para conversiones especificas:")
	code_lines.append("    - int: int(get_variable('key'))")
	code_lines.append("    - Decimal: Decimal(get_variable('key'))")
	code_lines.append("    - datetime: datetime.fromisoformat(get_variable('key'))")
	code_lines.append("    \"\"\"")
	code_lines.append("    raw = Variable.get(key, default_var=default_var)")
	code_lines.append("    try:")
	code_lines.append("        return json.loads(raw)")
	code_lines.append("    except Exception:")
	code_lines.append("        return raw")
	code_lines.append("")
	code_lines.append("### FUNCIONES DE CADA TAREA ###")
	code_lines.append("")

	# Funcion de HolidayCheckSensor:
	if output_py.endswith("PRINCIPAL.py"):
		code_lines.append("class HolidayCheckSensor(BaseSensorOperator):")
		code_lines.append("    \"\"\"")
		code_lines.append("    Sensor que espera hasta que el dia actual NO sea feriado.")
		code_lines.append("    La condicion se determina ejecutando una consulta SQL en la base de datos.")
		code_lines.append("    \"\"\"")
		code_lines.append("    def __init__(self, postgres_conn_id, *args, **kwargs):")
		code_lines.append("        super(HolidayCheckSensor, self).__init__(*args, **kwargs)")
		code_lines.append("        self.postgres_conn_id = postgres_conn_id")
		code_lines.append("")
		code_lines.append("    def poke(self, context):")
		code_lines.append("        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)")
		code_lines.append("        sql_query = \"\"\"")
		code_lines.append("            SELECT CASE ")
		code_lines.append("                       WHEN to_char(CURRENT_DATE, 'dd/mm/yy') IN (")
		code_lines.append("                           SELECT to_char(df_fecha, 'dd/mm/yy') ")
		code_lines.append("                           FROM ods.cl_dias_feriados ")
		code_lines.append("                           WHERE SUBSTRING(df_year FROM 3 FOR 2) = SUBSTRING(to_char(CURRENT_DATE, 'dd/mm/yy') FROM 7 FOR 2)")
		code_lines.append("                       )")
		code_lines.append("                       THEN 1 ")
		code_lines.append("                       ELSE 0 ")
		code_lines.append("                   END AS status;")
		code_lines.append("        \"\"\"")
		code_lines.append("        records = hook.get_records(sql_query)")
		code_lines.append("        # Suponiendo que la consulta retorna 1 fila, 1 columna:")
		code_lines.append("        status = records[0][0] if records else 0")
		code_lines.append("        self.log.info(\"Valor de status (1=feriado, 0=no feriado): %s\", status)")
		code_lines.append("        # Esperamos que status sea 0 para continuar con el flujo normal")
		code_lines.append("        return status == 0")
		code_lines.append("")

	# Para cada paso en el JSON definimos una funcion con el codigo de sus tareas
	for step in scensteps:
		# Obtenemos el nombre del paso y lo limpiamos quitando espacios
		step_name = step.get("StepName", "").strip().replace(" ", "_")
		step_type = step.get("StepType", "").strip()

		# Obtenemos las tareas que pertenezcan a este paso 
		tasks = step.get("SnpScenTask", [])
		
		# Si el nombre del paso termina con "BG", lo saltamos (no generamos la funcion)
		if step_name.endswith("BG") or step_name.endswith("Bg") or step_name in ["AT_DIAS_FERIADOS", "AT_DIAS_HABILES"]:
			continue

		# FUNCIONES DE CADA TAREA  

		# Si el paso es de tipo SE no usa funcion
		if step_type == "SE":
			pass
		else:
			code_lines.append(f"def {step_name}(**kwargs):")

		if not tasks:
			code_lines.append("    pass")
		else:
			# Marcador para saber si ya insertamos la línea del hook
			hook_inserted = False

			# Iteramos sobre las tareas definidas para el paso
			for task in tasks:

				task_type = task.get("TaskName1", "")
				
				if task_type == "Variable":
					snp_var_list = task.get("SnpVar", [])
					if snp_var_list:
						snp_var_obj = snp_var_list[0]
						var_name = snp_var_obj.get("VarName", "")

					if step_type in ["VD", "VS"]:  # Variables del tipo set o declare (guardan un valor fijo)
						value = snp_var_obj.get("DefV", "")
						code_lines.append(f"    value = '{value}'")
						code_lines.append(f"    Variable.set('{var_name}', serialize_value(value))")

					elif step_type == "V":  # Variables del tipo refresh (guardan el valor de un query)
						sql = task.get("DefTxt", "")
						code_lines.append("    hook = PostgresHook(postgres_conn_id='nombre_conexion')")
						code_lines.append("")
						# Usamos triple comilla para preservar el formato del SQL
						# Convertir el SQL de Oracle a Postgres
						parsed_sql = parseo_sql(sql)
						code_lines.append(f"    sql_query = f'''{parsed_sql}'''")
						code_lines.append("    result = hook.get_records(sql_query)")
						code_lines.append("")
						code_lines.append(f"    Variable.set('{var_name}', serialize_value(result[0][0]))")
				
				elif task_type != "Variable" and task_type != "Oracle Data Integrator Command": # Aqui entran los SnpScenTask del tipo Integration, Loading, Control y Procedure.
					
					# aqui es donde se coloca "hook = PostgresHook" una sola vez por funcion
					if not hook_inserted:
						code_lines.append("    hook = PostgresHook(postgres_conn_id='nombre_conexion')")
						code_lines.append("")
						hook_inserted = True

					# Primero se revisa el campo ColTxt, en caso de que contenga código SQL
					sql_query_coltxt = task.get("ColTxt", "")
					if sql_query_coltxt and sql_query_coltxt.lower() != "null":
						# Convertir el SQL de Oracle a Postgres
						parsed_sql_coltxt = parseo_sql(sql_query_coltxt)
						code_lines.append(f"    # {task.get('TaskName3', '')}")
						code_lines.append(f"    sql_query_coltxt = f'''{parsed_sql_coltxt}'''")
						code_lines.append(f"""    logger.info("Accion a ejecutarse: {task.get('TaskName3', '')}") """)
						code_lines.append("    hook.run(sql_query_coltxt)")
						code_lines.append(f"""    logger.info("Accion: {task.get('TaskName3', '')}, ejecutada exitosamente") """)
						code_lines.append("")

					# Luego se procesa el campo DefTxt
					sql_query_deftxt = task.get("DefTxt", "")
					parsed_sql_deftxt = parseo_sql(sql_query_deftxt)
					code_lines.append(f"    # {task.get('TaskName3', '')}")
					code_lines.append(f"    sql_query_deftxt = f'''{parsed_sql_deftxt}'''")
					code_lines.append(f"""    logger.info("Accion a ejecutarse: {task.get('TaskName3', '')}") """)
					code_lines.append("    hook.run(sql_query_deftxt)")
					code_lines.append(f"""    logger.info("Accion: {task.get('TaskName3', '')}, ejecutada exitosamente") """)
					code_lines.append("")
				  
		code_lines.append("")

	# Funciones de generacion de txt:
	if output_py.endswith("FILE.py"):
		code_lines.append("def GENERAR_REPORTE_TXT(**kwargs):")
		code_lines.append("    # Conexion a la bd ")
		code_lines.append("    hook = PostgresHook(postgres_conn_id='nombre_conexion')")
		code_lines.append("    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook")
		code_lines.append("")
		code_lines.append("    # Recuperar variables")
		code_lines.append("    FileAT = get_variable('FileAT_at#')")
		code_lines.append("    FileCodSupervisado = get_variable('FileCodSupervisado')")
		code_lines.append("    FechaFile = get_variable('FechaFile')")
		code_lines.append("")
		code_lines.append("    # Obtener registros")
		code_lines.append("    logger.info(\"Obteniendo registros de la base de datos...\")")
		code_lines.append("    registros = hook.get_records(\"SELECT * FROM tabla_final;\")")
		code_lines.append("    logger.info(f\"Se obtuvieron {len(registros)} registros.\")")
		code_lines.append("")
		code_lines.append("    # Rutas del archivo de salida en GCS y local")
		code_lines.append("    gcs_bucket = 'airflow-dags-data'")
		code_lines.append("    gcs_object_path = f\"data/AT#/SALIDAS/{FileAT}{FileCodSupervisado}{FechaFile}.txt\"")
		code_lines.append("    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal")
		code_lines.append("    local_file_path = os.path.join(temp_dir, f\"{FileAT}{FileCodSupervisado}{FechaFile}.txt\") # Ruta del archivo temporal local")
		code_lines.append("")
		code_lines.append("    try:")
		code_lines.append("        logger.info(f\"Escribiendo datos en archivo temporal local: {local_file_path}\")")
		code_lines.append("        # Escribir los registros en el archivo de texto temporal local")
		code_lines.append("        with open(local_file_path, 'w', encoding='utf-8') as f:")
		code_lines.append("            for row in registros:")
		code_lines.append("                linea = \"~\".join(str(v) if v is not None else \"\" for v in row)")
		code_lines.append("                f.write(linea + \"\\n\")")
		code_lines.append("")
		code_lines.append("        logger.info(f\"Archivo temporal local generado correctamente. Subiendo a GCS: gs://{gcs_bucket}/{gcs_object_path}\")")
		code_lines.append("        # Subir el archivo temporal local a GCS")
		code_lines.append("        gcs_hook.upload(")
		code_lines.append("            bucket_name=gcs_bucket,")
		code_lines.append("            object_name=gcs_object_path,")
		code_lines.append("            filename=local_file_path,")
		code_lines.append("        )")
		code_lines.append("        logger.info(\"Archivo subido correctamente.\")")
		code_lines.append("")
		code_lines.append("    except Exception as e:")
		code_lines.append("        logger.error(f\"Error durante la generacion o subida del archivo: {str(e)}\")")
		code_lines.append("        import traceback")
		code_lines.append("        logger.error(\"Traceback completo:\\n\" + traceback.format_exc())")
		code_lines.append("        raise")
		code_lines.append("")
		code_lines.append("    finally:")
		code_lines.append("        # Limpieza: Asegurarse de eliminar el archivo temporal y el directorio")
		code_lines.append("        if os.path.exists(local_file_path):")
		code_lines.append("            os.remove(local_file_path)")
		code_lines.append("            logger.info(f\"Archivo temporal eliminado: {local_file_path}\")")
		code_lines.append("        if os.path.exists(temp_dir):")
		code_lines.append("            os.rmdir(temp_dir)")
		code_lines.append("            logger.info(f\"Directorio temporal eliminado: {temp_dir}\")")
		code_lines.append("")

	elif output_py.endswith("ERROR.py"):
		code_lines.append("def GENERAR_REPORTE_TXT_ERROR(**kwargs):")
		code_lines.append("    # Conexion a la bd ")
		code_lines.append("    hook = PostgresHook(postgres_conn_id='nombre_conexion')")
		code_lines.append("    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Inicializar GCSHook")
		code_lines.append("")
		code_lines.append("    # Recuperar variables")
		code_lines.append("    FileAT = get_variable('FileAT_at#')")
		code_lines.append("    FechaFile = get_variable('FechaFile')")
		code_lines.append("    FileName_Error = get_variable('FileName_Error')")
		code_lines.append("")
		code_lines.append("    # Obtener registros")
		code_lines.append("    logger.info(\"Obteniendo registros de la base de datos...\")")
		code_lines.append("    registros = hook.get_records(\"SELECT * FROM tabla_final;\")")
		code_lines.append("    logger.info(f\"Se obtuvieron {len(registros)} registros.\")")
		code_lines.append("")
		code_lines.append("    # Rutas del archivo de salida en GCS y local")
		code_lines.append("    gcs_bucket = 'airflow-dags-data'")
		code_lines.append("    gcs_object_path = f\"data/AT#/SALIDAS/{FileName_Error}{FileAT}{FechaFile}.txt\"")
		code_lines.append("    temp_dir = tempfile.mkdtemp() # Crea un directorio temporal")
		code_lines.append("    local_file_path = os.path.join(temp_dir, f\"{FileName_Error}{FileAT}{FechaFile}.txt\") # Ruta del archivo temporal local")
		code_lines.append("")
		code_lines.append("    try:")
		code_lines.append("        logger.info(f\"Escribiendo datos en archivo temporal local: {local_file_path}\")")
		code_lines.append("        # Escribir los registros en el archivo de texto temporal local")
		code_lines.append("        with open(local_file_path, 'w', encoding='utf-8') as f:")
		code_lines.append("            for row in registros:")
		code_lines.append("                linea = \"~\".join(str(v) if v is not None else \"\" for v in row)")
		code_lines.append("                f.write(linea + \"\\n\")")
		code_lines.append("")
		code_lines.append("        logger.info(f\"Archivo temporal local generado correctamente. Subiendo a GCS: gs://{gcs_bucket}/{gcs_object_path}\")")
		code_lines.append("        # Subir el archivo temporal local a GCS")
		code_lines.append("        gcs_hook.upload(")
		code_lines.append("            bucket_name=gcs_bucket,")
		code_lines.append("            object_name=gcs_object_path,")
		code_lines.append("            filename=local_file_path,")
		code_lines.append("        )")
		code_lines.append("        logger.info(\"Archivo subido correctamente.\")")
		code_lines.append("")
		code_lines.append("    except Exception as e:")
		code_lines.append("        logger.error(f\"Error durante la generacion o subida del archivo: {str(e)}\")")
		code_lines.append("        import traceback")
		code_lines.append("        logger.error(\"Traceback completo:\\n\" + traceback.format_exc())")
		code_lines.append("        raise")
		code_lines.append("")
		code_lines.append("    finally:")
		code_lines.append("        # Limpieza: Asegurarse de eliminar el archivo temporal y el directorio")
		code_lines.append("        if os.path.exists(local_file_path):")
		code_lines.append("            os.remove(local_file_path)")
		code_lines.append("            logger.info(f\"Archivo temporal eliminado: {local_file_path}\")")
		code_lines.append("        if os.path.exists(temp_dir):")
		code_lines.append("            os.rmdir(temp_dir)")
		code_lines.append("            logger.info(f\"Directorio temporal eliminado: {temp_dir}\")")
		code_lines.append("")

	
	code_lines.append("###### DEFINICION DEL DAG ###### ")
	code_lines.append("")
	code_lines.append("default_args = {")
	code_lines.append("    'owner': 'airflow',")
	code_lines.append("    'start_date': datetime(2024, 1, 1),")
	code_lines.append("    'retries': 1,")
	code_lines.append("    'retry_delay': timedelta(minutes=5)")
	code_lines.append("}")
	code_lines.append("")
	code_lines.append(f"dag = DAG(dag_id='{dag_id}',")
	code_lines.append("          default_args=default_args,")
	code_lines.append("          schedule=None, # Aqui se programa cada cuanto ejecutar el DAG")
	code_lines.append("          catchup=False)")
	code_lines.append("")

	# Operadores de FileSensor y HolidayCheckSensor:
	if output_py.endswith("PRINCIPAL.py"):
		code_lines.append("wait_for_file = GCSObjectExistenceSensor(                  # En la secuencia de ejecucion, colocar este operador en el orden que convenga (Agregar un operador por cada insumo que use el AT)")
		code_lines.append("    task_id='wait_for_file',")
		code_lines.append("    bucket='airflow-dags-data', ")
		code_lines.append("    object='data/AT#/INSUMOS/nombre_insumo.csv',           # Colocar nombre del insumo") 
		code_lines.append("    poke_interval=10,                                      # Intervalo en segundos para verificar")
		code_lines.append("    timeout=60 * 10,                                       # Timeout en segundos (10 minutos para la prueba)")
		code_lines.append("    dag=dag")
		code_lines.append(")")
		code_lines.append("")
		code_lines.append("holiday_sensor = HolidayCheckSensor(     # En la secuencia de ejecucion, colocar este operador en el orden que convenga")
		code_lines.append("    task_id='holiday_sensor',")
		code_lines.append("    postgres_conn_id='nombre_conexion',  # Colocar nombre de la conexion a la bd")
		code_lines.append("    poke_interval=10,                    # Se verifica cada 10 segundos para la prueba (para produccion seria 86400seg para una verificacion diaria)")
		code_lines.append("    dag=dag")
		code_lines.append(")")
		code_lines.append("")


	# Lista para almacenar los nombres de las tareas (solo los que se generan)
	valid_step_names = []

	# Definimos los OPERADORES para cada función creada
	for step in scensteps:
		# Obtenemos el nombre del paso y lo limpiamos quitando espacios
		step_name = step.get("StepName", "").strip().replace(" ", "_")
		step_type = step.get("StepType", "").strip()
		tasks = step.get("SnpScenTask", [])

		# Si el nombre del paso termina en "_BG", lo saltamos. Simplemente continua con el siguiente paso, sin generar operador.
		if step_name.endswith("BG") or step_name.endswith("Bg") or step_name in ["AT_DIAS_FERIADOS", "AT_DIAS_HABILES"]:
			continue

		# Revisamos si en las tareas de este step existe un DefTxt que empiece con "OdiStartScen"
		found_odi_start_scen = False
		for t in tasks:
			def_txt_value = t.get("DefTxt", "")
			if def_txt_value.startswith("OdiStartScen"):
				found_odi_start_scen = True
				break
		
		if step_type == "SE" and found_odi_start_scen:
			# Usamos TriggerDagRunOperator para pasos con StepType 'SE'
			code_lines.append(f"{step_name}_task = TriggerDagRunOperator(")
			code_lines.append(f"    task_id='{step_name}_task',")
			code_lines.append(f"    trigger_dag_id='{step_name}',  # Acomodar ID del DAG a disparar (Se usa el PackName)")
			code_lines.append("    wait_for_completion=True,")
			code_lines.append("    dag=dag")
			code_lines.append(")")
			code_lines.append("")
			valid_step_names.append(f"{step_name}_task")

		elif step_type == "V" or step_type == "VS" or step_type == "VD" or step_type == "T" or step_type == "CD" or step_type == "F":
			# Para el resto usamos PythonOperator
			code_lines.append(f"{step_name}_task = PythonOperator(")
			code_lines.append(f"    task_id='{step_name}_task',")
			code_lines.append(f"    python_callable={step_name},")
			code_lines.append("    dag=dag")
			code_lines.append(")")
			code_lines.append("")
			valid_step_names.append(f"{step_name}_task")

	# OPERADORES PARA LAS FUNCIONES DE GENERAR TXT Y ENVIO DE CORREO
	if output_py.endswith("FILE.py"):
		code_lines.append("GENERAR_REPORTE_TXT_task = PythonOperator(")
		code_lines.append("    task_id='GENERAR_REPORTE_TXT_task',")
		code_lines.append("    python_callable=GENERAR_REPORTE_TXT,")
		code_lines.append("    dag=dag")
		code_lines.append(")")
		code_lines.append("")
		code_lines.append("Enviar_Email_task = EmailOperator(")
		code_lines.append("    task_id='Enviar_Email_task',")
		code_lines.append("    to='colocar_correo_aqui@gmail.com',          # correo destino")
		code_lines.append("    subject='DAG {{ dag.dag_id }} completado',   # asunto del correo")
		code_lines.append("    html_content=\"\"\"                             ")
		code_lines.append("        <h3>¡Hola!</h3>")
		code_lines.append("        <p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at# }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>")
		code_lines.append("    \"\"\",")
		code_lines.append("    conn_id='email_conn',")
		code_lines.append("    dag=dag")
		code_lines.append(")")

	elif output_py.endswith("ERROR.py"):
		code_lines.append("GENERAR_REPORTE_TXT_ERROR_task = PythonOperator(")
		code_lines.append("    task_id='GENERAR_REPORTE_TXT_ERROR_task',")
		code_lines.append("    python_callable=GENERAR_REPORTE_TXT_ERROR,")
		code_lines.append("    dag=dag")
		code_lines.append(")")

	
	# SECUENCIA DE EJECUCION DE TAREAS (basada en OkNextStep / KoNextStep)
	if valid_step_names:
		code_lines.append("###### SECUENCIA DE EJECUCION ######")

		sequence = " >> ".join(valid_step_names)
		
		if output_py.endswith("PRINCIPAL.py"):
			sequence = "wait_for_file >> holiday_sensor >> " + sequence 
			code_lines.append(sequence)
		elif output_py.endswith("FILE.py"):
			sequence += " >> GENERAR_REPORTE_TXT_task >> Enviar_Email_task"
			code_lines.append(sequence)
		elif output_py.endswith("ERROR.py"):
			sequence += " >> GENERAR_REPORTE_TXT_ERROR_task"
			code_lines.append(sequence)
		else:

			# Crear un mapa Nno → StepName
			nno_to_step = {}
			for step in scensteps:
				nno = str(step.get("Nno", "")).strip()
				step_name = step.get("StepName", "").strip().replace(" ", "_")
				if nno and step_name:
					nno_to_step[nno] = step_name

			# Generar relaciones de ejecución (ok y ko)
			for step in scensteps:
				step_name = step.get("StepName", "").strip().replace(" ", "_")
				if not step_name:
					continue

				current_task = f"{step_name}_task"
				ok_next = str(step.get("OkNextStep", "")).strip()
				ko_next = str(step.get("KoNextStep", "")).strip()

				# Si la tarea existe entre las válidas, agregamos sus relaciones
				if current_task in valid_step_names:

					# Rama ok
					if ok_next and ok_next in nno_to_step:
						next_ok_name = f"{nno_to_step[ok_next]}_task"
						if next_ok_name in valid_step_names:
							code_lines.append(f"{current_task} >> {next_ok_name}")

					# Rama ko
					if ko_next and ko_next in nno_to_step:
						next_ko_name = f"{nno_to_step[ko_next]}_task"
						if next_ko_name in valid_step_names:
							code_lines.append(f"{current_task} >> {next_ko_name}  # Rama KO")

		code_lines.append("")


	return "\n".join(code_lines)

# FUNCION PARA GENERAR EL CODIGO DEL DAG A PARTIR DEL JSON DE UNA INTERFACE
def generate_dag_code_interface(json_file):
	# Cargamos el JSON
	with open(json_file, 'r') as f:
		data = json.load(f)
	
	# Extraemos el nombre de la interface y la lista de pasos
	pop_list = data.get("SnpPop", [])
	pop = pop_list[0]
	dag_id = pop.get("PopName", "")
	scensteps = data.get("SnpScenStep", [])
	
	# Iniciamos la lista de lineas de codigo
	code_lines = []

	# ENCABEZADO: IMPORTS Y DEFINICION DEL AGENTE
	code_lines.append("from airflow import DAG")
	code_lines.append("from airflow.operators.python import PythonOperator, BranchPythonOperator")
	code_lines.append("from airflow.utils.dates import days_ago")
	code_lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
	code_lines.append("from airflow.models import Variable")
	code_lines.append("from airflow.operators.trigger_dagrun import TriggerDagRunOperator")
	code_lines.append("from airflow.sensors.filesystem import FileSensor")
	code_lines.append("from airflow.sensors.base import BaseSensorOperator")
	code_lines.append("from airflow.operators.email import EmailOperator")
	code_lines.append("from airflow.utils.decorators import apply_defaults")
	code_lines.append("from datetime import datetime")
	code_lines.append("import time")
	code_lines.append("")
	code_lines.append("### FUNCIONES DE CADA TAREA ###")
	code_lines.append("")

	# Para cada paso en el JSON definimos una funcion con el codigo de sus tareas
	for step in scensteps:
		# Obtenemos el nombre del paso y lo limpiamos quitando espacios
		step_name = step.get("StepName", "").strip().replace(" ", "_")

		# Obtenemos las tareas que pertenezcan a este paso 
		tasks = step.get("SnpScenTask", [])

		# FUNCIONES DE CADA TAREA 
		code_lines.append(f"def {step_name}(**kwargs):")

		
		if not tasks:
			code_lines.append("    pass")
		else:
			# Marcador para saber si ya insertamos la línea del hook
			hook_inserted = False

			# Iteramos sobre las tareas definidas para el paso
			for task in tasks:

				# aqui es donde se coloca "hook = PostgresHook" una sola vez por funcion
					if not hook_inserted:
						code_lines.append("    hook = PostgresHook(postgres_conn_id='mi_conexion')")
						code_lines.append("")
						hook_inserted = True

					# Primero se revisa el campo ColTxt, en caso de que contenga código SQL
					sql_query_coltxt = task.get("ColTxt", "")
					if sql_query_coltxt and sql_query_coltxt.lower() != "null":
						# Convertir el SQL de Oracle a Postgres
						parsed_sql_coltxt = parseo_sql(sql_query_coltxt)
						code_lines.append(f"    # {task.get('TaskName3', '')}")
						code_lines.append(f"    sql_query_coltxt = f'''{parsed_sql_coltxt}'''")
						code_lines.append("    hook.run(sql_query_coltxt)")
						code_lines.append("")

					# Luego se procesa el campo DefTxt
					sql_query_deftxt = task.get("DefTxt", "")
					parsed_sql_deftxt = parseo_sql(sql_query_deftxt)
					code_lines.append(f"    # {task.get('TaskName3', '')}")
					code_lines.append(f"    sql_query_deftxt = f'''{parsed_sql_deftxt}'''")
					code_lines.append("    hook.run(sql_query_deftxt)")
					code_lines.append("")

		code_lines.append("")


	code_lines.append("###### DEFINICION DEL DAG ###### ")
	code_lines.append("")
	code_lines.append("default_args = {")
	code_lines.append("    'owner': 'airflow',")
	code_lines.append("    'start_date': days_ago(1)")
	code_lines.append("}")
	code_lines.append("")
	code_lines.append(f"dag = DAG(dag_id='{dag_id}',")
	code_lines.append("          default_args=default_args,")
	code_lines.append("          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG")
	code_lines.append("          catchup=False)")
	code_lines.append("")

	# Definimos los OPERADORES para cada funcion creada
	for step in scensteps:

		# Obtenemos el nombre del paso y lo limpiamos quitando espacios
		step_name = step.get("StepName", "").strip().replace(" ", "_")

		code_lines.append(f"{step_name}_task = PythonOperator(")
		code_lines.append(f"    task_id='{step_name}_task',")
		code_lines.append(f"    python_callable={step_name},")
		code_lines.append("    provide_context=True,")
		code_lines.append("    dag=dag")
		code_lines.append(")")
		code_lines.append("")
			
		code_lines.append("###### SECUENCIA DE EJECUCION ######")
		code_lines.append(f"{step_name}_task")

	return "\n".join(code_lines)

# FUNCION PARA GENERAR EL CODIGO DEL DAG A PARTIR DEL JSON DE UN PROCEDURE
def generate_dag_code_procedure(json_file):
	# Cargamos el JSON
	with open(json_file, 'r') as f:
		data = json.load(f)
	
	# Extraemos el nombre dell procedure y la lista de pasos 
	procedure_list = data.get("SnpTrt", [])
	procedure = procedure_list[0]
	procedure_name = procedure.get("TrtName", "").strip().replace(" ", "_")
	scensteps = data.get("SnpTxtHeader", [])

	# Iniciamos la lista de lineas de codigo
	code_lines = []
	
	# ENCABEZADO: IMPORTS Y DEFINICION DEL AGENTE
	code_lines.append("from airflow import DAG")
	code_lines.append("from airflow.operators.python import PythonOperator, BranchPythonOperator")
	code_lines.append("from airflow.utils.dates import days_ago")
	code_lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
	code_lines.append("from airflow.models import Variable")
	code_lines.append("from airflow.operators.trigger_dagrun import TriggerDagRunOperator")
	code_lines.append("from airflow.sensors.filesystem import FileSensor")
	code_lines.append("from airflow.sensors.base import BaseSensorOperator")
	code_lines.append("from airflow.operators.email import EmailOperator")
	code_lines.append("from airflow.utils.decorators import apply_defaults")
	code_lines.append("from datetime import datetime")
	code_lines.append("import time")
	code_lines.append("")
	code_lines.append("### FUNCIONES DE CADA TAREA ###")
	code_lines.append("")

	# FUNCIONES DE CADA TAREA 
	code_lines.append(f"def {procedure_name}(**kwargs):")

	# Marcador para saber si ya insertamos la línea del hook
	hook_inserted = False
	# Para cada tarea del paso en el JSON definimos una funcion con el codigo de las tareas
	for task in scensteps:

		if not task:
			code_lines.append("    pass")
		else:
			
			if not hook_inserted:
				code_lines.append("    hook = PostgresHook(postgres_conn_id='mi_conexion')")
				code_lines.append("")
				hook_inserted = True

			tasks = task.get("Txt", "")
			parsed_sql_txt = parseo_sql(tasks)
			code_lines.append(f"    sql_query = '''{parsed_sql_txt}'''")
			code_lines.append("    hook.run(sql_query)")
			#code_lines.append("")
		
		code_lines.append("")

	code_lines.append("###### DEFINICION DEL DAG ###### ")
	code_lines.append("")
	code_lines.append("default_args = {")
	code_lines.append("    'owner': 'airflow',")
	code_lines.append("    'start_date': days_ago(1)")
	code_lines.append("}")
	code_lines.append("")
	code_lines.append(f"dag = DAG(dag_id='{procedure_name}',")
	code_lines.append("          default_args=default_args,")
	code_lines.append("          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG")
	code_lines.append("          catchup=False)")
	code_lines.append("")

	# Definimos los OPERADORES para cada funcion creada
	code_lines.append(f"{procedure_name}_task = PythonOperator(")
	code_lines.append(f"    task_id='{procedure_name}_task',")
	code_lines.append(f"    python_callable={procedure_name},")
	code_lines.append("    provide_context=True,")
	code_lines.append("    dag=dag")
	code_lines.append(")")
	code_lines.append("")

	code_lines.append("###### SECUENCIA DE EJECUCION ######")
	code_lines.append(f"{procedure_name}_task")

	return "\n".join(code_lines)


# FUNCION PRINCIPAL: LECTURA DE ARGUMENTOS Y ESCRITURA DEL ARCHIVO DE SALIDA.
if __name__ == '__main__':

	if len(sys.argv) < 3:
		print("Uso: python3 generador_de_dags.py <ruta_al_json> <ruta_al_output_py>")
		sys.exit(1)

	json_file = sys.argv[1]
	output_py = sys.argv[2]

	# Leemos el JSON y extraemos el tipo
	with open(json_file, 'r', encoding='utf-8') as f:
		data = json.load(f)

	tipo = data.get("tipo")

	# Llamamos a la funcion adecuada segun el tipo
	if tipo == "package":
		dag_code = generate_dag_code_package(json_file)
	elif tipo == "interface":
		dag_code = generate_dag_code_interface(json_file)
	elif tipo == "procedure":
		dag_code = generate_dag_code_procedure(json_file)
	else:
		raise ValueError(f"Tipo de objeto desconocido en el JSON: {tipo!r}")

	# Escribimos el DAG generado
	with open(output_py, 'w', encoding='utf-8') as f:
		f.write(dag_code)

	print(f"Tipo de objeto usado: {tipo}")
	print(f"Archivo DAG generado y guardado en: {output_py}")


# Ejemplo ejecucion: 

# En Linux:
# export AMBIENTE=produccion
# python3 procesamientoXML/parseo/generador_de_dags.py JSON/AT26/PACKAGE_AT26_TO_FILE_ERROR.json DAGs/AT26/DAG_AT26_TO_FILE_ERROR.py

# En Windows:
# set AMBIENTE=produccion
# Comprobar valor de la variable: echo %AMBIENTE% 
# python3 procesamientoXML/parseo/generador_de_dags.py JSON/AT26/PACKAGE_AT26_TO_FILE_ERROR.json DAGs/AT26/DAG_AT26_TO_FILE_ERROR.py

# python3 procesamientoXML/parseo/generador_de_dags.py JSON/SIG/ODS-OFSAA/Dimensiones/DIM-Comision/PKG_FL_DIM_LOAD_STG_ACTIVITY_COMISION.json DAGs/SIG/ODS-OFSAA/Dimensiones/DIM-Comision/PKG_FL_DIM_LOAD_STG_ACTIVITY_COMISION.py