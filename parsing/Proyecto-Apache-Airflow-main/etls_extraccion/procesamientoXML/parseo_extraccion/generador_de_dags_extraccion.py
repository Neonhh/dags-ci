import os
import json
import sys
from parser_sql_extraccion import parser_sql
from parser_sql_extraccion import parser_sql_sybase


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
	code_lines.append("from airflow.operators.python import PythonOperator")
	code_lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
	code_lines.append("from airflow.providers.jdbc.hooks.jdbc import JdbcHook")
	code_lines.append("from airflow.models import Variable")
	code_lines.append("from airflow.operators.trigger_dagrun import TriggerDagRunOperator")
	code_lines.append("from psycopg2.extras import execute_values")
	code_lines.append("from datetime import datetime, timedelta")
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
	
	# Para cada paso en el JSON definimos una FUNCION con el codigo de sus tareas
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
							parsed_sql = parser_sql(sql)
							code_lines.append(f"    sql_query = f'''{parsed_sql}'''")
							code_lines.append("    result = hook.get_records(sql_query)")
							code_lines.append("")
							code_lines.append(f"    Variable.set('{var_name}', serialize_value(result[0][0]))")
					
					else:
						# Si snp_var_list está vacio, no hay tarea que ejecutar, continuamos al siguiente elemento
						continue
				
				elif task_type != "Variable" and task_type != "Oracle Data Integrator Command": # Aqui entran los SnpScenTask del tipo Integration, Loading, Control y Procedure.
					
					# aqui es donde se coloca "hook = PostgresHook" una sola vez por funcion
					if not hook_inserted:
						code_lines.append("    pg_hook = PostgresHook(postgres_conn_id='ods')")
						code_lines.append("    sybase_hook = JdbcHook(jdbc_conn_id='sybase_ase_conn')")
						code_lines.append("")
						hook_inserted = True

					# Primero se revisa el campo ColTxt que contiene la consulta de Sybase
					sql_query_coltxt = task.get("ColTxt", "")
					if sql_query_coltxt and sql_query_coltxt.lower() != "null":
						
						parsed_sql_coltxt = parser_sql_sybase(sql_query_coltxt)
						code_lines.append(f"    # {task.get("TaskName3", "")}")
						code_lines.append(f"    sql_query_coltxt = f'''{parsed_sql_coltxt}'''")
						code_lines.append(f"""    logger.info("Accion a ejecutarse: {task.get("TaskName3", "")}") """)
						code_lines.append("")
						code_lines.append("    # conexion a sybase")
						code_lines.append("    sybase_conn = sybase_hook.get_conn()")
						code_lines.append("    sybase_cursor = sybase_conn.cursor()")
						code_lines.append("    sybase_cursor.execute(sql_query_coltxt)")
						code_lines.append("")
						code_lines.append(f"""    logger.info("Accion: {task.get("TaskName3", "")}, ejecutada exitosamente") """)
						code_lines.append("")

					# Luego se procesa el campo DefTxt que contiene las consultas de Oracle
					sql_query_deftxt = task.get("DefTxt", "")
					# Convertir el SQL de Oracle a Postgres
					parsed_sql_deftxt = parser_sql(sql_query_deftxt)

					code_lines.append(f"    # {task.get("TaskName3", "")}")
					code_lines.append(f"    sql_query_deftxt = f'''{parsed_sql_deftxt}'''")
					code_lines.append(f"""    logger.info("Accion a ejecutarse: {task.get("TaskName3", "")}") """)

					if task.get("TaskName3", "").strip().lower() == "load data":
						code_lines.append("")
						code_lines.append("    # insercion por lotes en postgres")
						code_lines.append("    pg_conn = pg_hook.get_conn()")
						code_lines.append("    pg_cursor = pg_conn.cursor()")
						code_lines.append("")
						code_lines.append("    batch_size = 10000")
						code_lines.append("    total_inserted = 0")
						code_lines.append("    batch_num = 1")
						code_lines.append("")
						code_lines.append("    while True:")
						code_lines.append("        rows = sybase_cursor.fetchmany(batch_size)")
						code_lines.append("        if not rows:")
						code_lines.append("            break")
						code_lines.append("        execute_values(pg_cursor, sql_query_deftxt, rows)")
						code_lines.append("        pg_conn.commit()")
						code_lines.append("")
						code_lines.append("        total_inserted += len(rows)")
						code_lines.append("        logger.info(f\"Lote {batch_num}: insertados {len(rows)} registros (Total acumulado: {total_inserted})\")")
						code_lines.append("")
						code_lines.append("        batch_num += 1")
						code_lines.append("")
						code_lines.append("    # cerrar conexiones")
						code_lines.append("    sybase_cursor.close()")
						code_lines.append("    sybase_conn.close()")
						code_lines.append("    pg_cursor.close()")
						code_lines.append("    pg_conn.close()")
						code_lines.append("")
					else:
						code_lines.append("    pg_hook.run(sql_query_deftxt)")
						#code_lines.append("""	   kwargs['ti'].log.info(f"Lote {batch_num}: insertados {len(rows)} registros (Total acumulado: {total_inserted})") """)

					code_lines.append(f"""    logger.info("Accion: {task.get("TaskName3", "")}, ejecutada exitosamente") """)
					code_lines.append("")
				  
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

	
	# SECUENCIA DE EJECUCION DE TAREAS (en orden de aparicion en el JSON)
	if valid_step_names:
		code_lines.append("###### SECUENCIA DE EJECUCION ######")
		sequence = " >> ".join(valid_step_names)	
		code_lines.append(sequence)
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
	code_lines.append("from airflow.operators.python import PythonOperator")
	code_lines.append("from airflow.utils.dates import days_ago")
	code_lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
	code_lines.append("from airflow.providers.odbc.hooks.odbc import ODBCHook")
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
						parsed_sql_coltxt = parser_sql(sql_query_coltxt)
						code_lines.append(f"    # {task.get("TaskName3", "")}")
						code_lines.append(f"    sql_query_coltxt = f'''{parsed_sql_coltxt}'''")
						code_lines.append("    hook.run(sql_query_coltxt)")
						code_lines.append("")

					# Luego se procesa el campo DefTxt
					sql_query_deftxt = task.get("DefTxt", "")
					parsed_sql_deftxt = parser_sql(sql_query_deftxt)
					code_lines.append(f"    # {task.get("TaskName3", "")}")
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
	code_lines.append("from airflow.operators.python import PythonOperator")
	code_lines.append("from airflow.utils.dates import days_ago")
	code_lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
	code_lines.append("from airflow.providers.odbc.hooks.odbc import ODBCHook")
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
			parsed_sql_txt = parser_sql(tasks)
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
		print("Uso: python3 generador_de_dags_extraccion.py <ruta_al_json> <ruta_al_output_py> [--force]")
		print("  --force: Sobrescribir si el archivo ya existe")
		sys.exit(1)

	json_file = sys.argv[1]
	output_py = sys.argv[2]
	force_overwrite = "--force" in sys.argv

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

	if os.path.exists(output_py) and not force_overwrite:
		print(f"El archivo {output_py} ya existe. Por seguridad, no se sobrescribirá. Use --force para permitir sobrescritura.")
		sys.exit(1)
	
	# Escribimos el DAG generado
	with open(output_py, 'w', encoding='utf-8') as f:
		f.write(dag_code)

	print(f"Tipo de objeto usado: {tipo}")
	print(f"Archivo DAG generado y guardado en: {output_py}")


# Ejemplo ejecucion: 

# En Linux:
# export AMBIENTE=produccion
# python3 procesamientoXML/parseo_extraccion/generador_de_dags_extraccion.py JSON/EXTRACCION/AT26/PACKAGE_AT26_TO_FILE_ERROR.json DAGs/EXTRACCION/AT26/DAG_AT26_TO_FILE_ERROR.py

# En Windows:
# set AMBIENTE=produccion
# Comprobar valor de la variable: echo %AMBIENTE% 
# python3 procesamientoXML/parseo_extraccion/generador_de_dags_extraccion.py JSON/EXTRACCION/AT/AT26/PACKAGE_AT26_TO_FILE_ERROR.json DAGs/EXTRACCION/AT/AT26/DAG_AT26_TO_FILE_ERROR.py
