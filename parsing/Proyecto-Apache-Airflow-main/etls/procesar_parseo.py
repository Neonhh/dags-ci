import os
import subprocess

# Carpetas
xml_dir = "XMLs"
json_dir = "JSON"
dag_dir = "DAGs"

# Recorre TODOS los XML dentro de sus subcarpetas
for root, dirs, files in os.walk(xml_dir):
    for file in files:
        if file.lower().endswith(".xml"):
            xml_path = os.path.join(root, file)

            # Extraer nombre base sin extensión
            base_name = os.path.splitext(file)[0]

            # Ruta relativa 
            relative_path = os.path.relpath(root, xml_dir)

            # Construir rutas correspondientes para JSON y DAG. Rutas de Salida.
            json_output_folder = os.path.join(json_dir, relative_path)
            json_filtered_folder = os.path.join(json_output_folder, "JSON_FILTRADOS")
            dag_output_folder = os.path.join(dag_dir, relative_path)

            # Crear carpetas si no existen
            os.makedirs(json_output_folder, exist_ok=True)
            os.makedirs(json_filtered_folder, exist_ok=True)
            os.makedirs(dag_output_folder, exist_ok=True)

            # Archivos de salida
            json_path = os.path.join(json_output_folder, base_name + ".json")
            json_filtered_path = os.path.join(json_filtered_folder, "FILTRADO_" + base_name + ".json")
            dag_path = os.path.join(dag_output_folder, "DAG_" + base_name + ".py")

            print(f"\n==============================")
            print(f"Procesando el XML: {xml_path}")
            print(f"Generando el JSON: {json_path}")
            print(f"Filtrando JSON:    {json_filtered_path}")
            print(f"Generando el DAG:  {dag_path}")
            print("==============================")

            # Ejecutar extracción XML → JSON
            print(f"\n--- Ejecutando extraer_info_xml_elementos.py ---")
            subprocess.run([
                "python3",
                "procesamientoXML/parseo/extraer_info_xml_elementos.py",
                xml_path,
                json_path
            ], check=True)

            # 2) Ejecutar filtrado JSON → JSON_FILTRADO
            print("\n--- Ejecutando filtrar_tareas_json.py ---")
            subprocess.run([
                "python3",
                "procesamientoXML/parseo/filtrar_tareas_json.py",
                json_path,
                json_filtered_path
            ], check=True)

            # Ejecutar generación JSON → DAG
            print("\n--- Ejecutando generador_de_dags.py ---")
            subprocess.run([
                "python3",
                "procesamientoXML/parseo/generador_de_dags.py",
                json_filtered_path,
                dag_path
            ], check=True)

print(f"\n==============================")
print("✔ Procesamiento completado exitosamente para todos los XML.")
print("==============================\n")