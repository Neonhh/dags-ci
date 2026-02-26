import os
import subprocess

# Raiz del proyecto (dags-ci)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))

# Carpetas de entrada y salida
xml_dir = "XMLs"
json_dir = "JSON"
dag_dir = os.path.join(project_root, "dags","migrados")

# Recorre TODOS los XML dentro de sus subcarpetas
for root, dirs, files in os.walk(xml_dir):
    for file in files:
        if file.lower().endswith(".xml"):
            xml_path = os.path.join(root, file)

            # Extraer nombre base sin extensión
            base_name = os.path.splitext(file)[0]

            # Construir rutas correspondientes para JSON y DAG
            relative_path = os.path.relpath(root, xml_dir)
            json_output_folder = os.path.join(json_dir, relative_path)
            dag_output_folder = os.path.join(dag_dir, relative_path)

            # Crear carpetas si no existen
            os.makedirs(json_output_folder, exist_ok=True)
            os.makedirs(dag_output_folder, exist_ok=True)

            json_path = os.path.join(json_output_folder, base_name + ".json")
            dag_path = os.path.join(dag_output_folder, "DAG_" + base_name + ".py")

            print(f"\n==============================")
            print(f"Procesando el XML: {xml_path}")
            print(f"Generando el JSON:  {json_path}")
            print(f"Generando el DAG:  {dag_path}")
            print("==============================")

            # Ejecutar extracción XML → JSON
            print(f"\n--- Ejecutando extraer_info_xml_elementos_extraccion.py ---")
            subprocess.run([
                "python3",
                "procesamientoXML/parseo_extraccion/extraer_info_xml_elementos_extraccion.py",
                xml_path,
                json_path
            ], check=True)

            # Ejecutar generación JSON → DAG
            print(f"\n--- Ejecutando generador_de_dags_extraccion.py ---")
            subprocess.run([
                "python3",
                "procesamientoXML/parseo_extraccion/generador_de_dags_extraccion.py",
                json_path,
                dag_path
            ], check=True)

print(f"\n==============================")
print("✔ Procesamiento completado exitosamente para todos los XML.")
print("==============================\n")