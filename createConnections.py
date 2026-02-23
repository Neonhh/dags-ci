import subprocess
import json
from pathlib import Path


def create_connection_via_cli(container_name: str, connection_data: dict) -> bool:
    """Crea una conexión en Airflow usando el CLI dentro del contenedor"""

    conn_id = connection_data.get("conn_id")
    conn_type = connection_data.get("conn_type")

    # Construir el comando base
    cmd = [
        "docker", "exec", container_name,
        "airflow", "connections", "add", conn_id,
        "--conn-type", conn_type
    ]

    # Agregar parámetros opcionales según el tipo de conexión
    if "host" in connection_data:
        cmd.extend(["--conn-host", connection_data["host"]])

    if "port" in connection_data:
        cmd.extend(["--conn-port", str(connection_data["port"])])

    if "login" in connection_data:
        cmd.extend(["--conn-login", connection_data["login"]])

    if "password" in connection_data:
        cmd.extend(["--conn-password", connection_data["password"]])

    if "database" in connection_data or "schema" in connection_data:
        schema = connection_data.get("database") or connection_data.get("schema")
        cmd.extend(["--conn-schema", schema])

    if "extra" in connection_data:
        # Convertir el dict extra a string JSON
        extra_json = json.dumps(connection_data["extra"])
        cmd.extend(["--conn-extra", extra_json])

    # Ejecutar el comando
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )

        if result.returncode == 0 or "Successfully added" in result.stdout:
            print(f"✅ Conexión '{conn_id}' creada exitosamente")
            return True
        elif "already exists" in result.stderr or "already exists" in result.stdout:
            print(f"⚠️  Conexión '{conn_id}' ya existe")
            return True
        else:
            print(f"❌ Error al crear conexión '{conn_id}':")
            print(result.stderr if result.stderr else result.stdout)
            return False
    except Exception as e:
        print(f"❌ Error al ejecutar comando para '{conn_id}': {e}")
        return False


if __name__ == "__main__":
    # Nombre del contenedor de Airflow (scheduler o webserver)
    AIRFLOW_CONTAINER = "airflow_655ea3-scheduler-1"

    # Leer conexiones desde el archivo JSON
    connections_file = Path(__file__).parent / "secrets/connections.json"
    with open(connections_file, "r") as f:
        connections = json.load(f)

    print(f"📋 Creando {len(connections)} conexiones en Airflow...\n")

    # Crear cada conexión
    success_count = 0
    for conn in connections:
        if create_connection_via_cli(AIRFLOW_CONTAINER, conn):
            success_count += 1
        print()  # Línea en blanco entre conexiones

    print(f"\n✨ Proceso completado: {success_count}/{len(connections)} conexiones creadas o ya existentes")
