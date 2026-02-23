from pathlib import Path
import re
from collections import defaultdict

print("🔍 Buscando correos electrónicos en los DAGs...\n")

# Buscar todos los correos en los DAGs
dag_folder = Path(__file__).parent / "dags"
email_pattern = r'["\']([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Za-z]{2,})["\']'

# Diccionario para almacenar: email -> [(archivo, línea_número)]
emails_found = defaultdict(list)

for dag_file in dag_folder.rglob("*.py"):
    with open(dag_file, "r") as file:
        lines = file.readlines()
        for line_num, line in enumerate(lines, 1):
            matches = re.finditer(email_pattern, line, re.IGNORECASE)
            for match in matches:
                email = match.group(1)
                emails_found[email].append((dag_file, line_num))

if not emails_found:
    print("⚠️  No se encontraron correos electrónicos en los DAGs.")
    exit(0)

# Mostrar correos encontrados
print(f"📧 Se encontraron {len(emails_found)} correo(s) único(s):\n")
for idx, (email, locations) in enumerate(emails_found.items(), 1):
    print(f"{idx}. {email} (aparece {len(locations)} veces)")
    # Mostrar primeras 3 ubicaciones como ejemplo
    for file_path, line_num in locations[:3]:
        rel_path = file_path.relative_to(dag_folder.parent)
        print(f"   - {rel_path}:{line_num}")
    if len(locations) > 3:
        print(f"   ... y {len(locations) - 3} más")
    print()

# Preguntar al usuario qué hacer con cada correo
replacements = {}
print("=" * 60)

for email in emails_found.keys():
    print(f"\n📧 Correo encontrado: {email}")
    response = input("¿Desea reemplazar este correo? (s/n): ").strip().lower()

    if response == 's':
        new_email = input("Ingrese el nuevo correo: ").strip()
        # Validar formato básico
        if re.match(r'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Za-z]{2,}$', new_email, re.IGNORECASE):
            replacements[email] = new_email
            print(f"✅ Se reemplazará '{email}' por '{new_email}'")
        else:
            print(f"⚠️  '{new_email}' no parece ser un correo válido. Se omitirá este reemplazo.")
    else:
        print(f"⏭️  Se omitirá '{email}'")

if not replacements:
    print("\n⚠️  No se realizaron cambios.")
    exit(0)

# Aplicar reemplazos
print(f"\n{'=' * 60}")
print("🔄 Aplicando cambios...\n")

files_updated = 0
total_replacements = 0

for dag_file in dag_folder.rglob("*.py"):
    with open(dag_file, "r") as file:
        content = file.read()

    original_content = content

    # Aplicar cada reemplazo
    for old_email, new_email in replacements.items():
        # Buscar con comillas simples y dobles
        pattern_single = f"'{old_email}'"
        pattern_double = f'"{old_email}"'

        if pattern_single in content:
            content = content.replace(pattern_single, f"'{new_email}'")
        if pattern_double in content:
            content = content.replace(pattern_double, f'"{new_email}"')

    if content != original_content:
        with open(dag_file, "w") as file:
            file.write(content)

        # Contar reemplazos en este archivo
        for old_email, new_email in replacements.items():
            count = original_content.count(f"'{old_email}'") + original_content.count(f'"{old_email}"')
            total_replacements += count

        files_updated += 1
        rel_path = dag_file.relative_to(dag_folder.parent)
        print(f"✏️  Actualizado: {rel_path}")

print(f"\n{'=' * 60}")
print("✅ Completado!")
print(f"   - Archivos modificados: {files_updated}")
print(f"   - Reemplazos totales: {total_replacements}")
print(f"{'=' * 60}")
