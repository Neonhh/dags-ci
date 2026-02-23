#!/usr/bin/env python3
"""
Script para convertir EmailOperator a PythonOperator con send_email() en todos los DAGs.

Este script:
1. Busca todos los archivos .py en la carpeta dags/
2. Identifica archivos que usan EmailOperator
3. Reemplaza EmailOperator por PythonOperator que llama a send_email()
4. Mantiene exactamente el mismo contenido del email (to, subject, html_content)
5. Actualiza los imports necesarios
6. Crea backups de los archivos modificados

Uso:
    python convert_email_operator.py [--dry-run] [--backup]
"""

import re
import os
import shutil
from pathlib import Path
from datetime import datetime


class EmailOperatorConverter:
    def __init__(self, dags_folder="dags", backup=True, dry_run=False):
        self.dags_folder = Path(dags_folder)
        self.backup = backup
        self.dry_run = dry_run
        self.stats = {
            "files_scanned": 0,
            "files_modified": 0,
            "operators_converted": 0
        }

    def find_python_files(self):
        """Encuentra todos los archivos Python en la carpeta dags/"""
        return list(self.dags_folder.rglob("*.py"))

    def has_email_operator(self, content):
        """Verifica si el archivo usa EmailOperator"""
        patterns = [
            r'from\s+airflow\.operators\.email\s+import\s+EmailOperator',
            r'from\s+airflow\.providers\.smtp\.operators\.smtp\s+import\s+EmailOperator',
            r'EmailOperator\s*\('
        ]
        return any(re.search(pattern, content) for pattern in patterns)

    def extract_email_operators(self, content):
        """Extrae todas las instancias de EmailOperator del contenido"""
        # Pattern más robusto para capturar EmailOperator multilinea
        pattern = r'(\w+)\s*=\s*EmailOperator\s*\(((?:[^()]*|\([^()]*\))*)\)'
        matches = re.finditer(pattern, content, re.DOTALL)

        operators = []
        for match in matches:
            var_name = match.group(1)
            params_str = match.group(2)
            full_match = match.group(0)

            # Detectar indentación: buscar el comienzo de la línea donde está var_name =
            lines_before = content[:match.start()].split('\n')
            if lines_before:
                last_line = lines_before[-1]
                # La indentación es todo lo que precede al primer caracter no-espacio
                indent = ''
                for char in last_line:
                    if char in ' \t':
                        indent += char
                    else:
                        break
            else:
                indent = ''

            # Extraer parámetros individuales
            params = self.parse_parameters(params_str)

            operators.append({
                'var_name': var_name,
                'params': params,
                'full_text': full_match,
                'start': match.start(),
                'end': match.end(),
                'indent': indent
            })

        return operators

    def parse_parameters(self, params_str):
        """Parsea los parámetros del EmailOperator"""
        params = {}

        # task_id
        task_id_match = re.search(r'task_id\s*=\s*["\']([^"\']+)["\']', params_str)
        if task_id_match:
            params['task_id'] = task_id_match.group(1)

        # to (puede ser string o variable)
        to_match = re.search(r'to\s*=\s*["\']([^"\']+)["\']', params_str)
        if to_match:
            params['to'] = f'"{to_match.group(1)}"'
        else:
            # Puede ser una lista o variable
            to_list_match = re.search(r'to\s*=\s*(\[[^\]]+\])', params_str)
            if to_list_match:
                params['to'] = to_list_match.group(1)
            else:
                # Puede ser referencia a variable
                to_var_match = re.search(r'to\s*=\s*([^,\n]+)', params_str)
                if to_var_match:
                    params['to'] = to_var_match.group(1).strip()

        # subject
        subject_match = re.search(r'subject\s*=\s*["\']([^"\']+)["\']', params_str)
        if subject_match:
            params['subject'] = subject_match.group(1)
        else:
            # Puede ser f-string o expresión
            subject_expr_match = re.search(r'subject\s*=\s*([^,\n]+)', params_str)
            if subject_expr_match:
                params['subject_expr'] = subject_expr_match.group(1).strip()

        # html_content (multilinea con triple quotes)
        html_triple_match = re.search(r'html_content\s*=\s*"""(.*?)"""', params_str, re.DOTALL)
        if html_triple_match:
            params['html_content'] = html_triple_match.group(1)
        else:
            html_triple_match = re.search(r"html_content\s*=\s*'''(.*?)'''", params_str, re.DOTALL)
            if html_triple_match:
                params['html_content'] = html_triple_match.group(1)
            else:
                # String simple
                html_simple_match = re.search(r'html_content\s*=\s*["\']([^"\']+)["\']', params_str)
                if html_simple_match:
                    params['html_content'] = html_simple_match.group(1)

        # conn_id (lo ignoramos porque usaremos la configuración global)

        # dag
        dag_match = re.search(r'dag\s*=\s*(\w+)', params_str)
        if dag_match:
            params['dag'] = dag_match.group(1)

        return params

    def generate_python_callable(self, operator_info):
        """Genera una función Python callable y un PythonOperator"""
        var_name = operator_info['var_name']
        params = operator_info['params']
        task_id = params.get('task_id', var_name.replace('_task', ''))
        indent = operator_info.get('indent', '')
        dag_ref = params.get('dag', 'dag')

        # Construir parámetros de send_email
        to_param = params.get('to', '"destinatario@example.com"')

        # Subject
        if 'subject' in params:
            subject_param = f'"{params["subject"]}"'
        elif 'subject_expr' in params:
            subject_param = params['subject_expr']
        else:
            subject_param = '"Email desde Airflow"'

        # HTML Content
        html_content = params.get('html_content', '<p>Email generado</p>')

        # Nombre de la función callable
        func_name = f"{var_name}_callable"

        # Generar código con la indentación correcta
        code = f'''{indent}def {func_name}(**kwargs):
{indent}    """Envía email usando send_email() - Convertido desde EmailOperator"""
{indent}    from airflow.utils.email import send_email
{indent}    send_email(
{indent}        to={to_param},
{indent}        subject={subject_param},
{indent}        html_content="""
{html_content}
{indent}        """
{indent}    )

{indent}{var_name} = PythonOperator(
{indent}    task_id="{task_id}",
{indent}    python_callable={func_name},
{indent}    dag={dag_ref}
{indent})'''

        return code

    def update_imports(self, content):
        """Actualiza los imports necesarios"""
        # Reemplazar import de EmailOperator directamente por PythonOperator
        content = re.sub(
            r'from\s+airflow\.operators\.email\s+import\s+EmailOperator',
            'from airflow.operators.python import PythonOperator',
            content
        )
        content = re.sub(
            r'from\s+airflow\.providers\.smtp\.operators\.smtp\s+import\s+EmailOperator',
            'from airflow.operators.python import PythonOperator',
            content
        )

        return content

    def convert_file(self, file_path):
        """Convierte un archivo reemplazando EmailOperator por send_email"""
        print(f"\n📄 Procesando: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        self.stats["files_scanned"] += 1

        if not self.has_email_operator(content):
            print("   ℹ️  No contiene EmailOperator, saltando...")
            return False

        # Extraer operadores
        operators = self.extract_email_operators(content)

        if not operators:
            print("   ⚠️  Contiene import pero no instancias de EmailOperator")
            return False

        print(f"   ✅ Encontrados {len(operators)} EmailOperator(s)")

        # Crear backup
        if self.backup and not self.dry_run:
            backup_path = f"{file_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(file_path, backup_path)
            print(f"   💾 Backup creado: {backup_path}")

        # Reemplazar operadores (de atrás hacia adelante para no afectar posiciones)
        new_content = content
        for operator in sorted(operators, key=lambda x: x['start'], reverse=True):
            new_code = self.generate_python_callable(operator)
            new_content = (
                new_content[:operator['start']] +
                new_code +
                new_content[operator['end']:]
            )
            self.stats["operators_converted"] += 1

        # Actualizar imports
        new_content = self.update_imports(new_content)

        if self.dry_run:
            print("   🔍 [DRY RUN] Cambios que se aplicarían:")
            print("   " + "-" * 60)
            # Mostrar diff simplificado
            for operator in operators:
                print(f"   - EmailOperator '{operator['var_name']}' → función @task")
            print("   " + "-" * 60)
        else:
            # Guardar cambios
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print("   ✅ Archivo modificado exitosamente")
            self.stats["files_modified"] += 1

        return True

    def convert_all(self):
        """Convierte todos los archivos en la carpeta dags/"""
        print("🚀 Iniciando conversión de EmailOperator a send_email()")
        print(f"📁 Carpeta: {self.dags_folder.absolute()}")
        print(f"🔍 Modo: {'DRY RUN (sin cambios)' if self.dry_run else 'MODIFICACIÓN'}")
        print(f"💾 Backups: {'Habilitados' if self.backup else 'Deshabilitados'}")
        print("=" * 70)

        files = self.find_python_files()
        print(f"\n📊 Archivos Python encontrados: {len(files)}")

        for file_path in files:
            try:
                self.convert_file(file_path)
            except Exception as e:
                print(f"   ❌ Error procesando {file_path}: {e}")

        # Mostrar estadísticas
        print("\n" + "=" * 70)
        print("📊 Resumen de conversión:")
        print(f"   Archivos escaneados:    {self.stats['files_scanned']}")
        print(f"   Archivos modificados:   {self.stats['files_modified']}")
        print(f"   Operadores convertidos: {self.stats['operators_converted']}")
        print("=" * 70)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Convierte EmailOperator a send_email() en todos los DAGs'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simula los cambios sin modificar archivos'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='No crear backups de los archivos'
    )
    parser.add_argument(
        '--dags-folder',
        default='dags',
        help='Carpeta que contiene los DAGs (default: dags)'
    )

    args = parser.parse_args()

    converter = EmailOperatorConverter(
        dags_folder=args.dags_folder,
        backup=not args.no_backup,
        dry_run=args.dry_run
    )

    converter.convert_all()

    if args.dry_run:
        print("\n💡 Ejecuta sin --dry-run para aplicar los cambios")


if __name__ == '__main__':
    main()
