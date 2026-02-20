import sys
import json
import re

if len(sys.argv) != 3:
    print("Uso: python3 filtrar_tareas_json.py <input.json> <output.json>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

# 1) Carga el JSON original
with open(input_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# 2) Prepara los patrones a filtrar:
patterns = [
    # a) DROP/CREATE TABLE con %COL_… dentro de snpRef.getObjectName(...)
    # drop table <?=snpRef.getObjectName(\"L\", \"%COL_\", \"W\") ?>
    # create table <?=snpRef.getObjectName(\"L\", \"%COL_\", \"W\") ?>
    re.compile(
        r'(?i)\b(?:DROP|CREATE)\s+TABLE\s+' 
        r'<\?=snpRef\.getObjectName\(\s*"L"\s*,\s*"%COL_[^"]+"\s*,\s*"W"\s*\)\s*\?>'
    ),

    # a.1) DROP/CREATE TABLE con %INT_… dentro de snpRef.getObjectName(...)
    re.compile(
        r'(?i)\b(?:DROP|CREATE)\s+TABLE\s+'
        r'<\?=snpRef\.getObjectName\(\s*"L"\s*,\s*"%INT_[^"]+"\s*,\s*"W"\s*\)\s*\?>'
    ),

    # b) DefTxt con "/* commit */" (posibles espacios alrededor)
    re.compile(r'^\s*/\*\s*commit\s*\*/\s*$', re.IGNORECASE),

    # c) Comienza con BEGIN<newline>DBMS_STATS.GATHER_TABLE_STATS
    re.compile(r'(?i)^BEGIN\s*[\r\n]+DBMS_STATS\.GATHER_TABLE_STATS'),

    # d) create table <?=snpRef.getObjectNameDefaultPSchema("L", "SNP_CHECK_TAB", "W") ?>
    re.compile(
        r'(?i)^CREATE\s+TABLE\s+'
        r'<\?=snpRef\.getObjectNameDefaultPSchema'
        r'\(\s*"L"\s*,\s*"SNP_CHECK_TAB"\s*,\s*"W"\s*\)\s*\?>'
    ),

    # e) delete from <?=snpRef.getObjectNameDefaultPSchema("L", "SNP_CHECK_TAB", "W") ?>
    re.compile(
        r'(?i)^DELETE\s+FROM\s+'
        r'<\?=snpRef\.getObjectNameDefaultPSchema'
        r'\(\s*"L"\s*,\s*"SNP_CHECK_TAB"\s*,\s*"W"\s*\)\s*\?>'
    ),

    # f) insert into <?=snpRef.getObjectNameDefaultPSchema(\"L\", \"SNP_CHECK_TAB\", \"W\") ?>
    re.compile(
        r'(?i)^INSERT\s+INTO\s+<\?=snpRef\.getObjectNameDefaultPSchema'
        r'\(\s*"L"\s*,\s*"SNP_CHECK_TAB"\s*,\s*"W"\s*\)\s*\?>'
    ),

    # g) DELETE FROM <?=snpRef.getObjectName("L", "%INT_<tabla>", "W")?>
    re.compile(
        r'(?i)^DELETE\s+FROM\s+<\?=snpRef\.getObjectName\(\s*"L"\s*,\s*"%INT_[^"]+"\s*,\s*"W"\s*\)\s*\?>'
    ),

    # h) DELETE FROM %ERR_…
    re.compile(
        r'(?i)^DELETE\s+FROM\s+<\?=snpRef\.getObjectName\(\s*"L"\s*,\s*"%ERR_[^"]+"\s*,\s*"W"\s*\)\s*\?>'
    ),
]

# 3) Recorre y filtra cada SnpScenTask
for step in data.get("SnpScenStep", []):
    original_tasks = step.get("SnpScenTask", [])
    filtered_tasks = []
    for task in original_tasks:
        txt = task.get("DefTxt", "")
        # Si cualquiera de los patrones coincide, descartamos la tarea
        if any(p.search(txt) for p in patterns):
            continue
        filtered_tasks.append(task)
    step["SnpScenTask"] = filtered_tasks

# 4) Guarda el JSON filtrado
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"Filtrado completado. Archivo guardado en: {output_path}")
