import sys
import xml.etree.ElementTree as ET
import json
import re

# Funcion para reemplazar la forma en que se acceden a las variables en ODI a Python
# y quitar el prefijo LS_ del nombre los esquemas.
def sql_optimizado(sql_text: str) -> str:
    """
    1) En los querys, reemplaza todas las apariciones de #<conexion>.<variable> por {<variable>}
    2) Quita el prefijo LS_ de cualquier esquema (LS_XYZ -> XYZ)
    """
    #  - #\w+     el prefijo con “#” y nombre de conexion (ODI_ATER, ODI_OTRA, etc)
    #  - \.(\w+)  punto y capturamos el nombre de variable (varCantDiasHabiles, etc)
    sql = re.sub(r"#\w+\.(\w+)", r"{\1}", sql_text)
    # Busca palabras que empiecen por 'LS_' y les quita ese prefijo
    sql = re.sub(r"\bLS_([A-Za-z]\w*)\b", r"\1", sql)
    return sql

# Funciones para extraer la informacion relevante de cada tipo de objeto, retornando un diccionario
def extraer_info_refresh(xml_file):
    """
    Extrae información de una variable de tipo refresh.
    Retorna un diccionario con:
      { "tipo": "refresh", "var_name": <nombre>, "ITxtVarIn": <valor>, "query": <consulta> }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    var_info = {}
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpVar":
            var_name = None
            itxt_var_in = None
            for field in obj.findall('Field'):
                name = field.get('name')
                text = field.text.strip() if field.text else ""
                if name == "VarName":
                    var_name = text
                elif name == "ITxtVarIn":
                    itxt_var_in = text
            if var_name is not None:
                var_info["tipo"] = "refresh"
                var_info["var_name"] = var_name
                var_info["ITxtVarIn"] = itxt_var_in if itxt_var_in else ""
                break

    # Buscamos la consulta en el objeto SnpTxtHeader cuyo ITxt coincida con ITxtVarIn
    query = ""
    if "ITxtVarIn" in var_info and var_info["ITxtVarIn"]:
        target_itxt = var_info["ITxtVarIn"]
        for obj in root.findall('Object'):
            if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpTxtHeader":
                itxt_val = None
                txt_val = None
                for field in obj.findall('Field'):
                    name = field.get('name')
                    text = field.text.strip() if field.text else ""
                    if name == "ITxt":
                        itxt_val = text
                    elif name == "Txt":
                        txt_val = text
                if itxt_val == target_itxt:
                    query = txt_val
                    break
    var_info["query"] = query
    return var_info

def extraer_info_set(xml_file):
    """
    Extrae información de una variable de tipo set.
    Retorna un diccionario con:
      { "tipo": "set", "var_name": <nombre>, "value": <valor> }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    var_info = {}
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpVar":
            var_name = None
            defv = None
            for field in obj.findall('Field'):
                name = field.get('name')
                text = field.text.strip() if field.text else ""
                if name == "VarName":
                    var_name = text
                elif name == "DefV":
                    defv = text
            if var_name is not None:
                var_info["tipo"] = "set"
                var_info["var_name"] = var_name
                var_info["value"] = defv if defv else ""
                break
    return var_info

def extraer_info_declare(xml_file):
    """
    Extrae información de una variable de tipo declare.
    Retorna un diccionario con:
      { "tipo": "declare", "var_name": <nombre>, "value": <valor> }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    var_info = {}
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpVar":
            var_name = None
            defv = None
            for field in obj.findall('Field'):
                name = field.get('name')
                text = field.text.strip() if field.text else ""
                if name == "VarName":
                    var_name = text
                elif name == "DefV":
                    defv = text
            if var_name is not None:
                var_info["tipo"] = "declare"
                var_info["var_name"] = var_name
                var_info["value"] = defv if defv else ""
                break
    return var_info

def extraer_info_procedure(xml_file):
    """
    Extrae informacion basica de un procedure exportado de ODI.
    Se recorre el XML buscando los objetos de interes:
    Retorna un diccionario con la siguiente estructura:
      {
         "tipo": "procedure",
         "SnpTrt": [
              { "TrtName": <valor> },
              ...
         ],
         "SnpTxtHeader": [
              { "Txt": <valor> },
              ...
         ],
         ...
      }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    procedure_info = {
        "tipo": "procedure",
        "SnpTrt": [],
        "SnpTxtHeader": [],
    }
    
    # Extraer objetos SnpTrt
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpTrt']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "TrtName":
                conn["TrtName"] = text
        if conn:
            procedure_info["SnpTrt"].append(conn)

    # Extraer objetos SnpTxtHeader
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpTxtHeader']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "Txt":
                conn["Txt"] = text
        if conn:
            procedure_info["SnpTxtHeader"].append(conn)

    return procedure_info

def extraer_info_interface(xml_file):
    """
    Extrae informacion basica de una interfaz exportada de ODI.
    Se recorre el XML buscando los objetos de interes:
    Retorna un diccionario con la siguiente estructura:
      {
         "tipo": "interface",
         "SnpPop": [
              { "LschemaName": <valor>, "PopName": <valor> },
              ...
         ],
         "SnpSourceTable": [
              { "LschemaName": <valor>, "TableName": <valor> },
              ...
         ],
         "SnpScenStep": [
              { "StepName": <valor>, "StepType": <valor> },
              ...
         ],
         ...
      }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    interface_info = {
        "tipo": "interface",
        "SnpLschema": [],
        "SnpTable": [],
        "SnpCond": [],
        "SnpTxtHeader": [],
        "SnpPop": [],
        "SnpSourceTab": [],
        "SnpScenStep": [],
        "SnpScenTask": [],
    }
    
    # Extraer objetos SnpLschema
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpLschema']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "LschemaName":
                conn["LschemaName"] = text
        if conn:
            interface_info["SnpLschema"].append(conn)

    # Extraer objetos SnpTable
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpTable']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "TableName":
                conn["TableName"] = text
            elif name == "ITable":
                conn["ITable"] = text
        if conn:
            interface_info["SnpTable"].append(conn)

    # Extraer objetos SnpCond
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpCond']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "ITable":
                conn["ITable"] = text
            elif name == "CondName":
                conn["CondName"] = text
            elif name == "ITxtCondSql":
                conn["ITxtCondSql"] = text
        if conn:
            interface_info["SnpCond"].append(conn)

    # Extraer objetos SnpTxtHeader
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpTxtHeader']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "ITxt":
                conn["ITxt"] = text
            elif name == "Txt":
                conn["Txt"] = text
        if conn:
            interface_info["SnpTxtHeader"].append(conn)

    # Extraer objetos SnpPop
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpPop']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "LschemaName":
                conn["LschemaName"] = text
            elif name == "PopName":
                conn["PopName"] = text
            elif name == "TableName":
                conn["TableName"] = text
        if conn:
            interface_info["SnpPop"].append(conn)

    # Extraer objetos SnpSourceTab
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpSourceTab']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "LschemaName":
                conn["LschemaName"] = text
            elif name == "TableName":
                conn["TableName"] = text
        if conn:
            interface_info["SnpSourceTab"].append(conn)

    # Extraer objetos SnpScenStep
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpScenStep']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "StepName":
                conn["StepName"] = text
            elif name == "StepType":
                conn["StepType"] = text
            elif name == "VarName":
                conn["VarName"] = text
            elif name == "TableName":
                conn["TableName"] = text
            elif name == "Nno":
                conn["Nno"] = text
        if conn:
            interface_info["SnpScenStep"].append(conn)

    # Extraer objetos SnpScenTask
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpScenTask']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "Nno":
                conn["Nno"] = text
            elif name == "ColTxt":
                conn["ColTxt"] = text
            elif name == "DefTxt":
                conn["DefTxt"] = text
            elif name == "TaskName1":
                conn["TaskName1"] = text
            elif name == "TaskName3":
                conn["TaskName3"] = text
            elif name == "OrdTrt":
                conn["OrdTrt"] = text
        if conn:
            interface_info["SnpScenTask"].append(conn)
    
    return interface_info

def extraer_info_package(xml_file):
    """
    Extrae información de un package exportado de ODI.
    Se recorre el XML buscando los objetos de interés:
    Retorna un diccionario con la siguiente estructura:
      {
         "tipo": "package",
         "SnpConnect": [
              { "ConName": <valor>, "ConnectType": <valor> },
              ...
         ],
         "SnpMtxt": [
              { "ITxt": <valor>, "Txt": <valor> },
              ...
         ],
         "SnpMorigTxt": [
              { "OrigineName": <valor>, "ITxtOrig": <valor> },
              ...
         ],
         ...
      }
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    pkg_info = {
        "tipo": "package",
        "SnpPackage": [],
        "SnpConnect": [],
        "SnpMtxt": [],
        "SnpMorigTxt": [],
        "SnpLschema": [],
        "SnpTable": [],
        "SnpCond": [],
        "SnpVar": [],
        "SnpTxtHeader": [],
        "SnpPop": [],
        "SnpSourceTab": [],
        "SnpScenStep": [],
        "SnpScenTask": [],
    }
    
    # Extraer objetos SnpPackage
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpPackage']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "PackName":
                conn["PackName"] = text
        if conn:
            pkg_info["SnpPackage"].append(conn)
    
    # Extraer objetos SnpConnect
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpConnect']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "ConName":
                conn["ConName"] = text
            elif name == "ConnectType":
                conn["ConnectType"] = text
        if conn:
            pkg_info["SnpConnect"].append(conn)
    
    # Extraer objetos SnpMtxt
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpMtxt']"):
        mtxt = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "ITxt":
                mtxt["ITxt"] = text
            elif name == "Txt":
                mtxt["Txt"] = text
        if mtxt:
            pkg_info["SnpMtxt"].append(mtxt)
    
    # Extraer objetos SnpMorigTxt
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpMorigTxt']"):
        morig = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "OrigineName":
                morig["OrigineName"] = text
            elif name == "ITxtOrig":
                morig["ITxtOrig"] = text
        if morig:
            pkg_info["SnpMorigTxt"].append(morig)

    # Extraer objetos SnpLschema
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpLschema']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "LschemaName":
                conn["LschemaName"] = text
        if conn:
            pkg_info["SnpLschema"].append(conn)

    # Extraer objetos SnpTable
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpTable']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "TableName":
                conn["TableName"] = text
            elif name == "ITable":
                conn["ITable"] = text
        if conn:
            pkg_info["SnpTable"].append(conn)

    # Extraer objetos SnpCond
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpCond']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "ITable":
                conn["ITable"] = text
            elif name == "CondName":
                conn["CondName"] = text
            elif name == "ITxtCondSql":
                conn["ITxtCondSql"] = text
        if conn:
            pkg_info["SnpCond"].append(conn)

    # Extraer objetos SnpVar
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpVar']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "DefV":
                conn["DefV"] = text
            elif name == "LschemaName":
                conn["LschemaName"] = text
            elif name == "VarName":
                conn["VarName"] = text
            elif name == "ITxtVar":
                conn["ITxtVar"] = text
            elif name == "ITxtVarIn":
                conn["ITxtVarIn"] = text
        if conn:
            pkg_info["SnpVar"].append(conn)

    # Extraer objetos SnpTxtHeader
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpTxtHeader']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "ITxt":
                conn["ITxt"] = text
            elif name == "Txt":
                conn["Txt"] = text
        if conn:
            pkg_info["SnpTxtHeader"].append(conn)

    # Extraer objetos SnpPop
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpPop']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "LschemaName":
                conn["LschemaName"] = text
            elif name == "PopName":
                conn["PopName"] = text
            elif name == "TableName":
                conn["TableName"] = text
        if conn:
            pkg_info["SnpPop"].append(conn)
    
    # Extraer objetos SnpSourceTab
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpSourceTab']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "LschemaName":
                conn["LschemaName"] = text
            elif name == "TableName":
                conn["TableName"] = text
        if conn:
            pkg_info["SnpSourceTab"].append(conn)

    # Extraer objetos SnpScenStep
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpScenStep']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "StepName":
                conn["StepName"] = text
            elif name == "StepType":
                conn["StepType"] = text
            elif name == "VarName":
                conn["VarName"] = text
            elif name == "TableName":
                conn["TableName"] = text
            elif name == "Nno":
                conn["Nno"] = text
            elif name == "OkNextStep":
                conn["OkNextStep"] = text
            elif name == "KoNextStep":
                conn["KoNextStep"] = text
        if conn:
            pkg_info["SnpScenStep"].append(conn)

    # Extraer objetos SnpScenTask
    for obj in root.findall("Object[@class='com.sunopsis.dwg.dbobj.SnpScenTask']"):
        conn = {}
        for field in obj.findall("Field"):
            name = field.get("name")
            text = field.text.strip() if field.text else ""
            if name == "Nno":
                conn["Nno"] = text
            elif name == "ColTxt":
                conn["ColTxt"] = text
            elif name == "DefTxt":
                conn["DefTxt"] = text
            elif name == "TaskName1":
                conn["TaskName1"] = text
            elif name == "TaskName3":
                conn["TaskName3"] = text
            elif name == "OrdTrt":
                conn["OrdTrt"] = text
        if conn:
            pkg_info["SnpScenTask"].append(conn)
    
    return pkg_info

# Funcion para detectar el tipo de elemento que se esta parseando
def detectar_tipo_elemento(xml_file):
    """
    Detecta automaticamente el tipo de elemento ODI
    segun el contenido del XML. Devuelve uno de:
      'package', 'interface', 'procedure', 'refresh', 'set', 'declare'
    o None si no se detecta.
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # 0) Si es un package (existe un objeto SnpPackage)
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpPackage":
            return "package"

    # 1) Primero verificamos si es una interface (SnpPop)
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpPop":
            return "interface"

    # 2) Si no es interface, revisamos si hay un SnpTrt => procedure
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpTrt":
            return "procedure"

    # 3) Revisamos variables (SnpVar)
    for obj in root.findall('Object'):
        if obj.get('class') == "com.sunopsis.dwg.dbobj.SnpVar":
            itxt_var_in_field = obj.find("Field[@name='ITxtVarIn']")
            defv_field        = obj.find("Field[@name='DefV']")
            ind_store_field   = obj.find("Field[@name='IndStore']")
            itxt_var_in = (itxt_var_in_field.text.strip() if itxt_var_in_field is not None and itxt_var_in_field.text else "")
            defv        = (defv_field.text.strip()        if defv_field is not None and defv_field.text else "")
            ind_store   = (ind_store_field.text.strip()   if ind_store_field is not None and ind_store_field.text else "")
            if itxt_var_in and itxt_var_in.lower() != "null":
                return "refresh"
            if defv:
                if 'H' in ind_store.upper():
                    return "set"
                elif 'S' in ind_store.upper():
                    return "declare"
                return "set"
    return None

# Funciones para construir el diccionario anidado a partir de los diccionarios creados en las funciones anteriores
def serializar_diccionario_arbol_procedure(procedure_info, output_filename):
    """
    Construye un diccionario de estructuras a partir de la informacion de procedure_info y lo serializa a un archivo JSON.
    """

    # Empezamos con un diccionario raíz.
    resultado = {}
    resultado["tipo"] = "procedure" 

    # Extraemos listas
    SnpTrt_list       = procedure_info.get("SnpTrt", [])
    SnpTxtHeader_list = procedure_info.get("SnpTxtHeader", [])

    # SnpLschema
    nuevo_SnpTrt = []
    for i, snp in enumerate(SnpTrt_list):
        tmp = {
            "Object"  : "com.sunopsis.dwg.dbobj.SnpTrt",
            "TrtName" : snp.get("TrtName", "")
        }
        nuevo_SnpTrt.append(tmp)
    
    # SnpLschema
    nuevo_SnpTxtHeader = []
    for i, snp in enumerate(SnpTxtHeader_list):
        tmp = {
            "Object" : "com.sunopsis.dwg.dbobj.SnpTxtHeader",
            "Txt"    : snp.get("Txt", "")
        }
        nuevo_SnpTxtHeader.append(tmp)

    # Ahora guardamos todas estas estructuras en el diccionario 'resultado'
    resultado["SnpTrt"]         = nuevo_SnpTrt
    resultado["SnpTxtHeader"]   = nuevo_SnpTxtHeader

    # Finalmente, serializamos a archivo JSON
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"Diccionario guardado en {output_filename}")

def serializar_diccionario_arbol_interface(interface_info, output_filename):
    """
    Construye un diccionario de estructuras anidadas a partir de la informacion de interface_info y lo serializa a un archivo JSON.
    """
    # Empezamos con un diccionario raíz.
    resultado = {}
    resultado["tipo"] = "interface" 

    # Extraemos listas
    SnpLschema_list   = interface_info.get("SnpLschema", [])
    SnpTable_list     = interface_info.get("SnpTable", [])
    SnpCond_list      = interface_info.get("SnpCond", [])
    SnpTxtHeader_list = interface_info.get("SnpTxtHeader", [])
    SnpPop_list       = interface_info.get("SnpPop", [])
    SnpSourceTab_list = interface_info.get("SnpSourceTab", [])
    SnpScenStep_list  = interface_info.get("SnpScenStep", [])
    SnpScenTask_list  = interface_info.get("SnpScenTask", [])

    # SnpLschema
    nuevo_SnpLschema = []
    for i, ls in enumerate(SnpLschema_list):
        tmp = {
            "Object"      : "com.sunopsis.dwg.dbobj.SnpLschema",
            "LschemaName" : ls.get("LschemaName", "")
        }
        nuevo_SnpLschema.append(tmp)

    # SnpTable (anidando SnpCond -> SnpTxtHeader)
    nuevo_SnpTable = []
    for i, table in enumerate(SnpTable_list):
        tmp_table = {
            "Object"   : "com.sunopsis.dwg.dbobj.SnpTable",
            "TableName": table.get("TableName", ""),
            "ITable"   : table.get("ITable", "")
        }

        # Para cada tabla, buscamos sus SnpCond (matching ITable)
        itable_value = table.get("ITable", "").strip()
        conds_encontradas = [c for c in SnpCond_list if c.get("ITable", "").strip() == itable_value]

        if conds_encontradas:
            tmp_table["SnpCond"] = []
            for cond in conds_encontradas:
                cond_obj = {
                    "Object"     : "com.sunopsis.dwg.dbobj.SnpCond",
                    "ITable"     : cond.get("ITable", ""),
                    "CondName"   : cond.get("CondName", ""),
                    "ITxtCondSql": cond.get("ITxtCondSql", "")
                }
                # Luego, para cada SnpCond, anidamos SnpTxtHeader que coincidan en ITxt = cond["ITxtCondSql"]
                itxt_value = cond.get("ITxtCondSql", "").strip()
                hijos_txtheader = [tx for tx in SnpTxtHeader_list if tx.get("ITxt", "").strip() == itxt_value]

                if hijos_txtheader:
                    cond_obj["SnpTxtHeader"] = []
                    for txh in hijos_txtheader:
                        cond_obj["SnpTxtHeader"].append({
                            "Object": "com.sunopsis.dwg.dbobj.SnpTxtHeader",
                            "ITxt"  : txh.get("ITxt", ""),
                            "Txt"   : txh.get("Txt", "")
                        })
                tmp_table["SnpCond"].append(cond_obj)

        nuevo_SnpTable.append(tmp_table)

    # SnpPop
    nuevo_SnpPop = []
    for i, pop in enumerate(SnpPop_list):
        tmp_pop = {
            "Object"     : "com.sunopsis.dwg.dbobj.SnpPop",
            "LschemaName": pop.get("LschemaName", ""),
            "PopName"    : pop.get("PopName", ""),
            "TableName (Target Table)"  : pop.get("TableName", "")
        }
        nuevo_SnpPop.append(tmp_pop)

    # SnpSourceTab
    nuevo_SnpSourceTab = []
    for i, tab in enumerate(SnpSourceTab_list):
        tmp_tab = {
            "Object"                    : "com.sunopsis.dwg.dbobj.SnpSourceTab",
            "LschemaName"               : tab.get("LschemaName", ""),
            "TableName (Source Table)"  : tab.get("TableName", "")
        }
        nuevo_SnpSourceTab.append(tmp_tab)

    # SnpScenStep (anidando SnpScenTask)
    nuevo_SnpScenStep = []
    for i, step in enumerate(SnpScenStep_list):
        parent_nno = step.get("Nno", "").strip()
        tmp_step = {
            "Object"   : "com.sunopsis.dwg.dbobj.SnpScenStep",
            "StepName" : step.get("StepName", ""),
            "StepType" : step.get("StepType", ""),
            "Nno"      : parent_nno,
            "TableName (Target Table)": step.get("TableName", "")
        }

        # Buscamos las tareas (SnpScenTask) con Nno = step["Nno"]
        tareas = [t for t in SnpScenTask_list if t.get("Nno", "").strip() == parent_nno]

        if tareas:
            tmp_step["SnpScenTask"] = []
            for tarea in tareas:
                tmp_task = {
                    "Object"    : "com.sunopsis.dwg.dbobj.SnpScenTask",
                    "Nno"       : tarea.get("Nno", ""),
                    "TaskName1" : tarea.get("TaskName1", ""),
                    "ColTxt"    : sql_optimizado(tarea.get("ColTxt", "")),
                    "DefTxt"    : sql_optimizado(tarea.get("DefTxt", "")),
                    "TaskName3": tarea.get("TaskName3", ""),
                    "OrdTrt":    tarea.get("OrdTrt", "")
                }
                tmp_step["SnpScenTask"].append(tmp_task)
        nuevo_SnpScenStep.append(tmp_step)

    # Ahora guardamos todas estas estructuras en el diccionario 'resultado'
    resultado["SnpLschema"]   = nuevo_SnpLschema
    resultado["SnpTable"]     = nuevo_SnpTable
    resultado["SnpPop"]       = nuevo_SnpPop
    resultado["SnpSourceTab"] = nuevo_SnpSourceTab
    resultado["SnpScenStep"]  = nuevo_SnpScenStep

    # Finalmente, serializamos a archivo JSON
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"Diccionario guardado en {output_filename}")

def serializar_diccionario_arbol_package(pkg_info, output_filename):
    """
    Construye un diccionario de estructuras anidadas a partir de la informacion de pkg_info y lo serializa a un archivo JSON.
    """

    # Empezamos con un diccionario raiz.
    resultado = {}
    resultado["tipo"] = "package" 

    # Extraemos listas
    SnpPackage_list   = pkg_info.get("SnpPackage", [])
    SnpConnect_list   = pkg_info.get("SnpConnect", [])
    SnpMtxt_list      = pkg_info.get("SnpMtxt", [])
    SnpMorig_list     = pkg_info.get("SnpMorigTxt", [])
    SnpLschema_list   = pkg_info.get("SnpLschema", [])
    SnpTable_list     = pkg_info.get("SnpTable", [])
    SnpCond_list      = pkg_info.get("SnpCond", [])
    SnpVar_list       = pkg_info.get("SnpVar", [])
    SnpTxtHeader_list = pkg_info.get("SnpTxtHeader", [])
    SnpPop_list       = pkg_info.get("SnpPop", [])
    SnpScenStep_list  = pkg_info.get("SnpScenStep", [])
    SnpScenTask_list  = pkg_info.get("SnpScenTask", [])

    
    # SnpPackage
    nuevo_SnpPackage = []
    for i, spkg in enumerate(SnpPackage_list):
        tmp = {
            "Object"   : "com.sunopsis.dwg.dbobj.SnpPackage",
            "PackName" : spkg.get("PackName", "")
        }
        nuevo_SnpPackage.append(tmp)


    # SnpConnect (anidando SnpMtxt y SnpMorigTxt según su índice i)
    nuevo_SnpConnect = []
    for i, conn in enumerate(SnpConnect_list):
        tmp_conn = {
            "Object"      : "com.sunopsis.dwg.dbobj.SnpConnect",
            "ConName"     : conn.get("ConName", ""),
            "ConnectType" : conn.get("ConnectType", ""),
        }
        # Si existe un SnpMtxt en la posición i, se lo anidamos:
        if i < len(SnpMtxt_list):
            m = SnpMtxt_list[i]
            tmp_conn["SnpMtxt"] = {
                "Object": "com.sunopsis.dwg.dbobj.SnpMtxt",
                "ITxt"  : m.get("ITxt", ""),
                "Txt"   : m.get("Txt", "")
            }
        # Si existe un SnpMorigTxt en la posición i, se lo anidamos:
        if i < len(SnpMorig_list):
            mo = SnpMorig_list[i]
            tmp_conn["SnpMorigTxt"] = {
                "Object"     : "com.sunopsis.dwg.dbobj.SnpMorigTxt",
                "OrigineName": mo.get("OrigineName", ""),
                "ITxtOrig"   : mo.get("ITxtOrig", "")
            }
        nuevo_SnpConnect.append(tmp_conn)

    
    # SnpLschema
    nuevo_SnpLschema = []
    for i, ls in enumerate(SnpLschema_list):
        tmp = {
            "Object"      : "com.sunopsis.dwg.dbobj.SnpLschema",
            "LschemaName" : ls.get("LschemaName", "")
        }
        nuevo_SnpLschema.append(tmp)


    # SnpTable (anidando SnpCond -> SnpTxtHeader)
    nuevo_SnpTable = []
    for i, table in enumerate(SnpTable_list):
        tmp_table = {
            "Object"   : "com.sunopsis.dwg.dbobj.SnpTable",
            "TableName": table.get("TableName", ""),
            "ITable"   : table.get("ITable", "")
        }

        # Para cada tabla, buscamos sus SnpCond (matching ITable)
        itable_value = table.get("ITable", "").strip()
        conds_encontradas = [c for c in SnpCond_list if c.get("ITable", "").strip() == itable_value]

        if conds_encontradas:
            tmp_table["SnpCond"] = []
            for cond in conds_encontradas:
                cond_obj = {
                    "Object"     : "com.sunopsis.dwg.dbobj.SnpCond",
                    "ITable"     : cond.get("ITable", ""),
                    "CondName"   : cond.get("CondName", ""),
                    "ITxtCondSql": cond.get("ITxtCondSql", "")
                }
                # Luego, para cada SnpCond, anidamos SnpTxtHeader que coincidan en ITxt = cond["ITxtCondSql"]
                itxt_value = cond.get("ITxtCondSql", "").strip()
                hijos_txtheader = [tx for tx in SnpTxtHeader_list if tx.get("ITxt", "").strip() == itxt_value]

                if hijos_txtheader:
                    cond_obj["SnpTxtHeader"] = []
                    for txh in hijos_txtheader:
                        cond_obj["SnpTxtHeader"].append({
                            "Object": "com.sunopsis.dwg.dbobj.SnpTxtHeader",
                            "ITxt"  : txh.get("ITxt", ""),
                            "Txt"   : txh.get("Txt", "")
                        })
                tmp_table["SnpCond"].append(cond_obj)

        nuevo_SnpTable.append(tmp_table)


    # SnpVar (anidando SnpTxtHeader según ITxtVar o ITxtVarIn)
    nuevo_SnpVar = []
    for i, var in enumerate(SnpVar_list):
        tmp_var = {
            "Object"      : "com.sunopsis.dwg.dbobj.SnpVar",
            "DefV"        : var.get("DefV", ""),
            "LschemaName" : var.get("LschemaName", ""),
            "VarName"     : var.get("VarName", ""),
            "ITxtVar"     : var.get("ITxtVar", ""),
            "ITxtVarIn"   : var.get("ITxtVarIn", "")
        }

        # Decidimos cuál de los dos (ITxtVarIn o ITxtVar) se usa para buscar SnpTxtHeader
        linking_value = var.get("ITxtVarIn", "")
        if linking_value == "" or linking_value == "null":
            linking_value = var.get("ITxtVar", "")

        linking_value = linking_value.strip()
        # Buscamos en SnpTxtHeader
        hijos_txtheader = [tx for tx in SnpTxtHeader_list if tx.get("ITxt", "").strip() == linking_value]
        if hijos_txtheader:
            tmp_var["SnpTxtHeader"] = []
            for txh in hijos_txtheader:
                tmp_var["SnpTxtHeader"].append({
                    "Object": "com.sunopsis.dwg.dbobj.SnpTxtHeader",
                    "ITxt"  : txh.get("ITxt", ""),
                    "Txt"   : txh.get("Txt", "")
                })

        nuevo_SnpVar.append(tmp_var)


    # SnpPop
    nuevo_SnpPop = []
    for i, pop in enumerate(SnpPop_list):
        tmp_pop = {
            "Object"     : "com.sunopsis.dwg.dbobj.SnpPop",
            "LschemaName": pop.get("LschemaName", ""),
            "PopName"    : pop.get("PopName", ""),
            "TableName (Target Table)"  : pop.get("TableName", "")
        }
        nuevo_SnpPop.append(tmp_pop)


    # SnpScenStep (anidando SnpScenTask) - si TaskName1=="Variable" anidamos la variable
    nuevo_SnpScenStep = []
    for step in SnpScenStep_list:
        parent_nno = step.get("Nno", "").strip()
        tmp_step = {
            "Object": "com.sunopsis.dwg.dbobj.SnpScenStep",
            "StepName": step.get("StepName", ""),
            "StepType": step.get("StepType", ""),
            "Nno": parent_nno,
            "TableName (Target Table)": step.get("TableName", ""),
            "OkNextStep": step.get("OkNextStep", ""),
            "KoNextStep": step.get("KoNextStep", "")
        }

        # 1) Recogemos todas las tareas de este paso
        tareas = [t for t in SnpScenTask_list if t.get("Nno", "").strip() == parent_nno]

        if tareas:
            # 2) Si todas son Procedure, las ordenamos por OrdTrt (Order del ODI)
            if all(t.get("TaskName1") == "Procedure" for t in tareas):
                tareas = sorted(
                    tareas,
                    key=lambda t: int(t.get("OrdTrt", 0) or 0)
                )
                
            # si no, dejamos el orden original (como viene por defecto por el export)
            tmp_step["SnpScenTask"] = []
            for tarea in tareas:
                # Aplicamos sql_optimizado a ambos campos
                tmp_task = {
                    "Object":    "com.sunopsis.dwg.dbobj.SnpScenTask",
                    "Nno":       tarea.get("Nno", ""),
                    "TaskName1": tarea.get("TaskName1", ""),
                    "ColTxt":    sql_optimizado(tarea.get("ColTxt", "")),
                    "DefTxt":    sql_optimizado(tarea.get("DefTxt", "")),
                    "TaskName3": tarea.get("TaskName3", ""),
                    "OrdTrt":    tarea.get("OrdTrt", "")
                }

                # Anidamos variables si TaskName1 == "Variable"
                if tarea.get("TaskName1") == "Variable":
                    vars_for_step = [
                        v for v in SnpVar_list
                        if v.get("VarName", "").strip() == step.get("StepName", "").strip()
                    ]
                    if vars_for_step:
                        tmp_task["SnpVar"] = []
                        for var_ in vars_for_step:
                            tmp_task["SnpVar"].append({
                                "Object":  "com.sunopsis.dwg.dbobj.SnpVar",
                                "VarName": var_.get("VarName", ""),
                                "DefV":    var_.get("DefV", "")
                            })

                tmp_step["SnpScenTask"].append(tmp_task)

        nuevo_SnpScenStep.append(tmp_step)


    # Ahora guardamos todas estas estructuras en el diccionario 'resultado'
    resultado["SnpPackage"]   = nuevo_SnpPackage
    resultado["SnpConnect"]   = nuevo_SnpConnect
    resultado["SnpLschema"]   = nuevo_SnpLschema
    resultado["SnpTable"]     = nuevo_SnpTable
    resultado["SnpVar"]       = nuevo_SnpVar
    resultado["SnpPop"]       = nuevo_SnpPop
    resultado["SnpScenStep"]  = nuevo_SnpScenStep

    # Finalmente, serializamos a archivo JSON
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"Diccionario guardado en: {output_filename}")


def main():
    """
    Se ejecuta desde la linea de comandos pasando un archivo XML,
    se detecta el tipo de elemento y se llama a la funcion apropiada.
    """
    if len(sys.argv) != 3:
        print("Uso: python <ruta_del_script.py> <ruta_del_archivo_xml> <nombre_del_archivo_json>")
        sys.exit(1)
    
    xml_file = sys.argv[1]
    json_file = sys.argv[2]
    
    tipo = detectar_tipo_elemento(xml_file)
    if not tipo:
        print("No se pudo detectar el tipo de elemento en el XML.")
        sys.exit(0)
    
    if tipo == "refresh":
        info = extraer_info_refresh(xml_file)
        print(f"Tipo de elemento detectado: {tipo}")
        print("Información extraída:")
        print(info)

    elif tipo == "set":
        info = extraer_info_set(xml_file)
        print(f"Tipo de elemento detectado: {tipo}")
        print("Información extraída:")
        print(info)

    elif tipo == "declare":
        info = extraer_info_declare(xml_file)
        print(f"Tipo de elemento detectado: {tipo}")
        print("Información extraída:")
        print(info)

    elif tipo == "procedure":
        info = extraer_info_procedure(xml_file)
        print(f"Tipo de objeto usado: {tipo}")
        serializar_diccionario_arbol_procedure(info, json_file)

    elif tipo == "interface":
        info = extraer_info_interface(xml_file)
        print(f"Tipo de objeto usado: {tipo}")
        serializar_diccionario_arbol_interface(info, json_file)

    elif tipo == "package":
        info = extraer_info_package(xml_file)
        print(f"Tipo de objeto usado: {tipo}")
        serializar_diccionario_arbol_package(info, json_file)

    else:
        print(f"Tipo de elemento detectado: {tipo}")
        print("Información extraída: {}")


if __name__ == "__main__":
    main()


# Ejecutar: python3 procesamientoXML/parseo/extraer_info_xml_elementos.py XMLs/AT26/Packages/PACKAGE_AT26_TO_FILE_ERROR.xml JSON/AT26/PACKAGE_AT26_TO_FILE_ERROR.json

# python3 procesamientoXML/parseo/extraer_info_xml_elementos.py XMLs/SIG/ODS-OFSAA/Dimensiones/DIM-Comision/PKG_FL_DIM_PROCCESSVAL_STG_ACTIVITY_COMISION.xml JSON/SIG/ODS-OFSAA/Dimensiones/DIM-Comision/PKG_FL_DIM_PROCCESSVAL_STG_ACTIVITY_COMISION.json