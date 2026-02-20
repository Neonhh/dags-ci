import sys
import xml.etree.ElementTree as ET

def extraer_clases(xml_file):
    """Extrae las clases únicas de los elementos <Object> en el XML."""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    clases = set()
    # Itera sobre todos los elementos "Object" y extrae el atributo "class"
    for obj in root.findall('.//Object'):
        clase = obj.get('class')
        if clase:
            clases.add(clase)
    return list(clases)

def main():
    if len(sys.argv) != 2:
        print("Uso: python3 <ruta_archivo_python> <ruta_archivo_xml>")
        sys.exit(1)
    
    xml_file = sys.argv[1]
    try:
        clases = extraer_clases(xml_file)
        print("Clases únicas encontradas en el XML:")
        for c in clases:
            print(c)
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")

if __name__ == "__main__":
    main()


# Ejecutar: python3 procesamientoXML/parseo/extraer_clases.py XMLs/AT26/Packages/subPackages/PACKAGE_AT26_PRINCIPAL.xml