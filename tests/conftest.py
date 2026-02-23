import sys
import os

# Obtenemos la ruta absoluta de la carpeta 'dags'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DAGS_DIR = os.path.join(BASE_DIR, "dags")

# La añadimos al camino de búsqueda de Python
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)
