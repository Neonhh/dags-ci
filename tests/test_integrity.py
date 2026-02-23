from airflow.models import DagBag
import os

def test_dag_import_errors():
    # Buscamos en la carpeta /dags de tu proyecto
    dag_path = os.path.join(os.path.dirname(__file__), '..','dags')
    dag_bag = DagBag(dag_folder=dag_path, include_examples=False)

    # Si hay errores, formateamos un mensaje claro para el reporte
    import_errors = dag_bag.import_errors

    error_msg = ""
    if import_errors:
        for file, error in import_errors.items():
            error_msg += f"\nArchivo: {file}\nError: {error}\n"

    assert len(import_errors) == 0, f"Se encontraron errores en los DAGs:{error_msg}"

def test_dag_ids_are_unique():
    """
    Verifica que no existan IDs de DAG duplicados entre carpetas (migration vs native).
    """
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    all_dag_ids = list(dagbag.dags.keys())
    unique_dag_ids = set(all_dag_ids)

    assert len(all_dag_ids) == len(unique_dag_ids), "Existen IDs de DAG duplicados en el proyecto."
