from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Función que imprimirá "Hola Mundo"
def imprimir_hola():
    print("Hola Mundo pruebita")

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 30),
}

# Definir el DAG
with DAG(
    'hola_mundo_dag',  # Nombre del DAG
    default_args=default_args,
    schedule_interval=None,  # No tiene una ejecución automática programada
    catchup=False
) as dag:

    tarea_hola = PythonOperator(
        task_id='imprimir_hola',  # Identificador de la tarea
        python_callable=imprimir_hola,  # Función que ejecutará
    )

    tarea_hola
