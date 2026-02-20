# Crear la carpeta del proyecto
mkdir -p mi-proyecto-migracion/{dags,scripts,tests,xml_source}
cd mi-proyecto-migracion

# Crear el entorno virtual
python3 -m venv venv

#Info para conectar con los otros repos
#https://gemini.google.com/app/e346f9dc3721425c?hl=es

# Activar el entorno
source venv/bin/activate

# Actualizar pip e instalar lo necesario
pip install --upgrade pip
pip install apache-airflow pytest

#Comando para probar
pytest tests/test_integrity.py

# Correr en local
astro dev start
# admin, admin (?)

### POD GCLOUD ###
gcloud container clusters get-credentials airflow --zone us-east1-b --project airflowtest-475719

kubectl -n airflow get pods

gcloud container clusters get-credentials airflow --zone us-east1-b --project airflowtest-475719


### GOOGLE CLOUD COMPOSER ###

# Para ver tu ID real
gcloud projects list

# Para fijarlo
gcloud config set project pruebas-pipeline

# Aqui me pedira billing
gcloud services enable composer.googleapis.com