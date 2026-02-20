from airflow.decorators import dag, task
from pendulum import datetime
# Importamos el Hook necesario
from airflow.providers.google.cloud.hooks.gcs import GCSHook

@dag(
    dag_id="test_gcs_simple_hook", # Cambié el ID para evitar conflictos
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "gcs"],
)
def test_gcs_simple():
    
    @task
    def check_gcs_access():
        """Test basic GCS access using Airflow Connection"""
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info("Testing GCS access via Hook...")
        
        try:
            # 1. Instanciamos el Hook apuntando a tu conexión de Airflow
            gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
            
            # 2. Obtenemos el cliente autenticado desde el Hook
            # Esto devuelve un objeto google.cloud.storage.Client igual que antes,
            # pero ya inyectado con las credenciales del JSON.
            client = gcs_hook.get_conn() 
            
            bucket_name = "airflow-standard-logs"
            
            bucket = client.bucket(bucket_name)
            exists = bucket.exists()
            
            logger.info(f"Bucket {bucket_name} exists: {exists}")
            logger.info(f"Project: {client.project}")
            
            if exists:
                # Try to list first 3 objects
                blobs = list(bucket.list_blobs(max_results=3))
                logger.info(f"Found {len(blobs)} objects in bucket")
                for blob in blobs:
                    logger.info(f"  - {blob.name}")
            
            return {"status": "success", "bucket": bucket_name, "exists": exists}
            
        except Exception as e:
            logger.error(f"GCS access failed: {e}")
            raise
    
    check_gcs_access()

test_gcs_simple()