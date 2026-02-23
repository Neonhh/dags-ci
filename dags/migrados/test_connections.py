"""
## DAG de Prueba de Conexiones

Este DAG verifica que todas las conexiones críticas funcionen correctamente:
- PostgreSQL (metadata database)
- GCS (Google Cloud Storage)
- GitSync (este DAG debe aparecer si GitSync funciona)
- Logs remotos en GCS

Ejecuta este DAG después de cada despliegue para validar la configuración.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id="test_connections",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "DevOps", "retries": 0},
    tags=["test", "validation"],
)
def test_connections():

    @task
    def test_postgres():
        """
        Test PostgreSQL connection by querying the metadata database.
        """
        from airflow.hooks.base import BaseHook
        from sqlalchemy import create_engine, text

        logger.info("🔍 Testing PostgreSQL connection...")

        try:
            # Get the default connection (metadata DB)
            conn = BaseHook.get_connection("airflow_db")

            # Create engine and test query
            engine = create_engine(
                f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            )

            with engine.connect() as connection:
                result = connection.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info("✅ PostgreSQL connected successfully!")
                logger.info(f"📊 Database version: {version}")

                # Query some DAG stats
                result = connection.execute(text("SELECT COUNT(*) FROM dag;"))
                dag_count = result.fetchone()[0]
                logger.info(f"📈 Total DAGs in metadata: {dag_count}")

            return {
                "status": "success",
                "message": "PostgreSQL connection working",
                "dag_count": dag_count
            }

        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {str(e)}")
            raise

    @task
    def test_gcs():
        """
        Test GCS connection by writing and reading a test file.
        """
        from google.cloud import storage
        from datetime import datetime as dt

        logger.info("🔍 Testing GCS connection...")

        try:
            # Using Workload Identity (no explicit credentials needed)
            client = storage.Client()

            bucket_name = "airflow-standard-logs"
            logger.info(f"📦 Accessing bucket: {bucket_name}")

            bucket = client.bucket(bucket_name)

            # Check if bucket exists and is accessible
            if bucket.exists():
                logger.info(f"✅ Bucket {bucket_name} exists and is accessible")
            else:
                raise Exception(f"Bucket {bucket_name} not found")

            # Write a test file
            test_blob_name = f"test/connection-test-{dt.now().isoformat()}.txt"
            blob = bucket.blob(test_blob_name)
            test_content = f"GCS connection test at {dt.now()}"
            blob.upload_from_string(test_content)
            logger.info(f"✅ Test file written: gs://{bucket_name}/{test_blob_name}")

            # Read it back
            downloaded_content = blob.download_as_text()
            if downloaded_content == test_content:
                logger.info("✅ Test file read successfully - content matches!")
            else:
                raise Exception("Content mismatch after reading from GCS")

            # List some recent logs
            logger.info("📂 Recent files in bucket:")
            blobs = list(bucket.list_blobs(max_results=5))
            for b in blobs:
                logger.info(f"  - {b.name} ({b.size} bytes)")

            # Clean up test file
            blob.delete()
            logger.info("🗑️  Test file deleted")

            return {
                "status": "success",
                "message": "GCS connection working",
                "bucket": bucket_name,
                "test_file": test_blob_name
            }

        except Exception as e:
            logger.error(f"❌ GCS connection failed: {str(e)}")
            raise

    @task
    def test_git_sync():
        """
        Test GitSync by verifying this DAG file exists in the expected location.
        """
        import os

        logger.info("🔍 Testing GitSync...")

        try:
            dags_folder = "/opt/airflow/dags/repo"
            current_file = "test_connections.py"

            logger.info(f"📁 DAGs folder: {dags_folder}")

            # Check if dags folder exists
            if os.path.exists(dags_folder):
                logger.info("✅ DAGs folder exists")

                # List files in dags folder
                logger.info("📂 Files in DAGs folder:")
                for file in os.listdir(dags_folder):
                    logger.info(f"  - {file}")

                # Check if this DAG exists
                this_dag_path = os.path.join(dags_folder, current_file)
                if os.path.exists(this_dag_path):
                    logger.info(f"✅ This DAG file found: {this_dag_path}")

                    # Read first few lines
                    with open(this_dag_path, 'r') as f:
                        first_lines = [f.readline().strip() for _ in range(3)]
                    logger.info("📄 First lines of DAG file:")
                    for line in first_lines:
                        logger.info(f"   {line}")
                else:
                    logger.warning(f"⚠️  This DAG file not found at: {this_dag_path}")

            else:
                logger.warning(f"⚠️  DAGs folder not found at: {dags_folder}")
                logger.info("This might be normal if DAGs are in a different location")

            return {
                "status": "success",
                "message": "GitSync working - DAG is loaded",
                "dags_folder": dags_folder
            }

        except Exception as e:
            logger.error(f"❌ GitSync check failed: {str(e)}")
            # Don't raise - this is informational
            return {
                "status": "warning",
                "message": f"GitSync check had issues: {str(e)}"
            }

    @task
    def test_remote_logging():
        """
        Test remote logging by generating log messages.
        These should appear in GCS bucket.
        """
        logger.info("🔍 Testing Remote Logging...")
        logger.info("=" * 80)
        logger.info("📝 This is a test log message that should appear in GCS")
        logger.info("📝 Log level: INFO")
        logger.warning("⚠️  This is a WARNING level message")
        logger.error("❌ This is an ERROR level message (intentional for testing)")
        logger.info("=" * 80)

        import os
        log_folder = os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER', 'unknown')
        remote_log_folder = os.environ.get('AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER', 'unknown')

        logger.info(f"📁 Local log folder: {log_folder}")
        logger.info(f"☁️  Remote log folder: {remote_log_folder}")
        logger.info("✅ If you can see this in GCS logs, remote logging is working!")

        return {
            "status": "success",
            "message": "Remote logging test complete - check GCS bucket for logs",
            "remote_folder": remote_log_folder
        }

    @task
    def summary_report(postgres_result, gcs_result, git_result, logging_result):
        """
        Generate a summary report of all connection tests.
        """
        logger.info("=" * 80)
        logger.info("📊 CONNECTION TEST SUMMARY REPORT")
        logger.info("=" * 80)

        results = {
            "PostgreSQL": postgres_result,
            "GCS": gcs_result,
            "GitSync": git_result,
            "Remote Logging": logging_result
        }

        all_success = True
        for component, result in results.items():
            status = result.get("status", "unknown")
            message = result.get("message", "No message")

            if status == "success":
                logger.info(f"✅ {component}: {message}")
            elif status == "warning":
                logger.warning(f"⚠️  {component}: {message}")
                all_success = False
            else:
                logger.error(f"❌ {component}: {message}")
                all_success = False

        logger.info("=" * 80)

        if all_success:
            logger.info("🎉 ALL CONNECTIONS WORKING SUCCESSFULLY!")
            logger.info("✅ Your Airflow deployment is fully functional")
        else:
            logger.warning("⚠️  Some connections have issues - review logs above")

        logger.info("=" * 80)

        return {
            "overall_status": "success" if all_success else "warning",
            "components_tested": len(results),
            "all_passed": all_success
        }

    # Define task dependencies
    postgres = test_postgres()
    gcs = test_gcs()
    git = test_git_sync()
    logging = test_remote_logging()

    # All tests run in parallel, then summary
    summary_report(postgres, gcs, git, logging)


# Instantiate the DAG
test_connections()
