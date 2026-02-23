from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    'test_email_smtp',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'email'],
    default_args={'email_on_failure': True, 'email': 'daniel.figueroa@kreadata.com'}
) as dag:

    def send_email_callable(**kwargs):
        """Envía email usando send_email() - Convertido desde EmailOperator"""
        from airflow.utils.email import send_email
        send_email(
            to="daniel.figueroa@kreadata.com",
            subject="Test SMTP - Airflow GKE",
            html_content="""

            <h3>¡Configuración SMTP Exitosa!</h3>
            <p>Este email confirma que Airflow puede enviar correos desde GKE.</p>
            <h4>Detalles del envío:</h4>
            <ul>
                <li><b>Servidor SMTP:</b> smtp.resend.com:465 (SSL)</li>
                <li><b>DAG:</b> {{ dag.dag_id }}</li>
                <li><b>Task:</b> {{ task.task_id }}</li>
                <li><b>Execution Date:</b> {{ ds }}</li>
                <li><b>Run ID:</b> {{ run_id }}</li>
            </ul>
            <p style="color: green;">✅ Si recibes este email, el EmailOperator está funcionando correctamente.</p>
        
            """
        )

    send_email = PythonOperator(
        task_id="send_test_email",
        python_callable=send_email_callable,
        dag=dag
    )

    # Esta tarea SIEMPRE falla, excepto si se pasa {"skip_failure": true} en Configuration JSON desde la UI
    @task
    def fail_task(**context):
        """
        Tarea que falla por defecto para probar las alertas de email.
        
        Para hacer que esta tarea pase exitosamente, ejecuta el DAG manualmente desde la UI:
        1. Click en "Trigger DAG"
        2. Expande "Advanced configuration"
        3. En "Configuration JSON" ingresa: {"skip_failure": true}
        4. Click "Trigger"
        
        La tarea verificará el parámetro skip_failure del dag_run.conf y:
        - Si skip_failure=true: La tarea pasa exitosamente ✅
        - Si skip_failure=false o no está presente: La tarea falla ❌ (para probar alertas)
        """
        # Obtener la configuración del DAG run (Configuration JSON desde la UI)
        dag_run_conf = context.get('dag_run').conf or {}
        skip_failure = dag_run_conf.get('skip_failure', False)

        if skip_failure:
            print("✅ skip_failure=true detectado en Configuration JSON")
            print("✅ La tarea pasará exitosamente sin fallar")
            return "Success - failure skipped by configuration"
        else:
            print("❌ skip_failure no está presente o es false en Configuration JSON")
            print("❌ La tarea fallará para probar las alertas de email")
            raise Exception(
                "Esta tarea está diseñada para fallar y probar el email de alerta. "
                "Para que pase exitosamente, ejecuta el DAG con Configuration JSON: "
                '{"skip_failure": true}'
            )

    # Instanciar la tarea y definir el orden: primero enviar el email, luego ejecutar la tarea que falla
    intentionally_fail = fail_task()
    intentionally_fail >> send_email
