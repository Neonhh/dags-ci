# Airflow Local con Astro CLI

Entorno de desarrollo local de Apache Airflow usando Astro CLI con conexión segura a Cloud SQL mediante proxy.

## Requisitos

### Software necesario

- **Docker Desktop** (v20.10 o superior)
- **Astro CLI** (v1.20 o superior)

  ```bash
  # Instalación en Mac
  brew install astro

  # Instalación en Linux/Windows
  # Ver: https://docs.astronomer.io/astro/cli/install-cli
  ```

- **Python 3.12+** (para scripts auxiliares)
- **gcloud CLI** (para administración de GCP)

### Credenciales y configuración en GCP

#### 1. Service Account para Cloud SQL Proxy

Se necesita un service account con el rol **Cloud SQL Client** para permitir la conexión al proxy:

```bash
# Crear service account
gcloud iam service-accounts create airflow-sql-proxy \
  --display-name="Airflow Cloud SQL Proxy" \
  --project=airflowtest-475719

# Asignar rol Cloud SQL Client
gcloud projects add-iam-policy-binding airflowtest-475719 \
  --member="serviceAccount:airflow-sql-proxy@airflowtest-475719.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"

# Generar key
gcloud iam service-accounts keys create secrets/cloud-sql-proxy-service-account.json \
  --iam-account=airflow-sql-proxy@airflowtest-475719.iam.gserviceaccount.com
```

#### 2. Configuración de Cloud SQL

La instancia de Cloud SQL debe tener **IP pública habilitada** para permitir conexiones a través del proxy desde fuera de GCP:

```bash
# Habilitar IP pública (si está deshabilitada)
gcloud sql instances patch airflow-metadata \
  --assign-ip \
  --project=airflowtest-475719
```

**Nota**: El proxy no se conecta directamente a la IP pública, sino que usa el API de Cloud SQL. La IP pública es necesaria para que el API permita la conexión.

#### 3. Service Account para acceso a GCS

Se necesita un service account con permisos para acceder a buckets de Google Cloud Storage:

```bash
# Este service account debe tener rol Storage Object Viewer/Admin
# Guardar la key en: secrets/gcp-service-account-key.json
```

## Estructura de Secrets

Todos los archivos de credenciales se almacenan en la carpeta `secrets/` (esta carpeta está excluida del control de versiones):

```
secrets/
├── cloud-sql-proxy-service-account.json  # Credenciales del proxy
├── gcp-service-account-key.json          # Credenciales para acceso a GCS
└── connections.json                       # Definición de conexiones de Airflow
```

### Archivo connections.json

Define todas las conexiones que se crearán automáticamente en Airflow:

```json
[
  {
    "conn_id": <tu_conn_id_aqui>,
    "conn_type": "postgres",
    "host": "astro_sql_proxy",
    "port": 5432,
    "database": <tu_database_aqui>,
    "login": <tu_user_aqui>,
    "password": <tu_password_aqui>
  },
  {
    "conn_id": "google_cloud_default",
    "conn_type": "google_cloud_platform",
    "extra": {
      "project": <tu_project_id_aqui>,
      "keyfile_path": "secrets/gcp-service-account-key.json",
      "keyfile_dict": <tu_json_aqui>
    }
  }
]
```

**Credenciales requeridas:**

- **Bases de datos PostgreSQL**: Usuario y contraseña con acceso a las bases en Cloud SQL
- **SMTP**: Credenciales para envío de emails (si se usan EmailOperator)
  - **Anteriormente**: Se usaba el correo corporativo `soporte-da@sannet.com.ve`
  - **Actualmente (recomendado)**: Se usa [Resend](https://resend.com) durante las pruebas
  - Ver sección "Configuración de Email" más abajo para detalles
- **GCS**: Key del service account con acceso a los buckets necesarios

## Inicio rápido

### 1. Clonar y configurar secrets

```bash
# Clonar el repositorio
cd astroAirflow

# Crear carpeta de secrets (si no existe)
mkdir -p secrets

# Copiar los archivos de credenciales necesarios a secrets/
# - cloud-sql-proxy-service-account.json
# - gcp-service-account-key.json
# - connections.json
```

### 2. Iniciar Airflow

```bash
astro dev start
```

Esto levantará automáticamente:

- Airflow Webserver en http://localhost:8080
- Scheduler, Triggerer y Dag Processor
- Base de datos PostgreSQL local (metadata de Airflow)
- **Cloud SQL Proxy** (automáticamente desde `docker-compose.override.yml`)

**Credenciales por defecto**: `admin / admin`

**Nota importante**: El archivo debe llamarse `docker-compose.override.yml` (extensión `.yml`, no `.yaml`) para que Astro CLI lo detecte automáticamente.

### 3. Crear conexiones en Airflow (primera vez)

```bash
python createConnections.py
```

Este script lee `secrets/connections.json` y crea automáticamente todas las conexiones en Airflow usando el CLI.

## Comandos útiles

### Ver logs

```bash
# Logs de Airflow
astro dev logs

# Logs del scheduler específicamente
astro dev logs -s

# Logs del proxy (levantado automáticamente por Astro)
docker logs -f astro_sql_proxy
```

### Reiniciar servicios

```bash
# Reiniciar Airflow (y el proxy automáticamente)
astro dev restart
```

### Detener todo

```bash
# Detener Airflow y el proxy
astro dev stop
```

### Ejecutar comandos de Airflow

```bash
# Listar DAGs
astro dev run dags list

# Testear una tarea
astro dev run tasks test <dag_id> <task_id> <execution_date>

# Crear un usuario
astro dev run users create --username admin --password admin --role Admin
```

## Conexiones disponibles

### Desde DAGs de Airflow

Los DAGs pueden conectarse a las bases de datos usando el proxy:

- **Host**: `astro_sql_proxy`
- **Port**: `5432`
- **Databases**: `ods`, `at26`, `repodataprd`

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id='ods')
records = hook.get_records("SELECT * FROM tabla")
```

### Desde tu máquina (DBeaver, pgAdmin, etc.)

Para conectarte directamente desde herramientas locales:

- **Host**: `localhost`
- **Port**: `5433`
- **Database**: `ods`, `at26`, o `repodataprd`
- **Username**: (ver `secrets/connections.json`)
- **Password**: (ver `secrets/connections.json`)

## Configuración de Email

### Configuración rápida con Resend

Para el envío de emails desde los DAGs, se usa **Resend** como proveedor SMTP:

#### 1. Obtener API Key de Resend

1. Registrarse en [https://resend.com](https://resend.com)
2. En el dashboard, ir a **API Keys** y crear una nueva
3. Copiar la API Key (formato: `re_xxxxxxxxxxxxx`)

#### 2. Configurar variables de entorno en `.env`

Agregar las siguientes variables al archivo `.env` en la raíz del proyecto:

```bash
# Configuración SMTP global para Airflow usando Resend
AIRFLOW__SMTP__SMTP_HOST=smtp.resend.com
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=resend
AIRFLOW__SMTP__SMTP_PASSWORD=re_your_api_key_here
AIRFLOW__SMTP__SMTP_MAIL_FROM=onboarding@resend.dev
```

**Nota**: Reemplazar `re_your_api_key_here` con tu API Key de Resend.

#### 3. Convertir DAGs existentes (si usaban EmailOperator)

Si tus DAGs usan `EmailOperator`, conviértelos automáticamente a `send_email()`:

```bash
# Ver qué cambios se aplicarán (sin modificar archivos)
python convert_email_operator.py --dry-run

# Aplicar conversión
python convert_email_operator.py

# Sin crear backups
python convert_email_operator.py --no-backup
```

Este script:

- ✅ Convierte todos los `EmailOperator` a funciones con `send_email()`
- ✅ Mantiene el mismo contenido del email (to, subject, html_content)
- ✅ Actualiza los imports automáticamente
- ✅ Crea backups de seguridad (opcional)

#### 4. Actualizar destinatarios de emails

**Importante**: Resend solo envía emails al correo asociado a tu cuenta (durante pruebas). Para cambiar los destinatarios en todos los DAGs:

```bash
python changeEmail.py
```

Este script:

- 🔍 Encuentra automáticamente todos los correos en los DAGs
- 📋 Muestra dónde aparece cada correo
- ✏️ Te permite reemplazar selectivamente cada uno

#### 5. Reiniciar Airflow

```bash
astro dev restart
```

### 📧 Cómo enviar emails en tus DAGs

Usa la función `send_email()` en lugar de `EmailOperator`:

```python
from airflow.decorators import task
from airflow.utils.email import send_email

@task
def send_notification():
    """Envía un email de notificación"""
    send_email(
        to='destinatario@example.com',
        subject='Proceso completado',
        html_content='<h3>El proceso finalizó exitosamente</h3>'
    )
```

**Ventajas**:

- ✅ Usa la configuración global de `.env`
- ✅ Funciona con alertas automáticas (`email_on_failure`, `email_on_retry`)
- ✅ Compatible con Resend SMTP
- ✅ No requiere conexiones adicionales en Airflow

### 📝 Notas

- **No se necesita** configurar conexiones SMTP en `connections.json`
- **No se necesita** especificar `conn_id` en el código
- Las alertas automáticas (`email_on_failure`) funcionan automáticamente con esta configuración
- En producción (GKE, Cloud Composer), esta misma configuración funciona correctamente

## Scripts auxiliares

### changeEmail.py

Encuentra y reemplaza correos electrónicos en todos los DAGs. Útil porque Resend solo envía emails al correo asociado a tu cuenta durante pruebas:

```bash
python changeEmail.py
```

### createConnections.py

Crea automáticamente las conexiones definidas en `secrets/connections.json`:

```bash
python createConnections.py
```

## Troubleshooting

### El proxy no se conecta a Cloud SQL

1. Verificar que la instancia esté corriendo:
   ```bash
   gcloud sql instances describe airflow-metadata --project=airflowtest-475719
   ```
2. Verificar permisos del service account:
   ```bash
   gcloud projects get-iam-policy airflowtest-475719 \
     --flatten="bindings[].members" \
     --filter="bindings.members:airflow-sql-proxy@*"
   ```
3. Ver logs del proxy:
   ```bash
   docker logs astro_sql_proxy
   ```

### El proxy no se inicia automáticamente

El proxy se levanta automáticamente con Astro CLI. Si no se inicia:

1. Verifica que el archivo se llame exactamente `docker-compose.override.yml` (extensión `.yml`, no `.yaml`)
2. Verifica que el archivo esté en la raíz del proyecto
3. Detén y reinicia Astro:
   ```bash
   astro dev stop
   astro dev start
   ```
4. Verifica que el contenedor esté corriendo:
   ```bash
   docker ps | grep astro_sql_proxy
   ```

### Error "network not found" al iniciar el proxy

Debes iniciar primero Airflow con `astro dev start` para que se cree la red de Docker.

### Conexiones no aparecen en Airflow

Verifica que el script se ejecutó correctamente:

```bash
docker exec airflow_655ea3-scheduler-1 airflow connections list
```

### DAGs no aparecen en la UI

1. Verificar errores en los DAGs:
   ```bash
   astro dev run dags list-import-errors
   ```
2. Los DAGs deben estar en la carpeta `dags/`
3. Reiniciar el scheduler si es necesario

## Arquitectura

```
┌─────────────────┐
│  Tu Máquina     │
│  (localhost)    │
└────────┬────────┘
         │
    ┌────┴─────────────────────┐
    │                          │
┌───▼─────────┐      ┌─────────▼─────┐
│   Airflow   │◄────►│  SQL Proxy    │
│  (Astro)    │      │ (Container)   │
└─────┬───────┘      └────────┬──────┘
      │                       │
┌─────▼─────────┐    ┌────────▼──────────┐
│ Google Cloud  │    │   Google Cloud    │
│ Storage       │    │   Cloud SQL       │
│ (Buckets)     │    │   (PostgreSQL)    │
└───────────────┘    └───────────────────┘
```

## Seguridad

⚠️ **Importante**:

- **NUNCA** commitear archivos de la carpeta `secrets/` al repositorio
- Usar variables de entorno para datos sensibles en producción
- Rotar periódicamente las credenciales de service accounts
- Limitar permisos de service accounts al mínimo necesario

## Más información

- [Documentación de Astro CLI](https://docs.astronomer.io/astro/cli/overview)
- [Cloud SQL Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy)
- [Apache Airflow](https://airflow.apache.org/docs/)
# dags-ci
