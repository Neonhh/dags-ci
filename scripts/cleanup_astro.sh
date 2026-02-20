#!/bin/bash

echo "🛑 Deteniendo Astro y limpiando contenedores..."
astro dev stop

echo "🧹 Eliminando proxy si quedó colgado..."
docker rm -f astro_sql_proxy 2>/dev/null

echo "🔌 Liberando el puerto 5432 (Postgres local)..."
sudo systemctl stop postgresql 2>/dev/null
sudo fuser -k 5432/tcp 2>/dev/null

echo "🌐 Limpiando redes de Docker huérfanas..."
docker network prune -f

echo "🗑️ Limpiando contenedores residuales..."
docker container prune -f

echo "✅ Limpieza completada. Intenta ejecutar: astro dev start"
