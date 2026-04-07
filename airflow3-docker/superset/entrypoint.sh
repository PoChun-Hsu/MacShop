# 20260407_001 - PoChun Hsu - [Add]     Create admin, database and dataset for superset

#!/bin/bash
set -e

echo "🔧 Superset init start"

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname admin \
  --lastname admin \
  --email admin@admin.com \
  --password admin || true

superset init

echo "🔧 Create database connection"
superset shell < /app/init_db.py

echo "🚀 Start Superset"
superset run -h 0.0.0.0 -p 8088
