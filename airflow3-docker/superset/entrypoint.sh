# 20260407_001 - PoChun Hsu - [Add]     Create admin, database and dataset for superset
# 20260408_001 - PoChun Hsu - [Alter]   code to trigger init_db.py

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

# 20260408_001 >>
echo "🔍 Running init_db.py"
superset shell <<EOF
exec(open('/app/init_db.py').read())
EOF
echo "✅ init_db.py done"
# 20260408_001 <<

echo "🚀 Start Superset"
superset run -h 0.0.0.0 -p 8088
