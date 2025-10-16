#!/usr/bin/env bash
set -euo pipefail

echo "==> Airflow DB migrate"
airflow db migrate

echo "==> Create admin user if missing"
# 避免受 header 影響，只比對實際使用者名稱欄
if ! airflow users list | awk 'NR>2 {print $1}' | grep -qx admin; then
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

echo "==> Upsert connection: postgres_default (conn_type=postgres)"
# 刪除舊的（若先前以 URI 建成 postgresql+psycopg2 會殘留），再正確建立
airflow connections delete postgres_default || true
airflow connections add postgres_default \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-port '5432' \
  --conn-login 'airflow' \
  --conn-password 'airflow' \
  --conn-schema 'airflow'

python - <<'PY'
import json, subprocess, sys
# 列出所有連線 (json)
out = subprocess.check_output(["airflow","connections","list","-o","json"], text=True)
for c in json.loads(out):
    if c.get("conn_type") == "postgresql+psycopg2":
        conn_id = c["conn_id"]
        print(f"Deleting legacy connection with bad type: {conn_id}", file=sys.stderr)
        subprocess.call(["airflow","connections","delete",conn_id])
PY

echo "==> Upsert connection: google_cloud_default"

# 從環境讀取，並提供安全預設（可依你實際專案ID/Scope調整）
GCP_KEY_PATH="${GCP_KEY_PATH:-/opt/airflow/keys/starry-center-405211-04dd3e1fa083.json}"
GCP_PROJECT="${GCP_PROJECT:-starry-center-405211}"
GCP_SCOPE="${GCP_SCOPE:-https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive}"

# 檢查金鑰是否存在（避免之後 task 才失敗）
if [ ! -f "$GCP_KEY_PATH" ]; then
  echo "ERROR: GCP key not found at $GCP_KEY_PATH" >&2
  exit 1
fi

airflow connections delete google_cloud_default || true
cat > /tmp/gcp_conn.json <<JSON
{
  "conn_type": "google_cloud_platform",
  "extra": {
    "extra__google_cloud_platform__key_path": "${GCP_KEY_PATH}",
    "extra__google_cloud_platform__project": "${GCP_PROJECT}",
    "extra__google_cloud_platform__scope": "${GCP_SCOPE}"
  }
}
JSON

airflow connections add google_cloud_default --conn-json "$(cat /tmp/gcp_conn.json)"

echo "==> Import Airflow pools (if any)"
POOLS_JSON="/opt/airflow/scripts/pools.json"
if [ -f "$POOLS_JSON" ]; then
  airflow pools import "$POOLS_JSON"
else
  echo "No pools.json found at $POOLS_JSON, skipping import"
fi

echo "==> Done"
