#!/usr/bin/env bash
set -euo pipefail
shopt -s inherit_errexit

# 彩色輸出
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

log() { echo -e "${GREEN}▶︎${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
error() { echo -e "${RED}❌${RESET} $*"; }

log "Starting airflow-init ..."

# ---------------------------------------
# 0. 預設環境變數
# ---------------------------------------
: "${AIRFLOW_DEFAULT_USER_USERNAME:=admin}"
: "${AIRFLOW_DEFAULT_USER_FIRSTNAME:=Admin}"
: "${AIRFLOW_DEFAULT_USER_LASTNAME:=User}"
: "${AIRFLOW_DEFAULT_USER_EMAIL:=admin@example.com}"
: "${AIRFLOW_DEFAULT_USER_PASSWORD:=admin}"
: "${AIRFLOW__CORE__DAGS_FOLDER:=/opt/airflow/dags}"
: "${AIRFLOW__LOGGING__BASE_LOG_FOLDER:=/opt/airflow/logs}"
: "${AIRFLOW__CORE__PLUGINS_FOLDER:=/opt/airflow/plugins}"
: "${AIRFLOW_HOME:=/opt/airflow}"

# ---------------------------------------
# 1. 多進程安全鎖（避免 parallel init）
# ---------------------------------------
LOCK_FILE="/tmp/airflow-init.lock"
exec 200>"$LOCK_FILE"
log "Acquiring initialization lock..."
flock -x 200
log "Lock acquired. Proceeding with initialization."

# ---------------------------------------
# 2. 等待 Postgres 可用
# ---------------------------------------
log "Checking PostgreSQL connection..."
RETRY_COUNT=0
until pg_isready -h postgres -U airflow -d airflow >/dev/null 2>&1; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ "$RETRY_COUNT" -gt 30 ]; then
    error "PostgreSQL not ready after 30 tries, exiting."
    exit 1
  fi
  echo "⏳ Waiting for PostgreSQL to be ready ($RETRY_COUNT/30)..."
  sleep 2
done
log "PostgreSQL is ready."

# ---------------------------------------
# 3. 初始化 / 升級資料庫
# ---------------------------------------
log "Migrating metadata DB..."
(
  flock -n 201 || { warn "Another init process is migrating DB, waiting..."; flock 201; }
  if airflow db migrate; then
    log "DB migrated successfully."
  else
    warn "DB migration failed, attempting 'airflow db upgrade'..."
    airflow db upgrade || error "DB upgrade failed."
  fi
) 201>/tmp/airflow-db.lock

# ---------------------------------------
# 4. 建立或更新 Admin 帳號
# ---------------------------------------
log "Ensuring default admin user exists..."
set +e
airflow users update-password \
  --username "${AIRFLOW_DEFAULT_USER_USERNAME}" \
  --password "${AIRFLOW_DEFAULT_USER_PASSWORD}" >/dev/null 2>&1
RC=$?
set -e

if [ "$RC" -ne 0 ]; then
  log "Admin user not found, creating..."
  airflow users create \
    --role Admin \
    --username "${AIRFLOW_DEFAULT_USER_USERNAME}" \
    --firstname "${AIRFLOW_DEFAULT_USER_FIRSTNAME}" \
    --lastname  "${AIRFLOW_DEFAULT_USER_LASTNAME}" \
    --email     "${AIRFLOW_DEFAULT_USER_EMAIL}" \
    --password  "${AIRFLOW_DEFAULT_USER_PASSWORD}" >/dev/null 2>&1
  log "Admin user created (${AIRFLOW_DEFAULT_USER_USERNAME})."
else
  log "Admin password updated (${AIRFLOW_DEFAULT_USER_USERNAME})."
fi

# ---------------------------------------
# 5. 確認資料夾狀態
# ---------------------------------------
log "Checking Airflow folders..."
for DIR in "${AIRFLOW__CORE__DAGS_FOLDER}" "${AIRFLOW__LOGGING__BASE_LOG_FOLDER}" "${AIRFLOW__CORE__PLUGINS_FOLDER}"; do
  if [ ! -d "$DIR" ]; then
    warn "Folder not found, creating: $DIR"
    mkdir -p "$DIR"
  fi
done

log "Effective folders:"
echo "  DAGS:    ${AIRFLOW__CORE__DAGS_FOLDER}"
echo "  LOGS:    ${AIRFLOW__LOGGING__BASE_LOG_FOLDER}"
echo "  PLUGINS: ${AIRFLOW__CORE__PLUGINS_FOLDER}"
echo "  AIRFLOW_HOME: ${AIRFLOW_HOME}"

# ---------------------------------------
# 6. 等待 DAGs Volume 掛載
# ---------------------------------------
log "Waiting for DAGs volume to be fully mounted..."
for i in {1..10}; do
  if [ -d "${AIRFLOW__CORE__DAGS_FOLDER}" ] && [ "$(ls -A "${AIRFLOW__CORE__DAGS_FOLDER}")" ]; then
    log "DAGs volume mounted successfully."
    break
  fi
  echo "⏳ Waiting ($i/10)..."
  sleep 2
done

# ---------------------------------------
# 7. 強制重新序列化 DAGs
# ---------------------------------------
log "Reserializing DAGs to refresh cache..."
(
  flock -n 202 || { warn "Another init process is reserializing DAGs, waiting..."; flock 202; }
  if airflow dags reserialize --no-confirm >/dev/null 2>&1; then
    log "DAG reserialization completed."
  else
    warn "DAG reserialize failed; Airflow will retry automatically."
  fi
) 202>/tmp/airflow-dags.lock

# ---------------------------------------
# 8. 建立 Airflow Connections
# ---------------------------------------
log "Upserting Airflow connections..."

# postgres_default
log "→ Upsert connection: postgres_default"
airflow connections delete postgres_default >/dev/null 2>&1 || true
airflow connections add postgres_default \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-port '5432' \
  --conn-login 'airflow' \
  --conn-password 'airflow' \
  --conn-schema 'airflow'
log "postgres_default connection ready."

# google_cloud_default
log "→ Upsert connection: google_cloud_default"
GCP_KEY_PATH="${GCP_KEY_PATH:-/opt/airflow/keys/starry-center-405211-04dd3e1fa083.json}"
GCP_PROJECT="${GCP_PROJECT:-starry-center-405211}"
GCP_SCOPE="${GCP_SCOPE:-https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive}"

if [ ! -f "$GCP_KEY_PATH" ]; then
  error "GCP key not found at $GCP_KEY_PATH"
  exit 1
fi

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

airflow connections delete google_cloud_default >/dev/null 2>&1 || true
airflow connections add google_cloud_default --conn-json "$(cat /tmp/gcp_conn.json)"
log "google_cloud_default connection ready."

# ---------------------------------------
# 9. 建立 Airflow Pools
# ---------------------------------------
log "Upserting Airflow pools..."

POOL_NAME="ptt_formatted_build_pool"
POOL_SLOTS=1
POOL_DESC="Pool for Spark formatted build job"

EXISTING_POOL=$(airflow pools list | grep -w "${POOL_NAME}" || true)
if [ -z "$EXISTING_POOL" ]; then
  log "→ Creating pool: ${POOL_NAME}"
  airflow pools set "${POOL_NAME}" "${POOL_SLOTS}" "${POOL_DESC}" >/dev/null 2>&1
  log "Pool created (${POOL_NAME}, slots=${POOL_SLOTS})."
else
  log "Pool already exists (${POOL_NAME}), skipping creation."
fi


# ---------------------------------------
# 10. 額外健康檢查
# ---------------------------------------
log "Verifying Airflow environment..."
airflow version || warn "Airflow version check failed."
if airflow plugins list >/dev/null 2>&1; then
  log "Airflow plugins loaded successfully."
else
  warn "Some plugins may not have loaded correctly."
fi

log "✅ airflow-init finished successfully."
flock -u 200
exit 0
