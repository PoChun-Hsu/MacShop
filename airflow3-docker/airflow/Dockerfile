# --------------------------------------------------------------------
# Base image: 官方 Apache Airflow 映像（3.1.0）
# 官方映像預設使用非 root 使用者 airflow (UID 50000)
# --------------------------------------------------------------------
FROM apache/airflow:3.1.0

# --------------------------------------------------------------------
# 安裝 Python 依賴
# --------------------------------------------------------------------
COPY ./airflow/requirements.airflow.txt /requirements.airflow.txt
RUN pip install --no-cache-dir -r /requirements.airflow.txt

# --------------------------------------------------------------------
# 以下需要對 /opt/airflow 目錄寫入，因此暫時切換為 root
# --------------------------------------------------------------------
USER root

# 複製初始化腳本到容器內
# airflow-init.sh 在專案根目錄下的 ./scripts/
COPY ./airflow/scripts/airflow-init.sh /opt/airflow/scripts/airflow-init.sh

# 設定腳本可執行權限
RUN chmod +x /opt/airflow/scripts/airflow-init.sh

# 複製 Airflow 設定檔
# airflow.cfg 放在 ./airflow/config/airflow.cfg
# COPY ./airflow/config/airflow.cfg /opt/airflow/airflow.cfg

# --------------------------------------------------------------------
# 切回 airflow 使用者（預設非 root，安全性較高）
# --------------------------------------------------------------------
USER airflow

# --------------------------------------------------------------------
# 預設 entrypoint 保持官方設定
# --------------------------------------------------------------------
