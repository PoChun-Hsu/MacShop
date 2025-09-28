# 20250926_001 - PoChun Hsu - [Alter]   Improved build efficiency by reusing pre-downloaded spark.tgz.
# 20250928_001 - PoChun Hsu - [Add]     Enabled Airflow to trigger jobs via Docker socket, delegating actual execution to Spark.

FROM apache/airflow:2.11.0

USER airflow

ARG AIRFLOW_VERSION=2.11.0
# Airflow 2.11 預設 Python 3.12
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 20250906_003
COPY --chown=airflow:root airflow/requirements.airflow.txt /requirements.txt

# 提高安裝穩定性
ENV PIP_DEFAULT_TIMEOUT=1200 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"

# 20250906_003 >>
# === 如果 Airflow 要呼叫 SparkSubmit，需要 Spark client/jar ===
# 這裡不用整包 Spark，只要能找到 jar
# COPY 來自 spark/jars 的 PostgreSQL JDBC driver
# 20250925_001
COPY spark/jars/postgresql-42.7.3.jar /opt/spark/jars/ 
# 20250906_003 <<

# 20250928_001 >>
USER root
RUN apt-get update && apt-get install -y docker.io
USER airflow
# 20250928_001 <<
