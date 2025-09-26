# 20250926_001 - PoChun Hsu - [Alter]    Improved build efficiency by reusing pre-downloaded spark.tgz.

# Dockerfile.spark
FROM bitnami/spark:3.5.0

# 使用 root 來放置檔案
USER 0

# 20250926_001 >>
# 20250926_001 <<

# 20250926_001 >>
# === 使用本機已下載好的 Spark 壓縮檔 ===
# 注意：docker-compose build 時 context 是 ./spark
# 但檔案放在 airflow/vendor，所以要 COPY ../airflow/vendor/...
COPY airflow/vendor/spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz

RUN mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# === 複製已下載好的 PostgreSQL JDBC driver ===
COPY spark/jars/postgresql-42.7.3.jar /opt/spark/jars/
# 20250926_001 <<

# 建立預設 apps 目錄，供你用 volume 掛載
RUN mkdir -p /opt/spark-apps

# 安裝 PySpark
RUN pip install --no-cache-dir pyspark==3.5.0

# 還原使用者
USER 1001
