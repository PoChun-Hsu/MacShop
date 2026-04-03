# 20250906_003 - PoChun Hsu - [Add]     Spark install into Dockerfile.spark
# 20250926_001 - PoChun Hsu - [Alter]   Improved build efficiency by reusing pre-downloaded spark.tgz.
# 20251015_001 - PoChun Hsu - [Alter]   Add OpenJDK and foreground mode support (SPARK_NO_DAEMONIZE=true)
# 20251016_001 - PoChun Hsu - [Drop]    pyspark. No need in the current structure.
# 20251016_002 - PoChun Hsu - [Alter]   Using slim package
# 20251016_003 - PoChun Hsu - [Add]     python
# 20251016_004 - PoChun Hsu - [Alter]   spark-3.5.0-bin-hadoop3.tgz to Spark folder.
# 20251016_005 - PoChun Hsu - [Add]     Make sure authority to execute spark scripts
# 20251102_001 - PoChun Hsu - [Add]     pip install from requirements.spark.txt 

# 20251015_001  
# FROM openjdk:17-jdk-slim 
# 👉 改為 Spark 官方建議 Java 17 runtime（較穩定）
FROM eclipse-temurin:17-jdk-jammy

USER root

# 20251016_003 - 安裝必要系統套件 + Python + pip + curl
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl bash procps && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------
# 安裝 Spark
# -------------------------------
# 20250926_001 - 使用預先下載的 Spark tarball（由 builder 提供，避免重複下載）
# 20251016_004 把 Spark改放到 Spark資料夾，因為 airflow不用
# 👉 新版路徑改為 spark/ 底下
COPY spark/spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz

# 20250926_001 - Extract spark to /opt/spark and strip root folder
RUN mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# 20251016_005 - 確保 Spark 執行腳本可執行（避免 Permission denied）
RUN chmod +x /opt/spark/sbin/*.sh

# -------------------------------
# 安裝 Python 套件（給 PySpark 使用）
# -------------------------------
# 20251102_001 >>
# 👉 路徑統一為 spark/requirements
COPY spark/requirements.spark.txt /opt/requirements/requirements.spark.txt
RUN pip install --no-cache-dir -r /opt/requirements/requirements.spark.txt
# 20251102_001 <<

# -------------------------------
# ✅ 核心：統一 jar 管理（取代 --packages）
# -------------------------------
# 20250906_005 - PoChun Hsu - [Add]     Add PostgreSQL JDBC driver
# 👉 舊方式（單一 jar）
# COPY jars/postgresql-42.7.3.jar /opt/spark/jars/

# 👉 新方式（統一管理所有 jar）
# - Spark 預設讀 /opt/spark/jars/*
# - 所有 service 共用（master / worker）
# - 避免 runtime --packages 不穩定
COPY spark_jars/*.jar /opt/spark/jars/

# -------------------------------
# ❌ 舊做法（已移除）
# -------------------------------
# RUN spark-shell --packages ...
# 👉 不再使用，因為：
# - 會在 build 時下載（不穩定）
# - executor 不一定拿得到
# - 與 docker image 不一致
# -------------------------------

# 20251016_004 - 設定環境變數
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

ENV PYSPARK_PYTHON=python3

# 20251015_001  
# 讓 start-master.sh、start-worker.sh 以前景模式運行
ENV SPARK_NO_DAEMONIZE=true  

ENV HOME=/root

# Create directory for spark apps
RUN mkdir -p /opt/spark-apps /opt/spark-output

# Install PySpark
# 20251016_001 目前架構不需要，spark-3.5.0-bin-hadoop3.tgz 已經有，安裝費時約１００秒
# RUN pip install --no-cache-dir pyspark==3.5.0

# 20251016_004 - 設定預設工作目錄
WORKDIR /opt/spark

# 20251016_004 Default entrypoint handled by docker-compose command
CMD ["/bin/bash"]
