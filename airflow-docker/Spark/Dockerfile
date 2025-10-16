# 20250906_003 - PoChun Hsu - [Add]     Spark install into Dockerfile.spark
# 20250926_001 - PoChun Hsu - [Alter]   Improved build efficiency by reusing pre-downloaded spark.tgz.
# 20251015_001 - PoChun Hsu - [Alter]   Add OpenJDK and foreground mode support (SPARK_NO_DAEMONIZE=true)
# 20251016_001 - PoChun Hsu - [Drop]    pyspark. No need in the current structure.

FROM openjdk:17-jdk-slim

# 20250906_005 - PoChun Hsu - [Add]     Add spark.tgz, jar, pyspark install
USER root

# 20251016_001 - 安裝必要系統套件 + Python + pip + curl
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl bash && \
    rm -rf /var/lib/apt/lists/*

# 20250926_001 - PoChun Hsu - [Alter]   Improved build efficiency by reusing pre-downloaded spark.tgz.
# 使用預先下載的 Spark tarball（由 builder 提供，避免重複下載）
COPY spark/spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz

# 20250926_001 - PoChun Hsu - [Add]     Extract spark to /opt/spark and strip root folder
RUN mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# 20251016_001 - 確保 Spark 執行腳本可執行（避免 Permission denied）
RUN chmod +x /opt/spark/sbin/*.sh

# 20250906_005 - PoChun Hsu - [Add]     Add PostgreSQL JDBC driver
COPY spark/jars/postgresql-42.7.3.jar /opt/spark/jars/

# 20251016_001 - 設定環境變數
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3
ENV SPARK_NO_DAEMONIZE=true  
# 讓 start-master.sh、start-worker.sh 以前景模式運行

# 20250906_005 - PoChun Hsu - [Add]     Create directory for spark apps
RUN mkdir -p /opt/spark-apps /opt/spark-output

# 20250906_005 - PoChun Hsu - [Add]     Install PySpark
# 20251016_001 目前架構不需要，spark-3.5.0-bin-hadoop3.tgz 已經有，安裝費時約１００秒
# RUN pip install --no-cache-dir pyspark==3.5.0

# 20251016_001 - 設定預設工作目錄
WORKDIR /opt/spark

# 20250906_005 - PoChun Hsu - [Note]    Default entrypoint handled by docker-compose command
CMD ["/bin/bash"]
