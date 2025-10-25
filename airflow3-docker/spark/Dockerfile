# -----------------------------------------------------------------------------
# Spark 3.5.0 - Builder for local Spark cluster with Hadoop 3
# Base: OpenJDK 17 (Slim)
# Maintainer: Sherry Li
# 20251025_001 - Fixed JAVA_HOME path and user initialization
# -----------------------------------------------------------------------------

FROM openjdk:17-jdk-slim

# -----------------------------------------------------------------------------
# Install dependencies
# -----------------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip curl procps && \
    rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# Copy Spark binary package
# -----------------------------------------------------------------------------
COPY spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz

# -----------------------------------------------------------------------------
# Extract Spark and create symlink
# -----------------------------------------------------------------------------
RUN mkdir -p /opt && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# -----------------------------------------------------------------------------
# Create directories for configuration and external jars
# -----------------------------------------------------------------------------
RUN mkdir -p /opt/spark/conf /opt/spark/external-jars

# -----------------------------------------------------------------------------
# Copy local Spark configuration and jars
# -----------------------------------------------------------------------------
COPY spark_conf/. /opt/spark/conf/
COPY spark_jars/. /opt/spark/external-jars/

# -----------------------------------------------------------------------------
# Environment variables
# -----------------------------------------------------------------------------
ENV SPARK_HOME=/opt/spark
# 正確的 Java 路徑：openjdk:17-jdk-slim 使用 /usr/local/openjdk-17
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH
# 修正 Spark/Hadoop 啟動時 user 為 null 導致 KerberosAuthException
ENV SPARK_USER=spark
ENV USER=spark

# -----------------------------------------------------------------------------
# 建立非 root user，避免 KerberosAuthException: invalid null input: name
# -----------------------------------------------------------------------------
RUN useradd -ms /bin/bash sparkuser && \
    echo "spark:x:1000:1000:spark user:/home/spark:/bin/bash" >> /etc/passwd
USER sparkuser

# -----------------------------------------------------------------------------
# Working directory
# -----------------------------------------------------------------------------
WORKDIR /opt/spark

# -----------------------------------------------------------------------------
# Default command (可在 docker-compose.yml 中覆蓋)
# -----------------------------------------------------------------------------
CMD ["/bin/bash"]
