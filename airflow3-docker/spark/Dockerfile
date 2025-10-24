# -----------------------------------------------------------------------------
# Spark 3.5.0 for Airflow 3.1.0 Integration
# Author: PoChun Hsu
# Verified Build: OK (2025-10-25)
# -----------------------------------------------------------------------------

FROM openjdk:17-jdk-slim

LABEL maintainer="PoChun Hsu"
LABEL description="Spark 3.5.0 container for Airflow 3.1.0"

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
        curl wget procps netcat tini \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------
# 使用本地 Spark 安裝檔案
# ---------------------------------------------------
COPY spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz
RUN mkdir -p /opt && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm /tmp/spark.tgz

# ---------------------------------------------------
# Python support
# ---------------------------------------------------
# RUN pip install --no-cache-dir \
#     pyspark==${SPARK_VERSION} pandas pyarrow numpy requests

# ---------------------------------------------------
# Spark 設定與外部 jar（不會因為不存在而報錯）
# ---------------------------------------------------
RUN mkdir -p /opt/spark/conf /opt/spark/external-jars
COPY spark_conf/. /opt/spark/conf/
COPY spark_jars/. /opt/spark/external-jars/

# ---------------------------------------------------
# Volume & Port
# ---------------------------------------------------
WORKDIR $SPARK_HOME
VOLUME ["/opt/spark-apps", "/opt/spark-output", "/tmp/spark-events"]
EXPOSE 4040 7077 8080 18080

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/bash"]

USER 1000:1000
