ARG DOTNET_SPARK_VERSION=1.0.0
FROM dotnet-spark-base-runtime:$DOTNET_SPARK_VERSION

ARG SPARK_VERSION=3.0.1

ENV DAEMON_RUN=true
ENV SPARK_VERSION=$SPARK_VERSION
ENV SPARK_HOME=/spark

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_DISABLED=""
ENV SPARK_MASTER_URL=""

ENV SPARK_WORKER_PORT 7078
ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_INSTANCES=1

ENV SPARK_SUBMIT_PACKAGES=""
ENV SPARK_DEBUG_DISABLED=""
ENV HADOOP_VERSION=2.7
ENV PATH="${SPARK_HOME}/bin:${DOTNET_WORKER_DIR}:${PATH}"

COPY bin/* /usr/local/bin/
COPY supervisor.conf /etc/supervisor.conf

RUN wget -q --show-progress --progress=bar:force:noscroll https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && chmod 755 /usr/local/bin/start-spark-slave.sh \
    && chmod 755 /usr/local/bin/start-spark-master.sh \
    && chmod 755 /usr/local/bin/start-spark-debug.sh

EXPOSE 8080 8081 8082 7077 6066 5567 4040

CMD ["supervisord", "-c", "/etc/supervisor.conf"]
