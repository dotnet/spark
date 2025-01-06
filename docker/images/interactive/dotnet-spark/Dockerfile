ARG DOTNET_SPARK_VERSION=1.0.0
FROM dotnet-spark-base-interactive:$DOTNET_SPARK_VERSION
LABEL maintainer="Martin Kandlbinder <indy@3rdman.de>"

ARG SPARK_VERSION=3.0.1
ARG DOTNET_SPARK_JAR="microsoft-spark-3-0_2.12-$DOTNET_SPARK_VERSION"
ENV DAEMON_RUN=true \
    DOTNETBACKEND_PORT=5567 \
    HADOOP_VERSION=2.7 \
    JUPYTER_ENABLE_LAB=true \
    SPARK_VERSION=$SPARK_VERSION \
    SPARK_HOME=/spark \
    PATH="${SPARK_HOME}/bin:${DOTNET_WORKER_DIR}:${PATH}"

USER root

COPY bin/* /usr/local/bin/
COPY *.ipynb ${HOME}/dotnet.spark/examples/

RUN cd /dotnet \
    && dotnet new console -o SparkDummy \
    && cd SparkDummy \
    && dotnet add package Microsoft.Spark \
    && dotnet build \
    && cp /dotnet/SparkDummy/bin/Debug/netcoreapp${DOTNET_CORE_VERSION}/microsoft-spark-*.jar ${HOME}/ \
    && rm -rf /dotnet/SparkDummy \
    && cd / \
    && echo "Downloading spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz ..." \
    && wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && chmod 755 /usr/local/bin/start-spark-debug.sh \
    && chown -R ${NB_UID} ${HOME}

USER ${NB_USER}
WORKDIR ${HOME}/dotnet.spark
