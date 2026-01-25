FROM ubuntu:19.10 AS deps

ENV DOTNET_SPARK_VERSION 0.10.0
ENV DOTNET_CORE_VERSION "netcoreapp3.1"
ENV SPARK_LIB_VER "spark-2.4.5-bin-hadoop2.7"
ENV DOTNET_WORKER_DIR "/spark/Microsoft.Spark.Worker-${DOTNET_SPARK_VERSION}"
ENV SPARK_HOME "/spark/${SPARK_LIB_VER}"
ENV SPARK_BIN "${SPARK_HOME}/bin"
ENV PATH "$SPARK_BIN:$PATH"

WORKDIR /build

RUN apt-get update -y \
    && apt-get install -y wget \
    && mkdir -p /spark \
    && wget -q https://archive.apache.org/dist/spark/spark-2.4.5/${SPARK_LIB_VER}.tgz \
    && tar -xzf ${SPARK_LIB_VER}.tgz --directory /spark \
    && wget -q https://github.com/dotnet/spark/releases/download/v${DOTNET_SPARK_VERSION}/Microsoft.Spark.Worker.${DOTNET_CORE_VERSION}.linux-x64-${DOTNET_SPARK_VERSION}.tar.gz \
    && tar -xzf Microsoft.Spark.Worker.${DOTNET_CORE_VERSION}.linux-x64-${DOTNET_SPARK_VERSION}.tar.gz --directory /spark \
    && wget -q https://packages.microsoft.com/config/ubuntu/19.10/packages-microsoft-prod.deb -O packages-microsoft-prod.deb

FROM ubuntu:19.10 as final

COPY --from=deps /spark /spark
COPY --from=deps /build /build

RUN apt-get -y update \
    && apt-get install -y ca-certificates apt-transport-https openjdk-8-jdk openjdk-8-jre \
    && dpkg -i /build/packages-microsoft-prod.deb \
    && apt-get -y update \
    && apt-get install -y dotnet-sdk-3.1 \
    && update-alternatives --config java 

WORKDIR /app

ENV DOTNET_SPARK_VERSION 0.10.0
ENV DOTNET_CORE_VERSION "netcoreapp3.1"
ENV SPARK_LIB_VER "spark-2.4.5-bin-hadoop2.7"
ENV DOTNET_WORKER_DIR "/spark/Microsoft.Spark.Worker-${DOTNET_SPARK_VERSION}"
ENV SPARK_HOME "/spark/${SPARK_LIB_VER}"
ENV SPARK_BIN "${SPARK_HOME}/bin"
ENV PATH "$SPARK_BIN:$PATH"

CMD spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local bin/Debug/${DOTNET_CORE_VERSION}/microsoft-spark-2.4.x-${DOTNET_SPARK_VERSION}.jar dotnet ${DLL_PATH}