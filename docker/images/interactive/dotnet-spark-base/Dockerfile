ARG DOTNET_CORE_VERSION=3.1
FROM dotnet-interactive:$DOTNET_CORE_VERSION

ARG DOTNET_SPARK_VERSION=1.0.0
ENV DOTNET_SPARK_VERSION=$DOTNET_SPARK_VERSION \
    DOTNET_WORKER_DIR=/dotnet/Microsoft.Spark.Worker-${DOTNET_SPARK_VERSION}

USER root

RUN mkdir -p /dotnet/Debug/netcoreapp${DOTNET_CORE_VERSION} \
    && wget -q https://github.com/dotnet/spark/releases/download/v${DOTNET_SPARK_VERSION}/Microsoft.Spark.Worker.netcoreapp${DOTNET_CORE_VERSION}.linux-x64-${DOTNET_SPARK_VERSION}.tar.gz \
    && tar -xvzf Microsoft.Spark.Worker.netcoreapp${DOTNET_CORE_VERSION}.linux-x64-${DOTNET_SPARK_VERSION}.tar.gz --directory /dotnet \
    && chmod 755 /dotnet/Microsoft.Spark.Worker-${DOTNET_SPARK_VERSION}/Microsoft.Spark.Worker \
    && rm Microsoft.Spark.Worker.netcoreapp${DOTNET_CORE_VERSION}.linux-x64-${DOTNET_SPARK_VERSION}.tar.gz
