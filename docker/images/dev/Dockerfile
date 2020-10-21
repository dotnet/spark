ARG SDK_IMAGE_TAG=3.1-bionic
FROM mcr.microsoft.com/dotnet/core/sdk:$SDK_IMAGE_TAG

ARG SPARK_VERSION=3.0.1
ARG MAVEN_VERSION=3.6.3
ARG HADOOP_VERSION=2.7
ARG DEBIAN_FRONTEND=noninteractive

ENV DAEMON_RUN=true
ENV SPARK_HOME=/spark

ENV M2_HOME=/usr/local/bin/maven/current
ENV PATH="${PATH}:${SPARK_HOME}/bin:${M2_HOME}/bin"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-utils \
        ca-certificates \
        dialog \
        openjdk-8-jdk \
        socat \
        software-properties-common \
        supervisor \
        unzip \
        wget \
    && add-apt-repository universe \
    && apt-get install -y apt-transport-https \
    && apt-get update \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/bin/maven \
    && cd /usr/local/bin/maven  \
    && wget -q --show-progress --progress=bar:force:noscroll https://www-us.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz  \
    && tar -xvzf apache-maven-${MAVEN_VERSION}-bin.tar.gz  \
    && ln -s apache-maven-${MAVEN_VERSION} current  \
    && rm apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    && git clone https://github.com/dotnet/spark.git ~/dotnet.spark \
    && cp ~/dotnet.spark/NuGet.config ~/.nuget/NuGet/NuGet.Config

RUN wget -q --show-progress --progress=bar:force:noscroll https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN curl -fsSL https://code-server.dev/install.sh | sh \
    && mkdir -p ~/.config/code-server \
    && echo "bind-addr: 0.0.0.0:8080\nauth: none\ncert : false\n" > ~/.config/code-server/config.yaml

COPY supervisor.conf /etc/supervisor.conf

CMD ["supervisord", "-c", "/etc/supervisor.conf"]
