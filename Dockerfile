FROM ubuntu
WORKDIR /app
ENV M2_HOME "/app/bin/maven/current"
ENV SPARK_HOME "/app/spark-2.4.1-bin-hadoop2.7"
ENV PATH "$M2_HOME/bin:$SPARK_HOME/bin:$PATH"
RUN apt-get update -y \
  && apt-get install -y software-properties-common wget \
  && add-apt-repository -y universe \
  && wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && apt-get update -y \
  && apt-get install -y apt-transport-https git-core dotnet-sdk-2.1 openjdk-8-jre \
  && mkdir -p /app/bin/maven \
  && cd /app/bin/maven \
  && wget https://www-us.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz \
  && tar -xvzf apache-maven-3.6.0-bin.tar.gz \
  && ln -s apache-maven-3.6.0 current && cd /app \
  && wget -q https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz \
  && tar -xzf spark-2.4.1-bin-hadoop2.7.tgz \
  && wget -q https://github.com/dotnet/spark/releases/download/v0.2.0/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.2.0.tar.gz \
  && tar -xzf Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.2.0.tar.gz \
  && git clone https://github.com/dotnet/spark.git dotnet.spark \
  && cd dotnet.spark/src/scala \
  && mvn clean package
CMD spark-shell
