FROM ubuntu
WORKDIR /app
RUN apt-get update -y \
        && apt-get install -y wget \
        && wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb \
        && dpkg -i packages-microsoft-prod.deb \
        && apt-get install -y software-properties-common \
        && add-apt-repository -y universe \
        && apt-get install -y apt-transport-https \
        && apt-get update -y \
        && apt-get install -y dotnet-sdk-2.1 \
        && apt-get install -y openjdk-8-jre \
        && mkdir -p ~/bin/maven \
        && cd ~/bin/maven \
        && wget https://www-us.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz \
        && tar -xvzf apache-maven-3.6.0-bin.tar.gz \
        && ln -s apache-maven-3.6.0 current \
        && export M2_HOME=~/bin/maven/current \
        && export PATH=${M2_HOME}/bin:${PATH} \
        && cd /app \
        && wget -q https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz \
        && tar -xzf spark-2.4.1-bin-hadoop2.7.tgz \
        && wget -q https://github.com/dotnet/spark/releases/download/v0.2.0/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.2.0.tar.gz \
        && tar -xzf Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.2.0.tar.gz \
        && export SPARK_HOME=$(pwd)/spark-2.4.1-bin-hadoop2.7 \
        && export PATH="$SPARK_HOME/bin:$PATH" \
        && apt-get install -y git-core \
        && git clone https://github.com/dotnet/spark.git dotnet.spark \
        && cd dotnet.spark/src/scala \
        && ~/bin/maven/current/bin/mvn clean package
CMD tail -f /dev/null
