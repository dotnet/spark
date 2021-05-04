FROM ubuntu:18.04
LABEL maintainer="Martin Kandlbinder <indy@3rdman.de>"

ARG DEBIAN_FRONTEND=noninteractive
ARG DOTNET_CORE_VERSION=3.1
ENV DOTNET_CORE_VERSION=$DOTNET_CORE_VERSION

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
    && wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && add-apt-repository universe \
    && apt-get install -y apt-transport-https \
    && apt-get update \
    && apt-get install -y dotnet-sdk-$DOTNET_CORE_VERSION \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf packages-microsoft-prod.deb
