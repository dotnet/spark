FROM ubuntu:18.04

ARG DOTNET_CORE_VERSION=3.1
ARG DEBIAN_FRONTEND=noninteractive
ENV DOTNET_CORE_VERSION=$DOTNET_CORE_VERSION
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1

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
    && wget -q --show-progress --progress=bar:force:noscroll https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && add-apt-repository universe \
    && apt-get install -y apt-transport-https \
    && apt-get update \
    && apt-get install -y dotnet-sdk-$DOTNET_CORE_VERSION \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf packages-microsoft-prod.deb
