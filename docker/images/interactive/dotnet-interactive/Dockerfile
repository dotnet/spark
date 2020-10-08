FROM jupyter/base-notebook:ubuntu-18.04

ARG NB_USER=jovyan
ARG NB_UID=1000
ARG DOTNET_CORE_VERSION=3.1
ARG DEBIAN_FRONTEND=noninteractive

ENV DOTNET_CORE_VERSION=$DOTNET_CORE_VERSION
ENV USER ${NB_USER}
ENV NB_UID ${NB_UID}
ENV HOME /home/${NB_USER}
ENV PATH="${PATH}:${HOME}/.dotnet/tools"

ENV \
    DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_USE_POLLING_FILE_WATCHER=true \
    NUGET_XMLDOC_MODE=skip \
    DOTNET_TRY_CLI_TELEMETRY_OPTOUT=true

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        dialog apt-utils wget ca-certificates openjdk-8-jdk bash software-properties-common supervisor unzip socat net-tools vim \
        libc6 libgcc1 libgssapi-krb5-2 libicu60 libssl1.1 libstdc++6 zlib1g \
    && wget -q --show-progress --progress=bar:force:noscroll https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && add-apt-repository universe \
    && apt-get install -y apt-transport-https \
    && apt-get update \
    && apt-get install -y dotnet-sdk-$DOTNET_CORE_VERSION \
    && apt-get autoremove -y --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf packages-microsoft-prod.deb

COPY ./nuget.config ${HOME}/nuget.config

RUN chown -R ${NB_UID} ${HOME}

USER ${USER}

RUN pip install nteract_on_jupyter \
    && dotnet tool install -g Microsoft.dotnet-interactive \
    && dotnet interactive jupyter install

