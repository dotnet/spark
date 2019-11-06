#!/bin/bash
#
# Install Livy and SparkMagic on Head node
#

set -e

# Uncomment if you want full tracing (for debugging purposes)
#set -o xtrace

if  [[ $HOSTNAME == hn* ]] ;
then
    # Update Livy Jars
    sudo wget https://sparkdotnetrepl.blob.core.windows.net/notebooks/livy_jar.zip
    sudo unzip livy_jar.zip
    sudo cp -rf livy_jar/* /usr/hdp/current/livy2-server/

    # Update SparkMagic
    sudo wget https://sparkdotnetrepl.blob.core.windows.net/notebooks/sparkmagic.zip
    sudo unzip sparkmagic.zip
    sudo cp -f sparkmagic/config.json /home/spark/.sparkmagic/
    sudo cp -rf sparkmagic/* /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/

    # install sparkdotnetkernel
    sudo /usr/bin/anaconda/bin/jupyter-kernelspec install /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel

    # Remove the temporary livy jars and sparkmagic files.
    sudo rm livy_jar.zip
    sudo rm -rf livy_jar
    sudo rm sparkmagic.zip
    sudo rm -rf sparkmagic

#
# Install SparkDotnet and Worker on Worker node
#

else
    # Install SparkDotNet
    SPARK_DOTNET_VERSION=$1
    # Check if parameter exists, otherwise error out
    [ -z "$SPARK_DOTNET_VERSION"] && { echo "Error: Sparkdotnet version parameter is missing..."; exit 1; }
    
    sudo dpkg --purge --force-all packages-microsoft-prod
    sudo wget -q https://packages.microsoft.com/config/ubuntu/`lsb_release -rs`/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
    sudo dpkg -i packages-microsoft-prod.deb
    sudo add-apt-repository universe
    sudo apt-get -yq install apt-transport-https
    sudo apt-get -yq update
    sudo apt-get -yq install dotnet-sdk-3.0

    sudo dotnet tool install dotnet-try --add-source https://dotnet.myget.org/F/dotnet-try/api/v3/index.json --tool-path /usr/share/dotnet-tools --version 1.0.19473.13

    # copy .NET for Apache Spark jar to SPARK's jar folder
    sudo mkdir -p tmp
    sudo wget "https://www.nuget.org/api/v2/package/Microsoft.Spark/${SPARK_DOTNET_VERSION}" -O tmp/"microsoft.spark.${SPARK_DOTNET_VERSION}.nupkg"
    sudo unzip tmp/"microsoft.spark.${SPARK_DOTNET_VERSION}.nupkg" -d tmp
    sudo install --verbose --mode 644 tmp/jars/"microsoft-spark-2.4.x-${SPARK_DOTNET_VERSION}.jar" "/usr/hdp/current/spark2-client/jars/microsoft-spark-2.4.x-${SPARK_DOTNET_VERSION}.jar"

    # cleanup unneeded packages
    sudo apt-get autoremove -yq

    # Remove the prod deb file and temporary jar file.
    sudo rm packages-microsoft-prod.deb

    # Install Microsoft.Spark.Worker
    # Path where packaged worker file (tgz) exists.
    SRC_WORKER_PATH_OR_URI="https://github.com/dotnet/spark/releases/download/v${SPARK_DOTNET_VERSION}/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-${SPARK_DOTNET_VERSION}.tar.gz"

    # The path on the executor nodes where Microsoft.Spark.Worker executable is installed.
    WORKER_INSTALLATION_PATH=/usr/local/bin

    # The path where all the dependent libraies are installed so that it doesn't
    # pollute the $WORKER_INSTALLATION_PATH.
    SPARKDOTNET_ROOT=$WORKER_INSTALLATION_PATH/spark-dotnet

    # Temporary worker file.
    TEMP_WORKER_FILENAME=/tmp/temp_worker.tgz

    # Extract version
    IFS='-' read -ra BASE_FILENAME <<< "$(basename $SRC_WORKER_PATH_OR_URI .tar.gz)"
    VERSION=${BASE_FILENAME[2]}

    IFS='.' read -ra VERSION_CHECK <<< "$VERSION"
    [[ ${#VERSION_CHECK[@]} == 3 ]] || { echo >&2 "Version check does not satisfy. Raise an issue here: https://github.com/dotnet/spark"; exit 1; }

    # Path of the final destination for the worker binaries
    # (the one we just downloaded and extracted)
    DEST_WORKER_PATH=$SPARKDOTNET_ROOT/Microsoft.Spark.Worker-$VERSION
    DEST_WORKER_BINARY=$DEST_WORKER_PATH/Microsoft.Spark.Worker

    # Clean up any existing files.
    sudo rm -f $WORKER_INSTALLATION_PATH/Microsoft.Spark.Worker
    sudo rm -rf $SPARKDOTNET_ROOT

    # Copy the worker file to a local temporary file.
    wget $SRC_WORKER_PATH_OR_URI -O $TEMP_WORKER_FILENAME

    # Untar the file.
    sudo mkdir -p $SPARKDOTNET_ROOT
    sudo tar xzf $TEMP_WORKER_FILENAME -C $SPARKDOTNET_ROOT

    # Make the file executable since dotnet doesn't set this correctly.
    sudo chmod 755 $DEST_WORKER_BINARY

    # Create a symlink.
    sudo ln -sf $DEST_WORKER_BINARY $WORKER_INSTALLATION_PATH/Microsoft.Spark.Worker

    # Remove the temporary nuget and worker file.
    sudo rm $TEMP_WORKER_FILENAME
    sudo rm -rf tmp
fi
