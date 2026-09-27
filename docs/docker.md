# Quickstart with Docker

If you want to try running .NET Spark applications without worrying about what to download and install, you can try using Docker instead.

## Build
To build the image, you can run:

```bash
DOCKER_BUILDKIT=1 docker build -t dotnet-spark .
```

This will create a `dotnet-spark` image.

## Running a Script
If you have already built your project and all that's left is to run it in an environment with a .NET Spark setup, then you can run:

```bash
docker run -v $(pwd)/mySparkApp:/app -e DLL_PATH=/app/bin/Debug/netcoreapp3.1/mySparkApp.dll -it dotnet-spark
```

where `mySparkApp` is your .NET Spark application directory and `DLL_PATH` is the path to your compiled application `.dll`.

If you want to build it inside the container and run it there, you can run:

```bash
docker run -v $(pwd)/mySparkApp:/app dotnet-spark bash 
```

Once inside the container, run the following:

```bash
# Install .NET Spark Package
dotnet add package Microsoft.Spark --version 0.10.0

# Build your .NET Project
dotnet build

# Run .NET Spark Application
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local bin/Debug/${DOTNET_CORE_VERSION}/microsoft-spark-2.4.x-${DOTNET_SPARK_VERSION}.jar dotnet /app/bin/Debug/netcoreapp3.1/mySparkApp.dll
```