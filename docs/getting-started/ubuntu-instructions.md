# Getting Started with Spark.NET on Ubuntu

These instructions will show you how to run a .NET for Apache Spark app using .NET 8 on Ubuntu 18.04.

## Pre-requisites

- Download and install the following: **[.NET 8 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)** | **[OpenJDK 8](https://openjdk.java.net/install/)** | **[Apache Spark 3.5.3 (Scala 2.12)](https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz)**
- This example uses the `microsoft-spark-3-5_2.12-<version>.jar` bridge with Java 8. Spark 4.0 requires JDK 17 and `microsoft-spark-4-0_2.13-<version>.jar`; validate the APIs you need before changing runtimes. See the [migration guide](../migration-guide.md#upgrading-after-spark-2x-support-removal).
- Download and install **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release:
    - Select a **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release from .NET for Apache Spark GitHub Releases page and download into your local machine (e.g., `~/bin/Microsoft.Spark.Worker`).
    - **IMPORTANT** Create a [new environment variable](https://help.ubuntu.com/community/EnvironmentVariables) `DOTNET_WORKER_DIR` and set it to the directory where you downloaded and extracted the Microsoft.Spark.Worker (e.g., `~/bin/Microsoft.Spark.Worker`).

For detailed instructions, you can see [Building .NET for Apache Spark from Source on Ubuntu](../building/ubuntu-instructions.md).

## Authoring a .NET for Apache Spark App

- Use the `dotnet` CLI to create a console application.
   ```shell
   dotnet new console -o HelloSpark
   ```
- Install `Microsoft.Spark` Nuget package into the project from the [spark nuget.org feed](https://www.nuget.org/profiles/spark) - see [Ways to install Nuget Package](https://docs.microsoft.com/en-us/nuget/consume-packages/ways-to-install-a-package)
    ```shell
    cd HelloSpark
    dotnet add package Microsoft.Spark
    ```
- Replace the contents of the `Program.cs` file with the following code:
    ```csharp
    using Microsoft.Spark.Sql;

    namespace HelloSpark
    {
        class Program
        {
            static void Main(string[] args)
            {
                var spark = SparkSession.Builder().GetOrCreate();
                var df = spark.Read().Json("people.json");
                df.Show();
            }
        }
    }
    ```
- Use the `dotnet` CLI to build the application:
    ```shell
    dotnet build
    ```


## Running your .NET for Apache Spark App
- Open your terminal and navigate into your app folder.
    ```shell
    cd <your-app-output-directory>
    ```
- Create `people.json` with the following content:
    ```json
    {"name":"Michael"}
    {"name":"Andy", "age":30}
    {"name":"Justin", "age":19}
    ```
- Run your app.
    ```shell
    spark-submit \
    --class org.apache.spark.deploy.dotnet.DotnetRunner \
    --master local \
    microsoft-spark-3-5_2.12-<version>.jar \
    dotnet HelloSpark.dll
    ```
    **Note**: This command assumes you have downloaded Apache Spark and added it to your PATH environment variable to be able to use `spark-submit`, otherwise, you would have to use the full path (e.g., `~/spark/bin/spark-submit`). For detailed instructions, you can see [Building .NET for Apache Spark from Source on Ubuntu](../building/ubuntu-instructions.md).
- The output of the application should look similar to the output below:
    ```text
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    ```
