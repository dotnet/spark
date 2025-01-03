# Getting Started with Spark .NET on MacOS

These instructions will show you how to run a .NET for Apache Spark app using .NET 8 on MacOSX.

## Pre-requisites

- Download and install **[.NET 8 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)** 
- Install **[Java 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)** 
     - Select the appropriate version for your operating system e.g., `jdk-8u231-macosx-x64.dmg`.
     - Install using the installer and verify you are able to run `java` from your command-line
- Download and install **[Apache Spark 2.4.4](https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz)**:
     - Add the necessary environment variables SPARK_HOME e.g., `~/bin/spark-2.4.4-bin-hadoop2.7/`
        ```bash
        export SPARK_HOME=~/bin/spark-2.4.4-bin-hadoop2.7/
        export PATH="$SPARK_HOME/bin:$PATH"
        source ~/.bashrc
        ```
- Download and install **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release:
    - Select a **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release from .NET for Apache Spark GitHub Releases page and download into your local machine (e.g., `/bin/Microsoft.Spark.Worker/`).
    - **IMPORTANT** Create a new environment variable using ```export DOTNET_WORKER_DIR <your_path>``` and set it to the directory where you downloaded and extracted the Microsoft.Spark.Worker (e.g., `/bin/Microsoft.Spark.Worker/`).
    - Make sure the worker is marked as executable and remove any "quarantined" attributes, e.g.:
        ```bash
        chmod 755 /bin/Microsoft.Spark.Worker/Microsoft.Spark.Worker
        xattr -d com.apple.quarantine /bin/Microsoft.Spark.Worker/*
        ```


## Authoring a .NET for Apache Spark App
- Use the `dotnet` CLI to create a console application.
    ```
    dotnet new console -o HelloSpark
    ```
- Install `Microsoft.Spark` Nuget package into the project from the [spark nuget.org feed](https://www.nuget.org/profiles/spark) - see [Ways to install Nuget Package](https://docs.microsoft.com/en-us/nuget/consume-packages/ways-to-install-a-package)
    ```
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
    ```bash
    dotnet build
    ```

## Running your .NET for Apache Spark App
- Open your terminal and navigate into your app folder:
    ```bash
    cd <your-app-output-directory>
    ```
- Create `people.json` with the following content:
    ```json
    { "name" : "Michael" }
    { "name" : "Andy", "age" : 30 }
    { "name" : "Justin", "age" : 19 }
    ```
- Run your app
    ```bash
    spark-submit \
    --class org.apache.spark.deploy.dotnet.DotnetRunner \
    --master local \
    microsoft-spark-<version>.jar \
    dotnet HelloSpark.dll
    ```
    **Note**: This command assumes you have downloaded Apache Spark and added it to your PATH environment variable to be able to use `spark-submit`, otherwise, you would have to use the full path (e.g., `~/spark/bin/spark-submit`).
    
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
