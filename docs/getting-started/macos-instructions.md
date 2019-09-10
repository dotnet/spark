# Getting Started with Spark .NET on MacOS

These instructions will show you how to run a .NET for Apache Spark app using .NET Core on MacOSX.

## Pre-requisites

- Download and install the following: **[.NET Core 2.1 SDK](https://dotnet.microsoft.com/download/dotnet-core/2.1)** 
- Download and install Java using 
  ```brew cask install java```
- Download and install Scala using
    ```shell
    brew install scala
    ```
- Download and install Apache Spark using ```brew install apache-spark```
    
- Download and install **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release:
    - Select a **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release from .NET for Apache Spark GitHub Releases page and download into your local machine (e.g., `/bin/Microsoft.Spark.Worker-0.4.0`).
    - **IMPORTANT** Create a new environment variable using ```export DOTNET_WORKER_DIR <your_path>``` and set it to the directory where you downloaded and extracted the Microsoft.Spark.Worker (e.g., `/bin/Microsoft.Spark.Worker-0.4.0`).


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
- Open your terminal and navigate into your app folder:
    ```shell
    cd <your-app-output-directory>
    ```
- Create `people.json` with the following content:
    ```json
    {"name":"Michael"}
    {"name":"Andy", "age":30}
    {"name":"Justin", "age":19}
    ```
- Run your app
    ```
    spark-submit `
    --class org.apache.spark.deploy.dotnet.DotnetRunner `
    --master local `
    microsoft-spark-2.4.x-<version>.jar `
    dotnet HelloSpark.dll
    ```
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
