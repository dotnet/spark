# Getting Started with Spark .NET on Windows

These instructions will show you how to run a .NET for Apache Spark app using .NET 8 on Windows.

## Pre-requisites

- Download and install the following: **[.NET 8 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)** | **[Visual Studio 2019](https://www.visualstudio.com/downloads/)** | **[Java 1.8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)** | **[Apache Spark 2.4.1](https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz)**
- Download and install **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release:
    - Select a **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release from .NET for Apache Spark GitHub Releases page and download into your local machine (e.g., `c:\bin\Microsoft.Spark.Worker\`).
    - **IMPORTANT** Create a [new environment variable](https://www.java.com/en/download/help/path.xml) `DOTNET_WORKER_DIR` and set it to the directory where you downloaded and extracted the Microsoft.Spark.Worker (e.g., `c:\bin\Microsoft.Spark.Worker`).

For detailed instructions, you can see [Building .NET for Apache Spark from Source on Windows](../building/windows-instructions.md).

## Authoring a .NET for Apache Spark App
- Open Visual Studio -> Create New Project -> Console App -> Name: `HelloSpark`
- Install `Microsoft.Spark` Nuget package into the solution from the [spark nuget.org feed](https://www.nuget.org/profiles/spark) - see [Ways to install Nuget Package](https://docs.microsoft.com/en-us/nuget/consume-packages/ways-to-install-a-package)
- Write the following code into `Program.cs`:
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
- Build the solution

## Running your .NET for Apache Spark App
- Open your terminal and navigate into your app folder:
    ```
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
    microsoft-spark-<version>.jar `
    dotnet HelloSpark.dll
    ```
    **Note**: This command assumes you have downloaded Apache Spark and added it to your PATH environment variable to be able to use `spark-submit`, otherwise, you would have to use the full path (e.g., `c:\bin\apache-spark\bin\spark-submit`). For detailed instructions, you can see [Building .NET for Apache Spark from Source on Windows](../building/windows-instructions.md).
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
