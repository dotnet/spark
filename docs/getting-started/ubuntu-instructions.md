# Getting Started with Spark.NET on Ubuntu

These instructions will show you how to run a .NET for Apache Spark app using .NET Core on Ubuntu 18.04.

## Pre-requisites

- Download and install the following: **[.NET Core 2.1 SDK](https://dotnet.microsoft.com/download/dotnet-core/2.1)** | **[OpenJDK 8](https://openjdk.java.net/install/)** | **[Apache Spark 2.4.1](https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz)**
- Download and install **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release:
    - Select a **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release from .NET for Apache Spark GitHub Releases page and download into your local machine (e.g., `~/bin/Microsoft.Spark.Worker`).
    - **IMPORTANT** Create a [new environment variable](https://help.ubuntu.com/community/EnvironmentVariables) `DotnetWorkerPath` and set it to the directory where you downloaded and extracted the Microsoft.Spark.Worker (e.g., `~/bin/Microsoft.Spark.Worker`).

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
- Create `people.json` with the following content:
    ```json
        {"name":"Michael"}
        {"name":"Andy", "age":30}
        {"name":"Justin", "age":19}
    ```
- Modify `HelloSpark.csproj` and add the following inside the `Project` tags:
    ```xml
    <ItemGroup>
        <Content Include="people.json">
            <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
        </Content>
    </ItemGroup>
    ```
- Use the `dotnet` CLI to build and publish the application:
    ```shell
    dotnet publish -f netcoreapp2.1 -r linux-x64 ./HelloSpark.csproj
    ```


## Running your .NET for Apache Spark App
- Navigate to the `publish` directory.
    ```shell
    cd bin/Debug/netcoreapp2.1/linux-x64/publish/
    ```
- Run your app.
    ```shell
    spark-submit \
    --class org.apache.spark.deploy.DotnetRunner \
    --master local \
    ./microsoft-spark-2.4.x-<version>.jar \
    ./HelloSpark
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
