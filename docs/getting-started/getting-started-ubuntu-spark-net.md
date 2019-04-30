# Getting Started with Spark.NET on Ubuntu

These instructions help get you started with Spark on Ubuntu 18.04.

## Download and Install Prerequisites

For instructions on how to download and install the pre-requisites, refer to the [Ubuntu build instructions](../building/ubuntu-instructions.md#pre-requisites) documentation.

## Create Console Application

Use the `dotnet` CLI in the terminal to create a console application.

```bash
dotnet new console -o HelloSpark && cd HelloSpark
```

## Install Microsoft.Spark NuGet Package

Then, use the `dotnet` CLI in the terminal to add the [`Microsoft.Spark` NuGet package](https://www.nuget.org/packages/Microsoft.Spark/) to the project.

```bash
dotnet add package Microsoft.Spark
```

## Write The Program

Replace the contents of the `Program.cs` file with the following code:

```csharp
using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

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

Add the following content to your `HelloSpark.csproj` file inside the `Project` tags:

```xml
<ItemGroup>
    <Content Include="people.json">
        <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
    </Content>
</ItemGroup>
```

## Create Data

From the `HelloSpark` directory, enter the following command into the terminal to create sample data:

```bash
cat << EOF > people.json
{"name":"Michael"} 
{"name":"Andy", "age":30} 
{"name":"Justin", "age":19} 
EOF
```

## Build And Publish The Application

Use the `dotnet` CLI from the terminal to build and publish the application:

```bash
dotnet publish -f netcoreapp2.1 -r linux-x64 ./HelloSpark.csproj
```

## Run The Application

Navigate to the `publish` directory.

```bash
cd bin/Debug/netcoreapp2.1/linux-x64/publish/
```

Then, run the application with the following command:

```bash
spark-submit \
--class org.apache.spark.deploy.DotnetRunner \
--master local \
microsoft-spark-2.4.x-0.1.0.jar \
HelloSpark
```

The output of the application should look similar to the output below:

```text
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```