![Icon](docs/img/dotnetsparklogo-6.png)

# .NET for Apache® Spark™

.NET for Apache Spark provides high performance APIs for using [Apache Spark](https://spark.apache.org/) from C# and F#. With these .NET APIs, you can access the most popular Dataframe and SparkSQL aspects of Apache Spark, for working with structured data, and Spark Structured Streaming, for working with streaming data. 

.NET for Apache Spark is compliant with .NET Standard - a formal specification of .NET APIs that are common across .NET implementations. This means you can use .NET for Apache Spark anywhere you write .NET code allowing you to reuse all the knowledge, skills, code, and libraries you already have as a .NET developer. 

.NET for Apache Spark runs on Windows, Linux, and macOS using .NET Core, or Windows using .NET Framework. It also runs on all major cloud providers including [Azure HDInsight Spark](deployment/README.md#azure-hdinsight-spark), [Amazon EMR Spark](deployment/README.md#amazon-emr-spark), [AWS](deployment/README.md#databricks) & [Azure](deployment/README.md#databricks) Databricks.

## Table of Contents

- [Get Started](#get-started)
- [Build Status](#build-status)
- [Building from Source](#building-from-source)
- [Samples](#samples)
- [Contributing](#contributing)
- [Inspiration and Special Thanks](#inspiration-and-special-thanks)
- [How to Engage, Contribute and Provide Feedback](#how-to-engage-contribute-and-provide-feedback)
- [.NET Foundation](#net-foundation)
- [Code of Conduct](#code-of-conduct)
- [License](#license)

## Get Started

In this section, we will show how to run a .NET for Apache Spark app using .NET Core on Windows.

1. **Pre-requisites**: 
    - Download and install the following: **[.NET Core 3.0 SDK](https://dotnet.microsoft.com/download/dotnet-core/3.0)** | **[Visual Studio 2019](https://www.visualstudio.com/downloads/)** | **[Java 1.8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)** | **[Apache Spark 2.4.x](https://spark.apache.org/downloads.html)**
    - Download and install **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release:
      - Select a **[Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases)** release from .NET for Apache Spark GitHub Releases page and download into your local machine (e.g., `c:\bin\Microsoft.Spark.Worker\`).
      - **IMPORTANT** Create a [new environment variable](https://www.java.com/en/download/help/path.xml) `DotnetWorkerPath` and set it to the directory where you downloaded and extracted the Microsoft.Spark.Worker (e.g., `c:\bin\Microsoft.Spark.Worker`).
  
    For detailed instructions, you can see [Building .NET for Apache Spark from Source on Windows](docs/building/windows-instructions.md).
2. **Authoring a .NET for Apache Spark App**:
    - Open Visual Studio -> Create New Project -> Console App (.NET Core) -> Name: `HelloSpark`
    - Install `Microsoft.Spark` Nuget package into the solution from the [spark nuget.org feed](https://www.nuget.org/profiles/spark) - see [Ways to install Nuget Package](https://docs.microsoft.com/en-us/nuget/consume-packages/ways-to-install-a-package)
    - Write the following code into `Program.cs`:
      ```csharp
        var spark = SparkSession.Builder().GetOrCreate();
        var df = spark.Read().Json("people.json");
        df.Show();
      ```
    - Build the solution
3. **Running your .NET for Apache Spark App**:
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
           --class org.apache.spark.deploy.DotnetRunner `
           --master local `
           microsoft-spark-2.4.x-<version>.jar `
           HelloSpark
       ```
       Note that this command assumes you have downloaded Apache Spark and added it to your PATH environment variable. For detailed instructions, you can see [Building .NET for Apache Spark from Source on Windows](docs/building/windows-instructions.md).

## Build Status

| ![Ubuntu icon](docs/img/ubuntu-icon-32.png) | ![Windows icon](docs/img/windows-icon-32.png) |
| :---:         |          :---: |
| Ubuntu | Windows |
| | [![Build Status](https://dnceng.visualstudio.com/public/_apis/build/status/dotnet.spark?branchName=master)](https://dev.azure.com/dnceng/public/_build?definitionId=459?branchName=master)|

## Building from Source

Building from source is very easy and the whole process (from cloning to being able to run your app) should take less than 15 minutes!

| |  | Instructions |
| :---: | :---         |      :--- |
| ![Windows icon](docs/img/windows-icon-32.png) | **Windows**    | <ul><li>Local - [.NET Framework 4.6.1](docs/building/windows-instructions.md#using-visual-studio-for-net-framework-461)</li><li>Local - [.NET Core 2.1.x](docs/building/windows-instructions.md#using-net-core-cli-for-net-core-21x)</li><ul>    |
| ![Ubuntu icon](docs/img/ubuntu-icon-32.png) | **Ubuntu**     | <ul><li>Local - [.NET Core 2.1.x](docs/building/ubuntu-instructions.md)</li><li>[Azure HDInsight Spark - .NET Core 2.1.x](deployment/README.md)</li></ul>      |

<a name="samples"></a>
## Samples

There are two types of samples/apps in the .NET for Apache Spark repo:

* ![Icon](docs/img/app-type-getting-started.png) Getting Started - .NET for Apache Spark code focused on simple and minimalistic scenarios.

* ![Icon](docs/img/app-type-e2e.png)  End-End apps/scenarios - Real world examples of industry standard benchmarks, usecases and business applications implemented using .NET for Apache Spark. 

We welcome contributions to both categories!

<table>
 <tr>
   <td width="25%">
      <h4><b>Analytics Scenario</b></h4>
  </td>
  <td>
      <h4 width="35%"><b>Description</b></h4>
  </td>
  <td>
      <h4><b>Scenarios</b></h4>
  </td>
 </tr>
 <tr>
   <td width="25%">
      <h5>Dataframes and SparkSQL</h5>
  </td>
  <td width="35%">
  Simple code snippets to help you get familiarized with the programmability experience of .NET for Apache Spark.
  </td>
    <td>
      <h5>Basic &nbsp;&nbsp;&nbsp;
      <a href="examples/Microsoft.Spark.CSharp.Examples/Sql/Basic.cs">C#</a> &nbsp; &nbsp; <a href="examples/Microsoft.Spark.FSharp.Examples/Sql/Basic.fs">F#</a>&nbsp;&nbsp;&nbsp;<a href="#"><img src="docs/img/app-type-getting-started.png" alt="Getting started icon"></a></h5>
  </td>
 </tr>
 <tr>
   <td width="25%">
      <h5>Structured Streaming</h5>
  </td>
  <td width="35%">
      Code snippets to show you how to utilize Apache Spark's Structured Streaming (<a href="https://spark.apache.org/docs/2.3.1/structured-streaming-programming-guide.html">2.3.1</a>, <a href="https://spark.apache.org/docs/2.3.2/structured-streaming-programming-guide.html">2.3.2</a>, <a href="https://spark.apache.org/docs/2.4.1/structured-streaming-programming-guide.html">2.4.1</a>, <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html">Latest</a>)
  </td>
  <td>
      <h5>Word Count &nbsp;&nbsp;&nbsp;
      <a href="examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredNetworkWordCount.cs">C#</a> &nbsp;&nbsp;&nbsp;<a href="examples/Microsoft.Spark.FSharp.Examples/Sql/Streaming/StructuredNetworkWordCount.fs">F#</a> &nbsp;&nbsp;&nbsp;<a href="#"><img src="docs/img/app-type-getting-started.png" alt="Getting started icon"></a></h5>
      <h5>Windowed Word Count &nbsp;&nbsp;&nbsp;<a href="examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredNetworkWordCountWindowed.cs">C#</a> &nbsp; &nbsp;<a href="examples/Microsoft.Spark.FSharp.Examples/Sql/Streaming/StructuredNetworkWordCountWindowed.fs">F#</a> &nbsp;&nbsp;&nbsp;<a href="#"><img src="docs/img/app-type-getting-started.png" alt="Getting started icon"></a></h5>      
      <h5>Word Count on data from <a href="https://kafka.apache.org/">Kafka</a> &nbsp;&nbsp;&nbsp;<a href="examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredKafkaWordCount.cs">C#</a> &nbsp;&nbsp;&nbsp;<a href="examples/Microsoft.Spark.FSharp.Examples/Sql/Streaming/StructuredKafkaWordCount.fs">F#</a> &nbsp; &nbsp;&nbsp;<a href="#"><img src="docs/img/app-type-getting-started.png" alt="Getting started icon"></a></h5>
  </td>
 </tr>
 <tr>
   <td width="25%">
      <h4>TPC-H Queries</h4>
  </td>
  <td width="35%">
  Code to show you how to author complex queries using .NET for Apache Spark.
  </td>
  <td>
      <h5>TPC-H Functional &nbsp;&nbsp;&nbsp;
      <a href="benchmark/csharp/Tpch/TpchFunctionalQueries.cs">C#</a> &nbsp;&nbsp;&nbsp;<a href="#"><img src="docs/img/app-type-e2e.png" alt="End-to-end app icon"></a></h5>
      <h5>TPC-H SparkSQL &nbsp;&nbsp;&nbsp;
      <a href="benchmark/csharp/Tpch/TpchSqlQueries.cs">C#</a>  &nbsp;&nbsp;&nbsp;<a href="#"><img src="docs/img/app-type-e2e.png" alt="End-to-end app icon"></a></h5>
  </td>
</tr>
 </tr> 
 </table>

## Contributing

We welcome contributions! Please review our [contribution guide](CONTRIBUTING.md).

## Inspiration and Special Thanks

This project would not have been possible without the outstanding work from the following communities:

- [Apache Spark](https://spark.apache.org/): Unified Analytics Engine for Big Data, the underlying backend execution engine for .NET for Apache Spark
- [Mobius](https://github.com/Microsoft/Mobius): C# and F# language binding and extensions to Apache Spark, a pre-cursor project to .NET for Apache Spark from the same Microsoft group.
- [PySpark](https://spark.apache.org/docs/latest/api/python/index.html): Python bindings for Apache Spark, one of the implementations .NET for Apache Spark derives inspiration from. 
- [sparkR](https://spark.apache.org/docs/latest/sparkr.html): one of the implementations .NET for Apache Spark derives inspiration from.
- [Apache Arrow](https://arrow.apache.org/): A cross-language development platform for in-memory data. This library provides .NET for Apache Spark with efficient ways to transfer column major data between the JVM and .NET CLR.
- [Pyrolite](https://github.com/irmen/Pyrolite) - Java and .NET interface to Python's pickle and Pyro protocols. This library provides .NET for Apache Spark with efficient ways to transfer row major data between the JVM and .NET CLR. 
- [Databricks](https://databricks.com/): Unified analytics platform. Many thanks to all the suggestions from them towards making .NET for Apache Spark run on Azure and AWS Databricks.

## How to Engage, Contribute and Provide Feedback

The .NET for Apache Spark team encourages [contributions](docs/contributing.md), both issues and PRs. The first step is finding an [existing issue](https://github.com/dotnet/spark/issues) you want to contribute to or if you cannot find any, [open an issue](https://github.com/dotnet/spark/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+).

## .NET Foundation

The .NET for Apache Spark project is part of the [.NET Foundation](http://www.dotnetfoundation.org).

## Code of Conduct

This project has adopted the code of conduct defined by the Contributor Covenant
to clarify expected behavior in our community.
For more information, see the [.NET Foundation Code of Conduct](https://dotnetfoundation.org/code-of-conduct).

<a name="license"></a>
## License

.NET for Apache Spark is licensed under the [MIT license](LICENSE).
