# Developer Guide

## Table of Contents
- [How to Do Local Debugging](#how-to-do-local-debugging)
- [How to Support New Spark Releases](#how-to-support-new-spark-releases)

## How to Do Local Debugging

### Debugging Spark .NET Application

Open a new command prompt window, run the following:
```shell
spark-submit \
  --class org.apache.spark.deploy.dotnet.DotnetRunner \
  --master local \
  <path-to-microsoft-spark-jar> \
  debug
```
and you will see the followng output:
```
***********************************************************************
* .NET back end running debug mode. Press enter to exit *
***********************************************************************
```
In this debug mode, `DotnetRunner` does not launch the .NET application, but waits for it to connect. Leave this command prompt window open.

Now you can start your .NET application with a C# debugger ([Visual Studio Debugger for Windows/macOS](https://visualstudio.microsoft.com/vs/) or [C# Debugger Extension in Visual Code](https://code.visualstudio.com/Docs/editor/debugging)) to debug your application.

### Debugging User Defined Function (UDF)

**Note that this is currently supported only on Windows with Visual Studio Debugger.**

Before running `spark-submit`, set the following environment variable:
```bat
set DOTNET_WORKER_DEBUG=1
```
Now, when you run your Spark application, a `Choose Just-In-Time Debugger` window will pop up. Choose a Visual Studio debugger.

The debugger will break at the following location in [TaskRunner.cs](../src/csharp/Microsoft.Spark.Worker/TaskRunner.cs):
```C#
if (EnvironmentUtils.GetEnvironmentVariableAsBool("DOTNET_WORKER_DEBUG"))
{
    Debugger.Launch(); // <-- The debugger will break here.
}
```

Now, navigate to the `.cs` file that contains the UDF that you plan to debug, and set a breakpoint. (The breakpoint will say `The breakpoint will not currently be hit` because the worker hasn't loaded the assembly that contains UDF yet.)

Hit `F5` to continue your application and the breakpoint will eventually be hit.

**Note that the `Choose Just-In-Time Debugger` window will pop-up for each task. Therefore, make sure to set the number of executors to a low number. For example, you can use `--master local[1]` option for `spark-submit` to set the number of tasks to 1, and hence launching a single debugger instance.**

### Debugging Scala code

If you need to debug the Scala side code (`DotnetRunner`, `DotnetBackendHandler`, etc.), you can use the following command, and attach a debugger to the running process using [IntelliJ](https://www.jetbrains.com/help/idea/attaching-to-local-process.html):

```shell
spark-submit \
  --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
  --class org.apache.spark.deploy.dotnet.DotnetRunner \
  --master local \
  <path-to-microsoft-spark-jar> \
  <path-to-your-app-exe> <argument(s)-to-your-app>
```

## How to Support New Spark Releases

We encourage developers to first read Apache Spark's [Versioning Policy](https://spark.apache.org/versioning-policy.html) and [Semantic Versioning](https://semver.org/) to gain the most out of the instructions below.

At a high-level, Spark's versions are: **[MAJOR].[FEATURE].[MAINTENANCE]**. We will cover the upgrade path for each type of version separately below (in increasing order of effort required).

### [MAINTENANCE]: Upgrading for a Patch Release Version
Since Apache Spark's [MAINTENANCE] releases involve only internal changes (e.g., bug fixes etc.), it is straightforward to upgrade the code base to support a [MAINTENANCE] release. The steps to do this are below:

1. In the corresponding `pom.xml`, update the `spark.version` value to the newly released version.
   * For example, if a new patch release is 2.4.3, you will update [src/scala/microsoft-spark-2.4.x/pom.xml](/src/scala/microsoft-spark-2.4.x/pom.xml) to have `<spark.version>2.4.3</spark.version>`.
2. Update `DotnetRunner.supportedSparkVersions` to include the newly released version.
   * For example, if a new patch release is 2.4.3, you will update [src/scala/microsoft-spark-2.4.x/src/main/scala/org/apache/spark/deploy/dotnet/DotnetRunner.scala](/src/scala/microsoft-spark-2.4.x/src/main/scala/org/apache/spark/deploy/dotnet/DotnetRunner.scala).
3. Update the [azure-pipelines.yml](/azure-pipelines.yml) to include E2E testing for the newly released version.

Refer to [this commit](https://github.com/dotnet/spark/commit/eb26baa46200bfcbe3e1080e650f335853d9990e) for an example.

### [FEATURE]: Upgrading for a Minor Release Version
*WIP*

### [MAJOR]: Upgrading for a Major Release Version
*WIP*
