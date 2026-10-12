# Developer Guide

## Table of Contents
- [How to Do Local Debugging](#how-to-do-local-debugging)
- [How to Support New Spark Releases](#how-to-support-new-spark-releases)
- [Preparing a .NET for Apache Spark Release](release-guide.md)

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
* .NET Backend running debug mode. Press enter to exit *
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
   * For a Spark 3.5 patch release, update [src/scala/microsoft-spark-3-5/pom.xml](../src/scala/microsoft-spark-3-5/pom.xml) to the selected `spark.version`.
   * Spark 4.0 uses one [Scala 2.13 bridge](../src/scala/microsoft-spark-4-0/pom.xml), currently built against Spark 4.0.4 with JDK 17. It is built separately from the JDK 8 Spark 3.x reactor; a patch release does not require a new bridge module.
2. Update `DotnetRunner.supportedSparkVersions` to include the newly released version.
   * For Spark 3.5, update [DotnetRunner.scala](../src/scala/microsoft-spark-3-5/src/main/scala/org/apache/spark/deploy/dotnet/DotnetRunner.scala).
   * For Spark 4.0, update its [DotnetRunner.scala](../src/scala/microsoft-spark-4-0/src/main/scala/org/apache/spark/deploy/dotnet/DotnetRunner.scala); the current version list is 4.0.0–4.0.4.
3. Update [azure-pipelines-pr.yml](../azure-pipelines-pr.yml) and, where needed, the shared [E2E template](../azure-pipelines-e2e-tests-template.yml) to validate the newly released version on the supported platforms. Check the selected test filters before interpreting a green run as compatibility evidence.
4. Build the release artifacts and validate the packaged NuGet, bridge JAR, and Worker together. A source-build test alone does not verify package contents; use the JAR included in that package and a Worker from the same build. The [release pipeline](../azure-pipelines-release.yml) checks the final NuGet against the freshly built Spark 4 bridge, then runs a Spark 4.0.4 Driver/Collect/scalar-UDF smoke test on Windows and Linux using the final NuGet and Worker ZIPs. This complements, rather than replaces, the full source E2E matrix.

To repeat the packaged-artifact smoke test locally with PowerShell 7, install Spark 4.0.4 and JDK 17 (plus the Windows Hadoop prerequisites when applicable), then run:

```powershell
./eng/Run-SparkReleaseSmokeTest.ps1 -PackageDirectory '<release-artifacts>' -SparkHome '<spark-4.0.4-bin-hadoop3>' -WorkDirectory '<new-work-directory>'
```

The artifact directory must contain exactly one core `Microsoft.Spark` NuGet package and its matching .NET 8 Worker ZIP for the current platform. The script uses a fresh NuGet cache and the packaged JAR; its logs remain in the new work directory. A local unsigned run does not validate release signatures. For RC version settings, final-artifact checks and publication steps, follow the [release guide](release-guide.md).

Refer to [this historical commit](https://github.com/dotnet/spark/commit/eb26baa46200bfcbe3e1080e650f335853d9990e) for an example of the process, not the current supported versions or file paths.

### [FEATURE]: Upgrading for a Minor Release Version
*WIP*

### [MAJOR]: Upgrading for a Major Release Version
*WIP*
