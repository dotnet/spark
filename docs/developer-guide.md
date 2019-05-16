# Developer Guide

## Table of Contents
- [How to Debug](#how-to-debug)
- [How to Support New Spark Releases](#how-to-support-new-spark-releases)

## How to debug

### Debugging .NET application

Open a new command prompt window, run the following:
```shell
spark-submit \
  --class org.apache.spark.deploy.DotnetRunner \
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

Now you can run your .NET application with any debugger to debug your application.

### Debugging Scala code

If you need to debug the Scala side code (`DotnetRunner`, `DotnetBackendHandler`, etc.), you can use the following command, and attach a debugger to the running process using [Intellij](https://www.jetbrains.com/help/idea/attaching-to-local-process.html):

```shell
spark-submit \
  --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
  --class org.apache.spark.deploy.DotnetRunner \
  --master local \
  <path-to-microsoft-spark-jar> \
  <path-to-your-app-exe> <argument(s)-to-your-app>
```

## How to Support New Spark Releases

### New patch version release
Apache Spark's new patch release involves only the internal changes such as bug fixes, etc. Thus, it is straightfoward to support a new patch version release.

1. In the corresponding `pom.xml`, update the `spark.version` value to the newly released version.
   * For example, if a new patch release is 2.4.3, you will update `src/scala/microsoft-spark-2.4.x/pom.xml` to have `<spark.version>2.4.3</spark.version>`.
2. Update `DotnetRunner.supportedSparkVersions` to include the newly released version.
   * For example, if a new patch release is 2.4.3, you will update `src/scala/microsoft-spark-2.4.x/src/main/scala/org/apache/spark/deploy/DotnetRunner.scala`.
3. Update the build pipeline to include E2E testing for the newly released version.

Refer to [] for an example.

### New minor version release
*WIP*

### New major version release
*WIP*
