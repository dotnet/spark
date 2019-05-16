# Developer Guide

## Table of Contents
- [How to Debug](#how-to-debug)
- [How to Support New Spark Releases](#support-new-releases)

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

<a name="support-new-releases"></a>
## How to Support New Spark Releases

### New patch version release

### New minor version release
*WIP*

### New major version release
*WIP*
