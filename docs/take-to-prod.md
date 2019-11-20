Taking your Spark .Net Application to Production
===

# Table of Contents
This how-to provides general instructions on how to take your .NET for Apache Spark application to production.
In this documentation, we will summary the most commonly asked scenarios when running Spark .Net Application.
And you will also learn how to package your application and submit your application with [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) and [Apachy Livy](https://livy.incubator.apache.org/).
- [How to take your application to production when you have single dependency](#how-to-take-your-application-to-production-when-you-have-single-dependency)
  - [Scenarios - Scenario 1 and Scenario 2](#scenarios---single-dependency)
  - [Package your application](#package-your-application---single-dependency)
  - [Launch your application](#launch-your-application---single-dependency)
- [How to take your application to production when you have multiple dependencies](#how-to-take-your-application-to-production-when-you-have-multiple-dependencies)
  - [Scenarios - Scenario 3, Scenario 4, Scenario 5 and Scenario 6](#scenarios---multiple-dependencies)
  - [Package your application](#package-your-application---multiple-dependencies)
  - [Launch your application](#launch-your-application---multiple-dependencies)

## How to take your application to production when you have single dependency
### Scenarios - single dependency
#### Scenario 1. SparkSession code and business logic in the same Program.cs file
This would be the simple usecase when you have SparkSession code and business logic (UDFs) in the same Program.cs file and in the same project (e.g. mySparkApp.csproj).
#### Scenario 2. SparkSession code and business logic in the same project, but different .cs files
This would be the usecase when you have SparkSession code and business logic (UDFs) in the different .cs files but in the same project (e.g. SparkSession in Program.cs, business logic in BusinessLogic.cs and both are in mySparkApp.csproj).

### Package your application - single dependency
Please follow [Get Started](https://github.com/dotnet/spark/#get-started) to build your application in Scenario 1 and Scenario 2.

### Launch your application - single dependency
#### 1. Using spark-submit
Please see below as an example of running your app with `spark-submit` in Scenario 1 and Scenario 2.
```shell
%SPARK_HOME%\bin\spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local \
--files bin\Debug\netcoreapp3.0\mySparkApp.dll \
bin\Debug\netcoreapp3.0\microsoft-spark-2.4.x-0.6.0.jar \
dotnet bin\Debug\netcoreapp3.0\mySparkApp.dll <app arg 1> <app arg 2> ... <app arg n>
```
#### 2. Using Apache Livy
Please see below as an example of running your app with Apache Livy in Scenario 1 and Scenario 2.
```shell
{
    "file": "adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-2.4.x-0.6.0.jar",
    "className": "org.apache.spark.deploy.dotnet.DotnetRunner",
    "files": [“adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.dll" ],
    "args": ["dotnet","adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.dll","<app arg 1>","<app arg 2>,"...","<app arg n>"]
}
```

## How to take your application to production when you have multiple dependencies
### Scenarios - multiple dependencies
#### Scenario 3. SparkSession code in one project that references another project including the business logic
This would be the usecase when you have SparkSession code in one project (e.g. mySparkApp.csproj) and business logic (UDFs) in another project (e.g. businessLogic.csproj).
#### Scenario 4. SparkSession code references a function from a Nuget package that has been installed in the csproj
This would be the usecase when SparkSession code references a function from a Nuget package in the same project (e.g. mySparkApp.csproj).
#### Scenario 5. SparkSession code references a function from a DLL on the user machine
This would be the usecase when SparkSession code reference business logic (UDFs) on the user machine (e.g. SparkSession code in the mySparkApp.csproj and businessLogic.dll on a different machine). 
#### Scenario 6. SparkSession code references functions and business logic from multiple projects/solutions that themselves depend on multiple Nuget packages
This would be a more complex usecase when you have SparkSession code reference business logic (UDFs) and functions from nuget packages in multiple projects and/or solutions.

### Package your application - multiple dependencies
- Please follow [Get Started](https://github.com/dotnet/spark/#get-started) to build your mySparkApp.csproj in Scenario 4 and Scenario 5 (and businessLogic.csproj for Scenario 3).
- Please see detailed steps [here](https://github.com/dotnet/spark/tree/master/deployment#preparing-your-spark-net-app) on how to build, publish and zip your application in Scenario 6. After packaging your .Net for Spark application, you will have a zip file (e.g. mySparkApp.zip) which has all the dependencies.

### Launch your application - multiple dependencies
#### 1. Using spark-submit
- Please see below as an example of running your app with `spark-submit` in Scenario 3 and Scenario 5.
And you should use `--files bin\Debug\netcoreapp3.0\nugetLibrary.dll` in Scenario 4.
```shell
%SPARK_HOME%\bin\spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local \
--files bin\Debug\netcoreapp3.0\businessLogic.dll \
bin\Debug\netcoreapp3.0\microsoft-spark-2.4.x-0.6.0.jar \
dotnet bin\Debug\netcoreapp3.0\mySparkApp.dll <app arg 1> <app arg 2> ... <app arg n>
```
- Please see below as an example of running your app with `spark-submit` in Scenario 6.
```shell
spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS=./udfs \
--conf spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS=./myLibraries.zip \
--archives hdfs://<path to your files>/businessLogics.zip#udfs,hdfs://<path to your files>/myLibraries.zip \
hdfs://<path to jar file>/microsoft-spark-2.4.x-0.6.0.jar \
hdfs://<path to your files>/mySparkApp.zip mySparkApp <app arg 1> <app arg 2> ... <app arg n>
```
#### 2. Using Apache Livy
- Please see below as an example of running your app with Apache Livy in Scenario 3 and Scenario 5.
And you should use `"files": ["adl://<cluster name>.azuredatalakestore.net/<some dir>/nugetLibrary.dll"]` in Scenario 4.
```shell
{
    "file": "adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-2.4.x-0.6.0.jar",
    "className": "org.apache.spark.deploy.dotnet.DotnetRunner",
    "files": [“adl://<cluster name>.azuredatalakestore.net/<some dir>/businessLogic.dll" ],
    "args": ["dotnet","adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.dll","<app arg 1>","<app arg 2>,"...","<app arg n>"]
}
```
- Please see below as an example of running your app with Apache Livy in Scenario 6.
```shell
{
    "file": "adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar",
    "className": "org.apache.spark.deploy.dotnet.DotnetRunner",
    "conf": {"spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS": "./udfs, ./myLibraries.zip"},
    "archives": ["adl://<cluster name>.azuredatalakestore.net/<some dir>/businessLogics.zip#udfs”, "adl://<cluster name>.azuredatalakestore.net/<some dir>/myLibraries.zip”],
    "args": ["adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.zip","mySparkApp","<app arg 1>","<app arg 2>,"...","<app arg n>"]
}
```
