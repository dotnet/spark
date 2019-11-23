Taking your .NET for Apache Spark Application to Production
===

# Table of Contents
This how-to provides general instructions on how to take your .NET for Apache Spark application to production.
In this documentation, we will summarize the most commonly asked scenarios when running a .NET for Apache Spark Application.
You will also learn how to package your application and submit your application with [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) and [Apachy Livy](https://livy.incubator.apache.org/).
- [How to deploy your application when you have a single dependency](#how-to-deploy-your-application-when-you-have-a-single-dependency)
  - [Scenarios](#scenarios)
  - [Package your application](#package-your-application)
  - [Launch your application](#launch-your-application)
- [How to deploy your application when you have multiple dependencies](#how-to-deploy-your-application-when-you-have-multiple-dependencies)
  - [Scenarios](#scenarios-1)
  - [Package your application](#package-your-application-1)
  - [Launch your application](#launch-your-application-1)

## How to deploy your application when you have a single dependency
### Scenarios
#### 1. SparkSession code and business logic in the same Program.cs file
This would be the simple use case when you have `SparkSession` code and business logic (UDFs) in the same Program.cs file and in the same project (e.g. mySparkApp.csproj).
#### 2. SparkSession code and business logic in the same project, but different .cs files
This would be the use case when you have `SparkSession` code and business logic (UDFs) in the different .cs files but in the same project (e.g. SparkSession in Program.cs, business logic in BusinessLogic.cs and both are in mySparkApp.csproj).

### Package your application
Please follow [Get Started](https://github.com/dotnet/spark/#get-started) to build your application.

### Launch your application
#### 1. Using spark-submit
Please make sure you have [pre-requisites](https://github.com/dotnet/spark/blob/master/docs/getting-started/windows-instructions.md#pre-requisites) to run the following command.
```powershell
%SPARK_HOME%\bin\spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master yarn \
--deploy-mode cluster \
--files <some dir>\<dotnet version>\mySparkApp.dll \
<some dir>\<dotnet version>\microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar \
dotnet <some dir>\<dotnet version>\mySparkApp.dll <app arg 1> <app arg 2> ... <app arg n>
```
#### 2. Using Apache Livy
```shell
{
    "file": "adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar",
    "className": "org.apache.spark.deploy.dotnet.DotnetRunner",
    "files": [“adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.dll" ],
    "args": ["dotnet","adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.dll","<app arg 1>","<app arg 2>,"...","<app arg n>"]
}
```

## How to deploy your application when you have multiple dependencies
### Scenarios
#### 1. SparkSession code in one project that references another project including the business logic
This would be the use case when you have `SparkSession` code in one project (e.g. mySparkApp.csproj) and business logic (UDFs) in another project (e.g. businessLogic.csproj).
#### 2. SparkSession code references a function from a Nuget package that has been installed in the csproj
This would be the use case when `SparkSession` code references a function from a Nuget package in the same project (e.g. mySparkApp.csproj).
#### 3. SparkSession code references a function from a DLL on the user's machine
This would be the use case when `SparkSession` code reference business logic (UDFs) on the user's machine (e.g. `SparkSession` code in the mySparkApp.csproj and businessLogic.dll on a different machine). 
#### 4. SparkSession code references functions and business logic from multiple projects/solutions that themselves depend on multiple Nuget packages
This would be a more complex use case when you have `SparkSession` code reference business logic (UDFs) and functions from Nuget packages in multiple projects and/or solutions.

### Package your application
Please see detailed steps [here](https://github.com/dotnet/spark/tree/master/deployment#preparing-your-spark-net-app) on how to build, publish and zip your application. After packaging your .NET for Apache Spark application, you will have a zip file (e.g. mySparkApp.zip) which has all the dependencies.

### Launch your application
#### 1. Using spark-submit
```shell
spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS=./udfs \
--conf spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS=./myLibraries.zip \
--archives hdfs://<path to your files>/businessLogics.zip#udfs,hdfs://<path to your files>/myLibraries.zip \
hdfs://<path to jar file>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar \
hdfs://<path to your files>/mySparkApp.zip mySparkApp <app arg 1> <app arg 2> ... <app arg n>
```
#### 2. Using Apache Livy
```shell
{
    "file": "adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar",
    "className": "org.apache.spark.deploy.dotnet.DotnetRunner",
    "conf": {"spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS": "./udfs, ./myLibraries.zip"},
    "archives": ["adl://<cluster name>.azuredatalakestore.net/<some dir>/businessLogics.zip#udfs”, "adl://<cluster name>.azuredatalakestore.net/<some dir>/myLibraries.zip”],
    "args": ["adl://<cluster name>.azuredatalakestore.net/<some dir>/mySparkApp.zip","mySparkApp","<app arg 1>","<app arg 2>,"...","<app arg n>"]
}
```
