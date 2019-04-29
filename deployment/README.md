Deploying your App on the Cloud
===

# Table of Contents
- [Pre-requisites](#pre-requisites)
- [Preparing Worker Dependencies](#preparing-worker-dependencies)
- [Cloud Deployment](#cloud-deployment)
  - [Azure HDInsight Spark](#azure-hdinsight-spark)
     - [Deploy Worker to Spark Cluster](#deploy-microsoftsparkworker)
     - [App deployment using spark-submit](#using-spark-submit)
     - [App deployment using Apache Livy](#using-apache-livy)
  - [Amazon EMR Spark](#amazon-emr-spark)
     - [Deploy Worker to Spark Cluster](#deploy-microsoftsparkworker-1)
     - [App deployment using spark-submit](#using-spark-submit-1)
     - [App deployment using Amazon EMR Steps](#using-amazon-emr-steps)
  - [Databricks (Azure & AWS)](#databricks)
     - [Deploy Worker to Spark Cluster](#deploy-microsoftsparkworker-2)
     - [App deployment using spark-submit](#using-spark-submit-2)

# Pre-requisites:
1. Tool to copy files to a distributed file system.
   - ADLS, WASB &rarr; [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
   - S3 &rarr; [AWS CLI](https://aws.amazon.com/cli/)
2. Download [install-worker.sh](install-worker.sh) to your local machine. This is a helper script that we will use later in the installation section to copy Spark .NET dependent files into your Spark cluster's worker nodes. install-worker.sh takes in three parameters:
   1. The Cloud Provider: `azure` or `aws`
   2. URI where `Microsoft.Spark.Worker.<release>.tar.gz` is uploaded (see the [Microsoft.Spark.Worker section](#microsoftsparkworker) for instructions on where to download this)
   3. Path on the executor node where the worker package will be installed (the path should be the directory that `yarn` user has access to).
   
   Example Usage: 
   ```shell
   install-worker.sh azure adl://<cluster name>.azuredatalakestore.net/<some dir>/Microsoft.Spark.Worker.<release>.tar.gz /usr/local/bin
   ```

# Preparing Worker Dependencies
Microsoft.Spark.Worker is a backend component that lives on the individual worker nodes of your Spark cluster. When you want to execute a C# UDF (user-defined function), Spark needs to understand how to launch the .NET CLR to execute this UDF. Microsoft.Spark.Worker provides a collection of classes to Spark that enable this functionality.

## Microsoft.Spark.Worker
1. Select a [Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases) Linux netcoreapp release to be deployed on your cluster.
   * For example, if you want `.NET for Apache Spark v0.1.0` using `netcoreapp2.1`, you'd download [Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.1.0.tar.gz](https://github.com/dotnet/spark/releases/download/v0.1.0/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.1.0.tar.gz).
2. Upload `Microsoft.Spark.Worker.<release>.tar.gz` and [install-worker.sh](install-worker.sh) to a distributed file system (e.g., HDFS, WASB, ADLS, S3) that your cluster has access to.

## Your Spark .NET `app`
1. Follow the [Get Started](https://github.com/dotnet/spark/#get-started) guide to build your app.
2. Publish your Spark .NET `app` as self-contained.
   ```shell
   # For example, you can run the following on Linux.
   foo@bar:~/path/to/app$ dotnet publish -c Release -f netcoreapp2.1 -r ubuntu.16.04-x64
   ```
3. Produce `<your app>.zip` for the published files.
   ```shell
   # For example, you can run the following on Linux using `zip`.
   foo@bar:~/path/to/app/bin/Release/netcoreapp2.1/ubuntu.16.04-x64/publish$ zip -r <your app>.zip .
   ```
4. Upload the following to a distributed file system (e.g., HDFS, WASB, ADLS, S3) that your cluster has access to:
   * `microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar` (Included as part of the [Microsoft.Spark](https://www.nuget.org/packages/Microsoft.Spark/) nuget and is colocated in your app's build output directory)
   * `<your app>.zip`
   * Files (e.g., dependency files, common data accessible to every worker) or Assemblies (e.g., DLLs that contain your user-defined functions, libraries that your `app` depends on) to be placed in the working directory of each executor.

# Cloud Deployment
## Azure HDInsight Spark
[Azure HDInsight Spark](https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-overview) is the Microsoft implementation of Apache Spark in the cloud that allows users to launch and configure Spark clusters in Azure. You can use HDInsight Spark clusters to process your data stored in Azure (e.g., [Azure Storage](https://azure.microsoft.com/en-us/services/storage/) and [Azure Data Lake Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)).

> **Note**: Azure HDInsight Spark is Linux-based. Therefore, if you are interested in deploying your app to Azure HDInsight Spark, make sure your app is .NET Standard compatible and that you use [.NET Core compiler](https://dotnet.microsoft.com/download) to compile your app.

### Deploy Microsoft.Spark.Worker
*Note that this step is required only once*

#### Run HDInsight Script Action
Run `install-worker.sh` on the cluster using [HDInsight Script Actions](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux):

* Script type: Custom
* Name: Install Microsoft.Spark.Worker (or anything that is descriptive)
* Bash script URI: The URI to which you uploaded `install-worker.sh` (e.g. adl://\<cluster name\>.azuredatalakestore.net/\<some dir\>/install-worker.sh)
* Node type(s): Worker
* Parameters: Parameters to `install-worker.sh`. For example, if you uploaded to Azure Data Lake then it would be `azure adl://<cluster name>.azuredatalakestore.net/<some dir>/Microsoft.Spark.Worker.<release>.tar.gz /usr/local/bin`.

The following captures the setting for a HDInsight Script Action:

<img src="../docs/img/deployment-hdi-action-script.png" alt="ScriptActionImage" width="500"/>

### Run your app on the cloud!
#### Using [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html)
1. `ssh` into one of the head nodes in the cluster.
2. Run `spark-submit`:
   ```shell
   foo@bar:~$ $SPARK_HOME/bin/spark-submit \
   --master yarn \
   --class org.apache.spark.deploy.DotnetRunner \
   --files <comma-separated list of assemblies that contain UDF definitions, if any> \
   adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar \
   adl://<cluster name>.azuredatalakestore.net/<some dir>/<your app>.zip <your app> <app arg 1> <app arg 2> ... <app arg n>
   ```

#### Using [Apache Livy](https://livy.incubator.apache.org/) 
You can use Apache Livy, the Apache Spark REST API, to submit Spark .NET jobs to an Azure HDInsight Spark cluster as documented in [Remote jobs with Apache Livy](https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-livy-rest-interface).
```shell
# For example, you can run the following on Linux using `curl`.
foo@bar:~$ curl -k -v -X POST "https://<your spark cluster>.azurehdinsight.net/livy/batches" \
-u "<hdinsight username>:<hdinsight password>" \
-H "Content-Type: application/json" \
-H "X-Requested-By: <hdinsight username>" \
-d @- << EOF
{
    "file":"adl://<cluster name>.azuredatalakestore.net/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar",
    "className":"org.apache.spark.deploy.DotnetRunner",
    "files":["adl://<cluster name>.azuredatalakestore.net/<some dir>/<udf assembly>", "adl://<cluster name>.azuredatalakestore.net/<some dir>/<file>"],
    "args":["adl://<cluster name>.azuredatalakestore.net/<some dir>/<your app>.zip","<your app>","<app arg 1>","<app arg 2>,"...","<app arg n>"]
}
EOF
```

## Amazon EMR Spark
[Amazon EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html) is a managed cluster platform that simplifies running big data frameworks on AWS.

> **Note**: AWS EMR Spark is Linux-based. Therefore, if you are interested in deploying your app to AWS EMR Spark, make sure your app is .NET Standard compatible and that you use [.NET Core compiler](https://dotnet.microsoft.com/download) to compile your app.

### Deploy Microsoft.Spark.Worker
*Note that this step is only required at cluster creation*

#### Create cluster using Amazon EMR Bootstrap Actions
Run `install-worker.sh` during cluster creation using [Bootstrap Actions](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html).
```shell
# For example, you can run the following on Linux using `aws` cli.
foo@bar:~$ aws emr create-cluster \
--name "Test cluster" \
--release-label emr-5.23.0 \
--use-default-roles \
--ec2-attributes KeyName=myKey \
--applications Name=Spark \
--instance-count 3 \
--instance-type m1.medium \
--bootstrap-actions Path=s3://mybucket/<some dir>/install-worker.sh,Name="Install Microsoft.Spark.Worker",Args=["aws","s3://mybucket/<some dir>/Microsoft.Spark.Worker.<release>.tar.gz","/usr/local/bin"]
```

### Run your app on the cloud!
#### Using [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html)
1. `ssh` into one of the nodes in the cluster.
2. Run `spark-submit`:
   ```shell
   foo@bar:~$ spark-submit \
   --master yarn \
   --class org.apache.spark.deploy.DotnetRunner \
   --files <comma-separated list of assemblies that contain UDF definitions, if any> \
   s3://mybucket/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar \
   s3://mybucket/<some dir>/<your app>.zip <your app> <app args>
   ```

#### Using [Amazon EMR Steps](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html)
Amazon EMR Steps can be used to submit jobs to the Spark framework installed on the EMR cluster.
```bash
# For example, you can run the following on Linux using `aws` cli.
foo@bar:~$ aws emr add-steps \
--cluster-id j-xxxxxxxxxxxxx \
--steps Type=spark,Name="Spark Program",Args=[--master,yarn,--files,s3://mybucket/<some dir>/<udf assembly>,--class,org.apache.spark.deploy.DotnetRunner,s3://mybucket/<some dir>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar,s3://mybucket/<some dir>/<your app>.zip,<your app>,<app arg 1>,<app arg 2>,...,<app arg n>],ActionOnFailure=CONTINUE
```

## Databricks
[Databricks](http://databricks.com) is a platform that provides cloud-based big data processing using Apache Spark.

> **Note**: [Azure](https://azure.microsoft.com/en-us/services/databricks/) and [AWS](https://databricks.com/aws) Databricks is Linux-based. Therefore, if you are interested in deploying your app to Databricks, make sure your app is .NET Standard compatible and that you use [.NET Core compiler](https://dotnet.microsoft.com/download) to compile your app.

### Deploy Microsoft.Spark.Worker
*Note that this step is required only once*

#### Cluster Node Initialization Scripts
Using Databrick's [init script](https://docs.databricks.com/user-guide/clusters/init-scripts.html) mechanism, we will run a shell script during startup for each cluster node before the Spark driver or worker JVM starts.

1. Configure your [Data Source](https://docs.databricks.com/spark/latest/data-sources/index.html) and mount it using [Databricks File System](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#dbfs).
2. Use the following [init script](https://docs.databricks.com/user-guide/clusters/init-scripts.html) to install `Microsoft.Spark.Worker` on the cluster nodes.
   ```scala
   dbutils.fs.put("dbfs:/databricks/<cluster-scoped or global path>/install-worker-wrapper.sh" ,"""
   #!/bin/bash
   set +e

   /bin/bash /dbfs/<your mount>/<path to>/install-worker.sh local /dbfs/<your mount>/<path to>/Microsoft.Spark.Worker.<release>.tar.gz /usr/local/bin
   """, true)
   ```
3. Restart the cluster.

### Run your app on the cloud!
#### Using [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html)
1. [Create a Job](https://docs.databricks.com/user-guide/jobs.html) and select *Configure spark-submit*.
2. Configure `spark-submit` with the following parameters:
   ```shell
   ["--files","/dbfs/<your mount>/<path-to>/<app assembly/file to deploy to worker>","--class"," org.apache.spark.deploy.DotnetRunner","/dbfs/<your mount>/<path to>/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar","/dbfs/<your mount>/<path to>/<app name>.zip","<app bin name>","app arg1","app arg2"]
   ```
