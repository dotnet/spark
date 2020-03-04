# Deploy Worker and UDF Binaries General Instruction

This how-to provides general instructions on how to deploy Worker and UDF (User-Defined Function) binaries, 
including which Environment Variables to set up and some commonly used parameters 
when launching applications with `spark-submit`.

## Configurations

### 1. Environment Variables
When deploying workers and writing UDFs, there are a few commonly used environment variables that you may need to set: 

<table>
  <tr>
    <td width="25%"><b>Environment Variable</b></td>
    <td width="75%"><b>Description</b></td>
  </tr>
  <tr>
    <td><b>DOTNET_WORKER_DIR</b></td>
    <td>Path where the <code>Microsoft.Spark.Worker</code> binary has been generated.</br>It's used by the Spark driver and will be passed to Spark executors. If this variable is not set up, the Spark executors will search the path specified in the <code>PATH</code> environment variable.</br><i>e.g. "C:\bin\Microsoft.Spark.Worker"</i></td>
  </tr>
  <tr>
    <td><b>DOTNET_ASSEMBLY_SEARCH_PATHS</b></td>
    <td>Comma-separated paths where <code>Microsoft.Spark.Worker</code> will load assemblies.</br>Note that if a path starts with ".", the working directory will be prepended. If in <b>yarn mode</b>, "." would represent the container's working directory.</br><i>e.g. "C:\Users\&lt;user name&gt;\&lt;mysparkapp&gt;\bin\Debug\&lt;dotnet version&gt;"</i></td>
  </tr>
  <tr>
    <td><b>DOTNET_WORKER_DEBUG</b></td>
    <td>If you want to <a href="https://github.com/dotnet/spark/blob/master/docs/developer-guide.md#debugging-user-defined-function-udf">debug a UDF</a>, then set this environment variable to <code>1</code> before running <code>spark-submit</code>.</td>
  </tr>
</table>

### 2. Parameter Options
Once the Spark application is [bundled](https://spark.apache.org/docs/latest/submitting-applications.html#bundling-your-applications-dependencies), you can launch it using `spark-submit`. The following table shows some of the commonly used options: 

<table>
  <tr>
    <td width="25%"><b>Parameter Name</b></td>
    <td width="75%"><b>Description</b></td>
  </tr>
  <tr>
    <td><b>--class</b></td>
      <td>The entry point for your application.</br><i>e.g. org.apache.spark.deploy.dotnet.DotnetRunner</i></td>
  </tr>
  <tr>
    <td><b>--master</b></td>
    <td>The <a href="https://spark.apache.org/docs/latest/submitting-applications.html#master-urls">master URL</a> for the cluster.</br><i>e.g. yarn</i></td>
  </tr>
  <tr>
    <td><b>--deploy-mode</b></td>
    <td>Whether to deploy your driver on the worker nodes (<code>cluster</code>) or locally as an external client (<code>client</code>).</br>Default: <code>client</code></td>
  </tr>
  <tr>
    <td><b>--conf</b></td>
      <td>Arbitrary Spark configuration property in <code>key=value</code> format.</br><i>e.g. spark.yarn.appMasterEnv.DOTNET_WORKER_DIR=.\worker\Microsoft.Spark.Worker</i></td>
  </tr>
  <tr>
    <td><b>--files</b></td>
    <td>Comma-separated list of files to be placed in the working directory of each executor.</br>
      <ul>
        <li>Please note that this option is only applicable for yarn mode.</li>
        <li>It supports specifying file names with # similar to Hadoop.</br>
      </ul>
      <i>e.g. <code>myLocalSparkApp.dll#appSeen.dll</code>. Your application should use the name as <code>appSeen.dll</code> to reference <code>myLocalSparkApp.dll</code> when running on YARN.</i></li></td>
  </tr>
  <tr>
    <td><b>--archives</b></td>
    <td>Comma-separated list of archives to be extracted into the working directory of each executor.</br>
      <ul>
        <li>Please note that this option is only applicable for yarn mode.</li>
        <li>It supports specifying file names with # similar to Hadoop.</br>
      </ul>
      <i>e.g. <code>hdfs://&lt;path to your worker file&gt;/Microsoft.Spark.Worker.zip#worker</code>. This will copy and extract the zip file to <code>worker</code> folder.</i></li></td>
  </tr>
  <tr>
    <td><b>application-jar</b></td>
    <td>Path to a bundled jar including your application and all dependencies.</br>
    <i>e.g. hdfs://&lt;path to your jar&gt;/microsoft-spark-&lt;version&gt;.jar</i></td>
  </tr>
  <tr>
    <td><b>application-arguments</b></td>
    <td>Arguments passed to the main method of your main class, if any.</br><i>e.g. hdfs://&lt;path to your app&gt;/&lt;your app&gt;.zip &lt;your app name&gt; &lt;app args&gt;</i></td>
  </tr>
</table>

> Note: Please specify all the `--options` before `application-jar` when launching applications with `spark-submit`, otherwise they will be ignored. Please see more `spark-submit` options [here](https://spark.apache.org/docs/latest/submitting-applications.html) and running spark on YARN details [here](https://spark.apache.org/docs/latest/running-on-yarn.html).

## FAQ
#### 1. Question: When I run a spark app with UDFs, I get the following error. What should I do?
> **Error:** [ ] [ ] [Error] [TaskRunner] [0] ProcessStream() failed with exception: System.IO.FileNotFoundException: Assembly 'mySparkApp, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null' file not found: 'mySparkApp.dll'

**Answer:** Please check if the `DOTNET_ASSEMBLY_SEARCH_PATHS` environment variable is set correctly. It should be the path that contains your `mySparkApp.dll`.

#### 2. Question: After I upgraded my Spark Dotnet version and reset the `DOTNET_WORKER_DIR` environment variable, why do I still get the following error?
> **Error:** Lost task 0.0 in stage 11.0 (TID 24, localhost, executor driver): java.io.IOException: Cannot run program "Microsoft.Spark.Worker.exe": CreateProcess error=2, The system cannot find the file specified.

**Answer:** Please try restarting your PowerShell window (or other command windows) first so that it can take the latest environment variable values. Then start your program.

#### 3. Question: After submitting my Spark application, I get the error `System.TypeLoadException: Could not load type 'System.Runtime.Remoting.Contexts.Context'`.
> **Error:** [ ] [ ] [Error] [TaskRunner] [0] ProcessStream() failed with exception: System.TypeLoadException: Could not load type 'System.Runtime.Remoting.Contexts.Context' from assembly 'mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=...'.

**Answer:** Please check the `Microsoft.Spark.Worker` version you are using. We currently provide two versions: **.NET Framework 4.6.1** and **.NET Core 2.1.x**. In this case, `Microsoft.Spark.Worker.net461.win-x64-<version>` (which you can download [here](https://github.com/dotnet/spark/releases)) should be used since `System.Runtime.Remoting.Contexts.Context` is only for .NET Framework.

#### 4. Question: How to run my spark application with UDFs on YARN? Which environment variables and parameters should I use?

**Answer:** To launch the spark application on YARN, the environment variables should be specified as `spark.yarn.appMasterEnv.[EnvironmentVariableName]`. Please see below as an example using `spark-submit`:
```shell
spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.appMasterEnv.DOTNET_WORKER_DIR=./worker/Microsoft.Spark.Worker-<version> \
--conf spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS=./udfs \
--archives hdfs://<path to your files>/Microsoft.Spark.Worker.net461.win-x64-<version>.zip#worker,hdfs://<path to your files>/mySparkApp.zip#udfs \
hdfs://<path to jar file>/microsoft-spark-2.4.x-<version>.jar \
hdfs://<path to your files>/mySparkApp.zip mySparkApp
```
