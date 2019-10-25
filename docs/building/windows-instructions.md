Building Spark .NET on Windows
==========================

# Table of Contents
- [Open Issues](#open-issues)
- [Pre-requisites](#pre-requisites)
- [Building](#building)
  - [Building Spark .NET Scala Extensions Layer](#building-spark-net-scala-extensions-layer)
  - [Building .NET Samples Application](#building-net-samples-application)
    - [Using Visual Studio for .NET Framework](#using-visual-studio-for-net-framework)
    - [Using .NET Core CLI for .NET Core](#using-net-core-cli-for-net-core)
- [Run Samples](#run-samples)

# Open Issues:
- [Allow users to choose which .NET framework to build for]()
- [Building through Visual Studio Code]()
- [Building fully automatically through .NET Core CLI]()

# Pre-requisites:

If you already have all the pre-requisites, skip to the [build](windows-instructions.md#building) steps below.

  1. Download and install the **[.NET Core SDK](https://dotnet.microsoft.com/download/dotnet-core/2.1)** - installing the SDK will add the `dotnet` toolchain to your path. .NET Core 2.1, 2.2 and 3.0 preview are supported.
  2. Install any edition of **[Visual Studio 2019](https://www.visualstudio.com/downloads/)** or [Visual Studio 2017](https://www.visualstudio.com/downloads/). The Community version is completely free. When configuring your installation, include these components at minimum:
     * .NET desktop development
       * All Required Components
         * .NET Framework 4.6.1 Development Tools
     * .NET Core cross-platform development	
       * All Required Components
  3. Install **[Java 1.8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)** 
     - Select the appropriate version for your operating system e.g., jdk-8u201-windows-x64.exe for Win x64 machine.
     - Install using the installer and verify you are able to run `java` from your command-line
  4. Install **[Apache Maven 3.6.0+](https://maven.apache.org/download.cgi)**
     - Download [Apache Maven 3.6.0](http://mirror.metrocast.net/apache/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.zip)
     - Extract to a local directory e.g., `c:\bin\apache-maven-3.6.0\`
     - Add Apache Maven to your [PATH environment variable](https://www.java.com/en/download/help/path.xml) e.g., `c:\bin\apache-maven-3.6.0\bin`
     - Verify you are able to run `mvn` from your command-line
  5. Install **[Apache Spark 2.3+](https://spark.apache.org/downloads.html)**
     - Download [Apache Spark 2.3+](https://spark.apache.org/downloads.html) and extract it into a local folder (e.g., `c:\bin\spark-2.3.2-bin-hadoop2.7\`) using [7-zip](https://www.7-zip.org/).
     - Add Apache Spark to your [PATH environment variable](https://www.java.com/en/download/help/path.xml) e.g., `c:\bin\spark-2.3.2-bin-hadoop2.7\bin`
     - Add a [new environment variable](https://www.java.com/en/download/help/path.xml) `SPARK_HOME` e.g., `C:\bin\spark-2.3.2-bin-hadoop2.7\`
     - Verify you are able to run `spark-shell` from your command-line
        <details>
        <summary>&#x1F4D9; Click to see sample console output</summary>

        ```
        Welcome to
              ____              __
             / __/__  ___ _____/ /__
            _\ \/ _ \/ _ `/ __/  '_/
           /___/ .__/\_,_/_/ /_/\_\   version 2.3.2
              /_/

        Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_201)
        Type in expressions to have them evaluated.
        Type :help for more information.

        scala> sc
        res0: org.apache.spark.SparkContext = org.apache.spark.SparkContext@6eaa6b0c
        ```

        </details>

  6. Install **[WinUtils](https://github.com/steveloughran/winutils)**
     - Download `winutils.exe` binary from [WinUtils repository](https://github.com/steveloughran/winutils). You should select the version of Hadoop the Spark distribution was compiled with, e.g. use hadoop-2.7.1 for Spark 2.3.2.
     - Save `winutils.exe` binary to a directory of your choice e.g., `c:\hadoop\bin`
     - Set `HADOOP_HOME` to reflect the directory with winutils.exe (without bin). For instance, using command-line:
       ```powershell
       set HADOOP_HOME=c:\hadoop
       ```
     - Set PATH environment variable to include `%HADOOP_HOME%\bin`. For instance, using command-line:
       ```powershell
       set PATH=%HADOOP_HOME%\bin;%PATH%
       ```


Please make sure you are able to run `dotnet`, `java`, `mvn`, `spark-shell` from your command-line before you move to the next section. Feel there is a better way? Please [open an issue](https://github.com/dotnet/spark/issues) and feel free to contribute.

> **Note**: A new instance of the command-line may be required if any environment variables were updated.

# Building

For the rest of the section, it is assumed that you have cloned Spark .NET repo into your machine e.g., `c:\github\dotnet-spark\`

```powershell
git clone https://github.com/dotnet/spark.git c:\github\dotnet-spark
```

## Building Spark .NET Scala Extensions Layer

When you submit a .NET application, Spark .NET has the necessary logic written in Scala that inform Apache Spark how to handle your requests (e.g., request to create a new Spark Session, request to transfer data from .NET side to JVM side etc.). This logic can be found in the [Spark .NET Scala Source Code](../../../src/scala).

Regardless of whether you are using .NET Framework or .NET Core, you will need to build the Spark .NET Scala extension layer. This is easy to do:

```powershell
cd src\scala
mvn clean package 
```
You should see JARs created for the supported Spark versions:
* `microsoft-spark-2.3.x\target\microsoft-spark-2.3.x-<version>.jar`
* `microsoft-spark-2.4.x\target\microsoft-spark-2.4.x-<version>.jar`

## Building .NET Samples Application

### Using Visual Studio for .NET Framework

  1. Open `src\csharp\Microsoft.Spark.sln` in Visual Studio and build the `Microsoft.Spark.CSharp.Examples` project under the `examples` folder (this will in turn build the .NET bindings project as well). If you want, you can write your own code in the `Microsoft.Spark.Examples` project:
  
      ```csharp
        // Instantiate a session
        var spark = SparkSession
            .Builder()
            .AppName("Hello Spark!")
            .GetOrCreate();

        var df = spark.Read().Json(args[0]);

        // Print schema
        df.PrintSchema();

        // Apply a filter and show results
        df.Filter(df["age"] > 21).Show();
      ```
     Once the build is successfuly, you will see the appropriate binaries produced in the output directory.
     <details>
     <summary>&#x1F4D9; Click to see sample console output</summary>
     
      ```powershell
            Directory: C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.CSharp.Examples\Debug\net461


        Mode                LastWriteTime         Length Name
        ----                -------------         ------ ----
        -a----         3/6/2019  12:18 AM         125440 Apache.Arrow.dll
        -a----        3/16/2019  12:00 AM          13824 Microsoft.Spark.CSharp.Examples.exe
        -a----        3/16/2019  12:00 AM          19423 Microsoft.Spark.CSharp.Examples.exe.config
        -a----        3/16/2019  12:00 AM           2720 Microsoft.Spark.CSharp.Examples.pdb
        -a----        3/16/2019  12:00 AM         143360 Microsoft.Spark.dll
        -a----        3/16/2019  12:00 AM          63388 Microsoft.Spark.pdb
        -a----        3/16/2019  12:00 AM          34304 Microsoft.Spark.Worker.exe
        -a----        3/16/2019  12:00 AM          19423 Microsoft.Spark.Worker.exe.config
        -a----        3/16/2019  12:00 AM          11900 Microsoft.Spark.Worker.pdb
        -a----        3/16/2019  12:00 AM          23552 Microsoft.Spark.Worker.xml
        -a----        3/16/2019  12:00 AM         332363 Microsoft.Spark.xml
        ------------------------------------------- More framework files -------------------------------------
      ```

      </details>

### Using .NET Core CLI for .NET Core

> Note: We are currently working on automating .NET Core builds for Spark .NET. Until then, we appreciate your patience in performing some of the steps manually.

  1. Build the Worker
      ```powershell
      cd C:\github\dotnet-spark\src\csharp\Microsoft.Spark.Worker\
      dotnet publish -f netcoreapp2.1 -r win10-x64
      ```
      <details>
      <summary>&#x1F4D9; Click to see sample console output</summary>

      ```powershell
      PS C:\github\dotnet-spark\src\csharp\Microsoft.Spark.Worker> dotnet publish -f netcoreapp2.1 -r win10-x64
      Microsoft (R) Build Engine version 16.0.462+g62fb89029d for .NET Core
      Copyright (C) Microsoft Corporation. All rights reserved.

        Restore completed in 299.95 ms for C:\github\dotnet-spark\src\csharp\Microsoft.Spark\Microsoft.Spark.csproj.
        Restore completed in 306.62 ms for C:\github\dotnet-spark\src\csharp\Microsoft.Spark.Worker\Microsoft.Spark.Worker.csproj.
        Microsoft.Spark -> C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark\Debug\netstandard2.0\Microsoft.Spark.dll
        Microsoft.Spark.Worker -> C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.Worker\Debug\netcoreapp2.1\win10-x64\Microsoft.Spark.Worker.dll
        Microsoft.Spark.Worker -> C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.Worker\Debug\netcoreapp2.1\win10-x64\publish\
      ```

      </details>
  2. Build the Samples
      ```powershell
      cd C:\github\dotnet-spark\examples\Microsoft.Spark.CSharp.Examples\
      dotnet publish -f netcoreapp2.1 -r win10-x64
      ```
      <details>
      <summary>&#x1F4D9; Click to see sample console output</summary>

      ```powershell
      PS C:\github\dotnet-spark\examples\Microsoft.Spark.CSharp.Examples> dotnet publish -f netcoreapp2.1 -r win10-x64
      Microsoft (R) Build Engine version 16.0.462+g62fb89029d for .NET Core
      Copyright (C) Microsoft Corporation. All rights reserved.

        Restore completed in 44.22 ms for C:\github\dotnet-spark\src\csharp\Microsoft.Spark\Microsoft.Spark.csproj.
        Restore completed in 336.94 ms for C:\github\dotnet-spark\examples\Microsoft.Spark.CSharp.Examples\Microsoft.Spark.CSharp.Examples.csproj.
        Microsoft.Spark -> C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark\Debug\netstandard2.0\Microsoft.Spark.dll
        Microsoft.Spark.CSharp.Examples -> C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.CSharp.Examples\Debug\netcoreapp2.1\win10-x64\Microsoft.Spark.CSharp.Examples.dll
        Microsoft.Spark.CSharp.Examples -> C:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.CSharp.Examples\Debug\netcoreapp2.1\win10-x64\publish\
      ```

      </details>

# Run Samples

Once you build the samples, running them will be through `spark-submit` regardless of whether you are targeting .NET Framework or .NET Core apps. Make sure you have followed the [pre-requisites](#pre-requisites) section and installed Apache Spark.

  1. Set the `DOTNET_WORKER_DIR` or `PATH` environment variable to include the path where the `Microsoft.Spark.Worker` binary has been generated (e.g., `c:\github\dotnet\spark\artifacts\bin\Microsoft.Spark.Worker\Debug\net461` for .NET Framework, `c:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.Worker\Debug\netcoreapp2.1\win10-x64\publish` for .NET Core)
  2. Open Powershell and go to the directory where your app binary has been generated (e.g., `c:\github\dotnet\spark\artifacts\bin\Microsoft.Spark.CSharp.Examples\Debug\net461` for .NET Framework, `c:\github\dotnet-spark\artifacts\bin\Microsoft.Spark.CSharp.Examples\Debug\netcoreapp2.1\win10-x64\publish` for .NET Core)
  3. Running your app follows the basic structure:
     ```powershell
     spark-submit.cmd `
       [--jars <any-jars-your-app-is-dependent-on>] `
       --class org.apache.spark.deploy.dotnet.DotnetRunner `
       --master local `
       <path-to-microsoft-spark-jar> `
       <path-to-your-app-exe> <argument(s)-to-your-app>
     ```

     Here are some examples you can run:
     - **[Microsoft.Spark.Examples.Sql.Basic](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Basic.cs)**
         ```powershell
         spark-submit.cmd `
         --class org.apache.spark.deploy.dotnet.DotnetRunner `
         --master local `
         C:\github\dotnet-spark\src\scala\microsoft-spark-2.3.x\target\microsoft-spark-2.3.x-0.6.0.jar `
         Microsoft.Spark.CSharp.Examples.exe Sql.Basic %SPARK_HOME%\examples\src\main\resources\people.json
         ```
     - **[Microsoft.Spark.Examples.Sql.Streaming.StructuredNetworkWordCount](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredNetworkWordCount.cs)**
         ```powershell
         spark-submit.cmd `
         --class org.apache.spark.deploy.dotnet.DotnetRunner `
         --master local `
         C:\github\dotnet-spark\src\scala\microsoft-spark-2.3.x\target\microsoft-spark-2.3.x-0.6.0.jar `
         Microsoft.Spark.CSharp.Examples.exe Sql.Streaming.StructuredNetworkWordCount localhost 9999
         ```
     - **[Microsoft.Spark.Examples.Sql.Streaming.StructuredKafkaWordCount (maven accessible)](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredKafkaWordCount.cs)**
         ```powershell
         spark-submit.cmd `
         --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 `
         --class org.apache.spark.deploy.dotnet.DotnetRunner `
         --master local `
         C:\github\dotnet-spark\src\scala\microsoft-spark-2.3.x\target\microsoft-spark-2.3.x-0.6.0.jar `
         Microsoft.Spark.CSharp.Examples.exe Sql.Streaming.StructuredKafkaWordCount localhost:9092 subscribe test
         ```
     - **[Microsoft.Spark.Examples.Sql.Streaming.StructuredKafkaWordCount (jars provided)](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredKafkaWordCount.cs)**
         ```powershell
         spark-submit.cmd 
         --jars path\to\net.jpountz.lz4\lz4-1.3.0.jar,path\to\org.apache.kafka\kafka-clients-0.10.0.1.jar,path\to\org.apache.spark\spark-sql-kafka-0-10_2.11-2.3.2.jar,`path\to\org.slf4j\slf4j-api-1.7.6.jar,path\to\org.spark-project.spark\unused-1.0.0.jar,path\to\org.xerial.snappy\snappy-java-1.1.2.6.jar `
         --class org.apache.spark.deploy.dotnet.DotnetRunner `
         --master local `
         C:\github\dotnet-spark\src\scala\microsoft-spark-2.3.x\target\microsoft-spark-2.3.x-0.6.0.jar `
         Microsoft.Spark.CSharp.Examples.exe Sql.Streaming.StructuredKafkaWordCount localhost:9092 subscribe test
          ```

Feel this experience is complicated? Help us by taking up [Simplify User Experience for Running an App](https://github.com/dotnet/spark/issues/6)
