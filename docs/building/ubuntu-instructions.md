Building Spark .NET on Ubuntu 18.04
==========================

# Table of Contents
- [Building Spark .NET on Ubuntu 18.04](#building-spark-net-on-ubuntu-1804)
- [Table of Contents](#table-of-contents)
- [Open Issues:](#open-issues)
- [Pre-requisites:](#pre-requisites)
- [Building](#building)
  - [Building Spark .NET Scala Extensions Layer](#building-spark-net-scala-extensions-layer)
  - [Building .NET Sample Applications using .NET 8 CLI](#building-net-sample-applications-using-net-8-cli)
- [Run Samples](#run-samples)

# Open Issues:
- [Building through Visual Studio Code]()

# Pre-requisites:

If you already have all the pre-requisites, skip to the [build](ubuntu-instructions.md#building) steps below.

  1. Download and install **[.NET 8 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)** - installing the SDK will add the `dotnet` toolchain to your path.
  2. Install **[OpenJDK 8](https://openjdk.java.net/install/)** 
     - You can use the following command:
       ```bash
       sudo apt install openjdk-8-jdk
       ```
     - Verify you are able to run `java` from your command-line
       <details>
       <summary>&#x1F4D9; Click to see sample java -version output</summary>
       
       ```
       openjdk version "1.8.0_191"
       OpenJDK Runtime Environment (build 1.8.0_191-8u191-b12-2ubuntu0.18.04.1-b12)
       OpenJDK 64-Bit Server VM (build 25.191-b12, mixed mode)
       ```
     - If you already have multiple OpenJDK versions installed and want to select OpenJDK 8, use the following command:
       ```bash
       sudo update-alternatives --config java
       ```
  3. Install **[Apache Maven 3.6.3+](https://maven.apache.org/download.cgi)**
     - Run the following command:
       ```bash
       mkdir -p ~/bin/maven
       cd ~/bin/maven
       wget https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
       tar -xvzf apache-maven-3.6.3-bin.tar.gz
       ln -s apache-maven-3.6.3 current
       export M2_HOME=~/bin/maven/current
       export PATH=${M2_HOME}/bin:${PATH}
       source ~/.bashrc
       ```
       
       Note that these environment variables will be lost when you close your terminal. If you want the changes to be permanent, add the `export` lines to your `~/.bashrc` file.
     - Verify you are able to run `mvn` from your command-line
       <details>
       <summary>&#x1F4D9; Click to see sample mvn -version output</summary>
       
       ```
       Apache Maven 3.6.3 (cecedd343002696d0abb50b32b541b8a6ba2883f)
       Maven home: ~/bin/apache-maven-3.6.3
       Java version: 1.8.0_242, vendor: Oracle Corporation, runtime: /usr/lib/jvm/java-8-openjdk-amd64/jre
       Default locale: en_US, platform encoding: ANSI_X3.4-1968
       OS name: "linux", version: "4.4.0-142-generic", arch: "amd64", family: "unix"
       ```
  4. Install **[Apache Spark 2.3+](https://spark.apache.org/downloads.html)**
     - Download [Apache Spark 2.3+](https://spark.apache.org/downloads.html) and extract it into a local folder (e.g., `~/bin/spark-2.3.2-bin-hadoop2.7`)
     - Add the necessary [environment variables](https://www.java.com/en/download/help/path.xml) `SPARK_HOME` e.g., `~/bin/spark-2.3.2-bin-hadoop2.7/`
       ```bash
       export SPARK_HOME=~/bin/spark-2.3.2-hadoop2.7
       export PATH="$SPARK_HOME/bin:$PATH"
       source ~/.bashrc
       ```
       
       Note that these environment variables will be lost when you close your terminal. If you want the changes to be permanent, add the `export` lines to your `~/.bashrc` file.
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

Please make sure you are able to run `dotnet`, `java`, `mvn`, `spark-shell` from your command-line before you move to the next section. Feel there is a better way? Please [open an issue](https://github.com/dotnet/spark/issues) and feel free to contribute.

# Building

For the rest of the section, it is assumed that you have cloned Spark .NET repo into your machine e.g., `~/dotnet.spark/`

```
git clone https://github.com/dotnet/spark.git ~/dotnet.spark
```

## Building Spark .NET Scala Extensions Layer

When you submit a .NET application, Spark .NET has the necessary logic written in Scala that inform Apache Spark how to handle your requests (e.g., request to create a new Spark Session, request to transfer data from .NET side to JVM side etc.). This logic can be found in the [Spark .NET Scala Source Code](../../src/scala).

Let us now build the Spark .NET Scala extension layer. This is easy to do:

```
cd src/scala
mvn clean package
```
You should see JARs created for the supported Spark versions:
* `microsoft-spark-2-3/target/microsoft-spark-2-3_2.11-<version>.jar`
* `microsoft-spark-2-4/target/microsoft-spark-2-4_2.11-<version>.jar`
* `microsoft-spark-3-0/target/microsoft-spark-3-0_2.12-<version>.jar`
* `microsoft-spark-3-0/target/microsoft-spark-3-5_2.12-<version>.jar`

## Building .NET Sample Applications using .NET 8 CLI

  1. Build the Worker
      ```bash
      cd ~/dotnet.spark/src/csharp/Microsoft.Spark.Worker/
      dotnet publish -f net8.0 -r linux-x64
      ```
      <details>
      <summary>&#x1F4D9; Click to see sample console output</summary>

      ```bash
      user@machine:/home/user/dotnet.spark/src/csharp/Microsoft.Spark.Worker$ dotnet publish -f net8.0 -r linux-x64
      Microsoft (R) Build Engine version 16.0.462+g62fb89029d for .NET Core
      Copyright (C) Microsoft Corporation. All rights reserved.
      
        Restore completed in 36.03 ms for /home/user/dotnet.spark/src/csharp/Microsoft.Spark.Worker/Microsoft.Spark.Worker.csproj.
        Restore completed in 35.94 ms for /home/user/dotnet.spark/src/csharp/Microsoft.Spark/Microsoft.Spark.csproj.
        Microsoft.Spark -> /home/user/dotnet.spark/artifacts/bin/Microsoft.Spark/Debug/netstandard2.0/Microsoft.Spark.dll
        Microsoft.Spark.Worker -> /home/user/dotnet.spark/artifacts/bin/Microsoft.Spark.Worker/Debug/net8.0/linux-x64/Microsoft.Spark.Worker.dll
        Microsoft.Spark.Worker -> /home/user/dotnet.spark/artifacts/bin/Microsoft.Spark.Worker/Debug/net8.0/linux-x64/publish/
      ```

      </details>

  2. Build the Samples
      ```bash
      cd ~/dotnet.spark/examples/Microsoft.Spark.CSharp.Examples/
      dotnet publish -f net8.0 -r linux-x64
      ```
      <details>
      <summary>&#x1F4D9; Click to see sample console output</summary>

      ```bash
      user@machine:/home/user/dotnet.spark/examples/Microsoft.Spark.CSharp.Examples$ dotnet publish -f net8.0 -r linux-x64
      Microsoft (R) Build Engine version 16.0.462+g62fb89029d for .NET Core
      Copyright (C) Microsoft Corporation. All rights reserved.

        Restore completed in 37.11 ms for /home/user/dotnet.spark/src/csharp/Microsoft.Spark/Microsoft.Spark.csproj.
        Restore completed in 281.63 ms for /home/user/dotnet.spark/examples/Microsoft.Spark.CSharp.Examples/Microsoft.Spark.CSharp.Examples.csproj.
        Microsoft.Spark -> /home/user/dotnet.spark/artifacts/bin/Microsoft.Spark/Debug/netstandard2.0/Microsoft.Spark.dll
        Microsoft.Spark.CSharp.Examples -> /home/user/dotnet.spark/artifacts/bin/Microsoft.Spark.CSharp.Examples/Debug/net8.0/linux-x64/Microsoft.Spark.CSharp.Examples.dll
        Microsoft.Spark.CSharp.Examples -> /home/user/dotnet.spark/artifacts/bin/Microsoft.Spark.CSharp.Examples/Debug/net8.0/linux-x64/publish/
      ```

     </details>

# Run Samples

Once you build the samples, you can use `spark-submit` to submit your .NET 8 apps. Make sure you have followed the [pre-requisites](#pre-requisites) section and installed Apache Spark.

  1. Set the `DOTNET_WORKER_DIR` or `PATH` environment variable to include the path where the `Microsoft.Spark.Worker` binary has been generated (e.g., `~/dotnet.spark/artifacts/bin/Microsoft.Spark.Worker/Debug/net8.0/linux-x64/publish`)
  2. Open a terminal and go to the directory where your app binary has been generated (e.g., `~/dotnet.spark/artifacts/bin/Microsoft.Spark.CSharp.Examples/Debug/net8.0/linux-x64/publish`)
  3. Running your app follows the basic structure:
     ```bash
     spark-submit \
       [--jars <any-jars-your-app-is-dependent-on>] \
       --class org.apache.spark.deploy.dotnet.DotnetRunner \
       --master local \
       <path-to-microsoft-spark-jar> \
       <path-to-your-app-binary> <argument(s)-to-your-app>
     ```

     Here are some examples you can run:
     - **[Microsoft.Spark.Examples.Sql.Batch.Basic](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Batch/Basic.cs)**
         ```bash
         spark-submit \
         --class org.apache.spark.deploy.dotnet.DotnetRunner \
         --master local \
         ~/dotnet.spark/src/scala/microsoft-spark-<version>/target/microsoft-spark-<version>.jar \
         ./Microsoft.Spark.CSharp.Examples Sql.Batch.Basic $SPARK_HOME/examples/src/main/resources/people.json
         ```
     - **[Microsoft.Spark.Examples.Sql.Streaming.StructuredNetworkWordCount](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredNetworkWordCount.cs)**
         ```bash
         spark-submit \
         --class org.apache.spark.deploy.dotnet.DotnetRunner \
         --master local \
         ~/dotnet.spark/src/scala/microsoft-spark-<version>/target/microsoft-spark-<version>.jar \
         ./Microsoft.Spark.CSharp.Examples Sql.Streaming.StructuredNetworkWordCount localhost 9999
         ```
     - **[Microsoft.Spark.Examples.Sql.Streaming.StructuredKafkaWordCount (maven accessible)](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredKafkaWordCount.cs)**
         ```bash
         spark-submit \
         --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 \
         --class org.apache.spark.deploy.dotnet.DotnetRunner \
         --master local \
         ~/dotnet.spark/src/scala/microsoft-spark-<version>/target/microsoft-spark-<version>.jar \
         ./Microsoft.Spark.CSharp.Examples Sql.Streaming.StructuredKafkaWordCount localhost:9092 subscribe test
         ```
     - **[Microsoft.Spark.Examples.Sql.Streaming.StructuredKafkaWordCount (jars provided)](../../examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/StructuredKafkaWordCount.cs)**
         ```bash
         spark-submit \
         --jars path/to/net.jpountz.lz4/lz4-1.3.0.jar,path/to/org.apache.kafka/kafka-clients-0.10.0.1.jar,path/to/org.apache.spark/spark-sql-kafka-0-10_2.11-2.3.2.jar,`path/to/org.slf4j/slf4j-api-1.7.6.jar,path/to/org.spark-project.spark/unused-1.0.0.jar,path/to/org.xerial.snappy/snappy-java-1.1.2.6.jar \
         --class org.apache.spark.deploy.dotnet.DotnetRunner \
         --master local \
         ~/dotnet.spark/src/scala/microsoft-spark-<version>/target/microsoft-spark-<version>.jar \
         ./Microsoft.Spark.CSharp.Examples Sql.Streaming.StructuredKafkaWordCount localhost:9092 subscribe test
          ```

Feel this experience is complicated? Help us by taking up [Simplify User Experience for Running an App](https://github.com/dotnet/spark/issues/6)
