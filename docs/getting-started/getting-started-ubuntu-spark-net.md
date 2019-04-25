# Getting Started with Spark.NET on Ubuntu

These instructions help get you started with Spark on Ubuntu 18.04

## Download and Install Prerequisites

- [Java 1.8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Spark 2.4.x](https://spark.apache.org/downloads.html)
- Maven
- [.NET Core SDK 2.1](https://dotnet.microsoft.com/download/dotnet-core/2.1)

### Install Java

Navigate to the following [link](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and download `jdk-8u211-linux-x64.tar.gz`.

Then, extract the contents of the `tar.gz` folder:

```bash
tar -xvzf jdk-8u211-linux-x64.tar.gz
```

Add Java 1.8 to the list of Java versions on your system:

```bash
sudo update-alternatives --install "/usr/bin/java" "java" "/home/$USER/jdk1.8.0_211/bin/java" 1500
sudo update-alternatives --install "/usr/bin/javac" "javac" "/home/$USER/jdk1.8.0_211/bin/javac" 1500
sudo update-alternatives --install "/usr/bin/javaws" "javaws" "/home/$USER/jdk1.8.0_211/bin/javaws" 1500
```

Follow the prompts and select 1.8 as the version:

```
sudo update-alternatives
```

### Download Spark

Download the latest version of Spark:

```bash
wget https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz
```

Extract the contents of the compressed folder:

```bash
tar -xvzf spark-2.4.1-bin-hadoop2.7.tgz
```

### Install .NET Core SDK

Set up the respositories

```bash
wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
```

Install the SDK

```bash
sudo add-apt-repository universe
sudo apt-get install apt-transport-https
sudo apt-get update
sudo apt-get install dotnet-sdk-2.1
```

### Download Microsoft.Spark.Worker

```bash
wget https://github.com/dotnet/spark/releases/download/v0.1.0/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.1.0.tar.gz
```

Extract the contents of the compressed directory

```bash
tar -xvzf Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.1.0.tar.gz
```

## Set Environment Variables

Set up environment variables with the following commands:

```bash
echo "export JAVA_HOME=~/jdk1.8.0_211" >> ~/.bashrc
echo "export SPARK_HOME=~/spark-2.4.1-bin-hadoop2.7" >> ~/.bashrc
echo "export DotnetWorkerPath=~/Microsoft.Spark.Worker-0.1.0" >> ~/.bashrc
echo "export M2_HOME=~/bin/maven/current" >> ~/.bashrc
export PATH=${M2_HOME}/bin:$PATH
echo "export PATH=$SPARK_HOME/bin:$PATH" >> ~/.bashrc
source ~/.bashrc
```

## Create Console Application

```bash
dotnet new console -o HelloSpark && cd HelloSpark
```

## Install Nuget Package

```bash
dotnet add package Microsoft.Spark
```

## Write The Program

Replace the contents of the `Program.cs` file with the following code:

```csharp
using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace HelloSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var df = spark.Read().Json("people.json");
            df.Show();
        }
    }
}
```

Add the following content to your `HelloSpark.csproj` file:

```xml
<ItemGroup>
    <Content Include="people.json">
        <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
    </Content>
</ItemGroup>
```

## Create Data

Inside of the `HelloSpark` directory, enter the following command

```bash
cat << EOF > people.json
{"name":"Michael"} 
{"name":"Andy", "age":30} 
{"name":"Justin", "age":19} 
EOF
```

## Build And Publish The Application

Build and publish the application with the following command:

```bash
dotnet publish -f netcoreapp2.1 -r linux-x64 ./HelloSpark.csproj
```

## Run The Application

From the `HelloSpark` directory, enter the following command to run the application:

```bash
spark-submit \
--class org.apache.spark.deploy.DotnetRunner \
--master local \
./bin/Debug/netcoreapp2.1/linux-x64/publish/microsoft-spark-2.4.x-0.1.0.jar \
./bin/Debug/netcoreapp2.1/linux-x64/publish/HelloSpark
```

The output of the application should look similar to the output below:

```text
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```