# Log Processing with Batch Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
to analyze a log file. In the world of big data, this is known as **log processing**.

## Problem

Our goal here is to determine if strings in a file are complete log entries and to then analyze their contents. We'll be analyzing the 
[Apache Unix Log Samples](http://www.monitorware.com/en/logsamples/apache.php). The zip file contains both access logs and error logs, 
and this sample is tailored for the access logs.

This sample is an example of **batch processing** since we're analyzing data that has already been stored and is not actively growing 
or changing.

## Solution

Let's explore how we can use Spark to tackle this problem.

### 1. Create a Spark Session

In any Spark application, we need to establish a new SparkSession, which is the entry point to programming Spark with the Dataset and 
DataFrame API.

```CSharp
SparkSession spark = SparkSession
                .Builder()
                .AppName("Apache User Log Processing")
                .GetOrCreate();
```

By calling on the *spark* object created above, we can access Spark and DataFrame functionality throughout our program.

### 2. Read Input File into a DataFrame

Now that we have an entry point to the Spark API, let's read in our log file. We'll store it in a DataFrame, while is a distributed collection of data organized into named columns.

```CSharp
DataFrame df = spark.Read().Text("Path to input data set");
```

### 3. Register a UDF

A UDF is a *user-defined function,* which we can use in Spark applications to process our data.

```CSharp
spark.Udf().Register<string, string, bool>("GeneralReg", (log, type) => RegTest(log, type));
```

In the above example, we are registering a new UDF called *GeneralReg.* It takes in two strings (which we have named
*log* and *type*) and produces a boolean output.

The *RegTest* method will compare each log entry to a [regular expression](https://docs.microsoft.com/en-us/dotnet/standard/base-types/regular-expression-language-quick-reference) (AKA regex), which is a sequence of characters that defines a pattern. We use regular expressions to gain meaningful insights from patterns in our log data, such as whether or not an entry relates to spam:

```CSharp
Regex myRegex = new Regex("\\b(?=spam)\\b");
```

### 4. Use Spark SQL

Spark SQL allows us to make SQL calls on our data. It is common to combine UDFs and Spark SQL so that we can apply a UDF to all 
rows of our DataFrame.

```CSharp
DataFrame generalDf = spark.Sql("SELECT logs.value, GeneralReg(logs.value, 'genfilter') FROM Logs");
```

### 5. Running Your App

Checkout the directions for building and running this app on [Windows](../../../../docs/building/windows-instructions.md) or [Ubuntu](../../../../docs/building/ubuntu-instructions.md).

#### Windows Example:

```CSharp
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local C:\GitHub\spark\src\scala\microsoft-spark-2.4.x\target\microsoft-spark-2.4.x-0.5.0.jar Microsoft.Spark.CSharp.Examples.exe Batch.Logging %SPARK_HOME%\examples\src\main\resources\access_log.txt
```

**Note:** The above command assumes your GitHub projects data is stored in **access_log.txt** and you have moved this file
to the `%SPARK_HOME%\examples\src\main\resources` directory.


## Next Steps

View the [full coding example](../Logging.cs) to see an example of reading and analyzing log data. 

While this example is tailored specifically for the Apache access logs, you can try modifying the regular expressions and reading in the Apache error log files to gain further practice with log processing in .NET for Apache Spark.
