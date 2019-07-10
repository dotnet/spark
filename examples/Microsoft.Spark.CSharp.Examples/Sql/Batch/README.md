# Log Processing with Batch Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
to analyze a log file. In the world of big data, this is known as **log processing**.

## Problem

Our goal here is to determine if strings in a file are complete log entries and to then analyze their contents. We will be analyzing the 
[Apache Unix Log Samples](http://www.monitorware.com/en/logsamples/apache.php). The zip file contains both access logs and error logs, 
and this sample is tailored for the access logs.

This sample is an example of **batch processing**, as we are analyzing data that has already been stored and is not actively growing 
or changing.

## Solution

### 1. Create a Spark Session

In any Spark application, we must establish a new SparkSession, which is the entry point to programming Spark with the Dataset and 
DataFrame API.

```CSharp
SparkSession spark = SparkSession
                .Builder()
                .AppName("Apache User Log Processing")
                .GetOrCreate();
```

By calling on the *spark* object created above, we can access Spark and DataFrame functionality throughout our program.

### 2. Read Input File into a DataFrame

We next read in our input file. We store it in a DataFrame object, while is a distributed collection of data organized into 
named columns.

```CSharp
DataFrame df = spark.Read().Text("Path to input data set");
```

### 3. Register a UDF

A UDF is a *user-defined function.* We use UDFs in Spark applications to perform calculations or analysis on our data.

```CSharp
spark.Udf().Register<string, string, bool>("GeneralReg", (log, type) => RegTest(log, type));
```

In the above example, we are registering a new UDF called *GeneralReg.* It takes in two strings (which we have named
*log* and *type,* and produces a boolean output.

A [regular expression](https://docs.microsoft.com/en-us/dotnet/standard/base-types/regular-expression-language-quick-reference)
(commonly known as a "regex") is a sequence of characters that defines a pattern. We use regular expressions in our UDFs in
log processing to gain meaningful insights and patterns from our log data, such as whether or not an entry relates to spam.

```CSharp
Regex myRegex = new Regex("\\b(?=spam)\\b");
```

### 4. Use Spark SQL

Spark SQL allows us to make SQL calls on our data. It is common to combine UDFs and Spark SQL so that we can apply a UDF to all 
rows of our DataFrame.

```CSharp
DataFrame generalDf = spark.Sql("SELECT logs.value, GeneralReg(logs.value, 'genfilter') FROM Logs");
```
## Next Steps
View the full coding example to see an example of reading and analyzing log data. While this example is tailored specifically for the
Apache access logs, try modifying the regular expressions and reading in the Apache error log files to gain further practice with
log processing in .NET for Apache Spark.
