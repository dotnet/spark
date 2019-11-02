# Log Processing with Batch Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
to analyze a log file. In the world of big data, this is known as **log processing**.

## Problem

Our goal here is to determine if strings in a file are complete log entries and to then analyze their contents.

This sample is an example of **batch processing** since we're analyzing data that has already been stored and is not actively growing 
or changing.

## Dataset

We'll be analyzing a set of
[Apache log entries](https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs). Apache log files give us information about how users are interacting with various content on a server. Logs are divided into two categories: access logs and error logs, and this coding example is tailored for access log files.

You can read more about Apache log files [here](https://httpd.apache.org/docs/1.3/logs.html).

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

Now that we have an entry point to the Spark API, let's read in our log file. We'll store it in a `DataFrame`, which is a distributed collection of data organized into named columns.

```CSharp
DataFrame df = spark.Read().Text("Path to input data set");
```

### 3. Register a UDF

A UDF is a *user-defined function,* which we can use in Spark applications to process our data.

```CSharp
spark.Udf().Register<string, bool>("GeneralReg", log => Regex.IsMatch(log, s_apacheRx));
```

In the above example, we are registering a new UDF called *GeneralReg.* It takes in a string (which we have named
*log*) and produces a boolean output.

This UDF will compare each log entry to a [regular expression](https://docs.microsoft.com/en-us/dotnet/standard/base-types/regular-expression-language-quick-reference) (AKA regex), which is a sequence of characters that defines a pattern. 

We use regular expressions to gain meaningful insights from patterns in our log data. In the above UDF, our regex *s_apacheRX* defines the pattern all valid log entries should follow:

```CSharp
static readonly string s_apacheRx =
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
```

### 4. Use Spark SQL

Spark SQL allows us to make SQL calls on our data. It is common to combine UDFs and Spark SQL so that we can apply a UDF to all 
rows of our DataFrame.

```CSharp
DataFrame generalDf = spark.Sql("SELECT logs.value, GeneralReg(logs.value) FROM Logs");
```

In the above example, we use Spark SQL to call our *GeneralReg* UDF. This allows us to apply that function to each row of our DataFrame, which means each log entry will be tested in *GeneralReg* to determine if they're valid log entries.

We can continue on in our app to create additional UDFs and call them through additional SQL commands to continue filtering and determining patterns in our log data.

### 5. Running Your App

Checkout the directions for building and running this app on [Windows](../../../../../docs/building/windows-instructions.md) or [Ubuntu](../../../../../docs/building/ubuntu-instructions.md).

#### Windows Example:

```powershell
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local /path/to/microsoft-spark-<version>.jar Microsoft.Spark.CSharp.Examples.exe Sql.Batch.Logging /path/to/access_log.txt
```

> **Note:** Be sure to update the above command with the actual paths to your Microsoft Spark jar file and **access_log.txt**.

## Next Steps

View the [full coding example](../Logging.cs) to see an example of reading and analyzing log data.
