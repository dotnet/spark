# .NET for Apache Spark C# Samples: Streaming

[.NET for Apache Spark](https://dot.net/spark) is a free, open-source, and cross-platform big data analytics framework.

In the **Streaming** folder, we provide C# samples which will help you get started with the big data analytics scenario known as
**structured streaming.** Stream processing means we're analyzing data live as it's being produced.

## Problem

In StructuredLogProcessing.cs specifically, the goal is to determine if incoming content is a valid log entry. We'll be using the access log format found in the [Apache Unix Log Samples](http://www.monitorware.com/en/logsamples/apache.php). 

This sample is an example of **stream processing** since we're processing data in real time, and **log processing** as it
involves gaining insights from a file of log entries.

## Solution

### 1. Create a Spark Session

In any Spark application, we need to establish a new SparkSession, which is the entry point to programming Spark with the Dataset and 
DataFrame API.

```CSharp
SparkSession spark = SparkSession
       .Builder()
       .AppName("StructuredLogProcessing")
       .GetOrCreate();
```

By calling on the *spark* object created above, we can access Spark and DataFrame functionality throughout our program.

### 2. Establish and Connect to Data Stream

#### Establish Stream: Netcat

netcat (also known as *nc*) allows you to read from and write to network connections. We'll establish a network
connection with netcat through a terminal window.

[Download netcat](https://sourceforge.net/projects/nc110/files/), extract the file from the zip download, and append the 
directory you extracted to your "PATH" environment variable.

To start a new connection, open a command prompt. For Linux users, run ```nc -lk 9999``` to connect to localhost on port 9999.

Windows users can run ```nc -vvv -l -p 9999``` to connect to localhost port 9999. The result should look something like this:

![NetcatConnect](https://github.com/bamurtaugh/spark/blob/StreamingLog/examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/netconnect.PNG)

Our Spark program will be listening for input we type into this command prompt.

#### Connect to Stream: ReadStream()

The ```ReadStream()``` method returns a DataStreamReader that can be used to read streaming data in as a DataFrame. We'll include the host and port information so that our Spark app knows where to expect its streaming data.

```CSharp
DataFrame words = spark
      .ReadStream()
      .Format("socket")
      .Option("host", hostname)
      .Option("port", port)
      .Load();
```

### 3. Register a UDF

A UDF is a *user-defined function.* We can use UDFs in Spark applications to perform calculations and analysis on our data.

```CSharp
spark.Udf().Register<string, bool>("MyUDF", input => ValidLogTest(input));
```

In the above code snippet, we register a UDF that will pass each log entry it receives to the method *ValidLogTest.*

ValidLogTest compares the log entry to a [regular expression](https://docs.microsoft.com/en-us/dotnet/standard/base-types/regular-expression-language-quick-reference) (commonly known as a "regex"). We use regular expressions in our UDFs in log processing to gain meaningful insights and patterns from our log data. 

### 4. Use Spark SQL

Next, we'll use Spark SQL to make SQL calls on our data. It's common to combine UDFs and Spark SQL so that we can apply a UDF to each 
row of our DataFrame.

```CSharp
DataFrame sqlDf = spark.Sql("SELECT WordsEdit.value, MyUDF(WordsEdit.value) FROM WordsEdit"); 
```

### 5. Display Your Stream

We can use ```DataFrame.WriteStream()``` to establish characteristics of our output, such as printing our results to the console and only displaying the most recent output and not all of our previous output as well. 

```CSharp
var query = sqlDf
      .WriteStream()
      .Format("console")
      .Start();
```

### 6. Running Your Code

Structured streaming in Spark processes data through a series of small **batches**. 
When you run your program, the command prompt where we established the netcat will allow you to start typing.
In our example, when you hit *enter* after entering data in the command prompt, Spark will consider that a batch and run the UDF. 

![StreamingOutput](https://github.com/bamurtaugh/spark/blob/StreamingLog/examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/streamingnc.PNG)
