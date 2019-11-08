# .NET for Apache Spark C# Samples: Streaming

[.NET for Apache Spark](https://dot.net/spark) is a free, open-source, and cross-platform big data analytics framework.

In the **Streaming** folder, we provide C# samples which will help you get started with the big data analytics scenario known as
**structured streaming.** Stream processing means we're analyzing data live as it's being produced.

## Problem

These samples are examples of **stream processing** since we're processing data in real time.

## Solution

You'll see there are four different samples included in the *Streaming* folder:
* **[StructuredNetworkWordCount.cs](StructuredNetworkWordCount.cs)** - word count on data streamed from any source (i.e. netcat)
* **[StructuredNetworkWordCountWindowed.cs](StructuredNetworkWordCountWindowed.cs)** - word count on data with windowing logic
* **[StructuredKafkaWordCount.cs](StructuredKafkaWordCount.cs)** - word count on data streamed from Kafka
* **[StructuredNetworkCharacterCount.cs](StructuredNetworkCharacterCount.cs)** - sample counting number of characters in each string read from a stream; demonstrating power of UDFs + stream processing

While the steps below apply to most stream processing apps, some of the specific code snippets or submission instructions pertain specifically to the [StructuredNetworkCharacterCount.cs](StructuredNetworkCharacterCount.cs) example.

### 1. Create a Spark Session

In any Spark application, we need to establish a new `SparkSession`, which is the entry point to programming Spark with the Dataset and DataFrame API.

```CSharp
SparkSession spark = SparkSession
    .Builder()
    .AppName("Streaming example with a UDF")
    .GetOrCreate();
```

By calling on the *spark* object created above, we can access Spark and DataFrame functionality throughout our program.

### 2. Establish and Connect to Data Stream

Depending upon which sample you choose to run, you need to setup a connection to a data stream. One popular way to test out stream processing is through **netcat.**

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

The `ReadStream()` method returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`. We'll include the host and port information so that our Spark app knows where to expect its streaming data.

```CSharp
DataFrame lines = spark
    .ReadStream()
    .Format("socket")
    .Option("host", hostname)
    .Option("port", port)
    .Load();
```

### 3. Register a UDF

A UDF is a *user-defined function.* We can use UDFs in Spark applications to perform calculations and analysis on our data.

```CSharp
Func<Column, Column> udfArray =
    Udf<string, string[]>((str) => new string[] { str, $"{str} {str.Length}");
```

In the above code snippet from [StructuredNetworkCharacterCount.cs](StructuredNetworkCharacterCount.cs), we register a UDF called `udfArray`. This UDF will process each string it receives from the netcat terminal to produce an array that includes: the original string (contained in *str*), the original string concatenated with the length of that original string. 

For example, entering *Hello world* in the terminal would produce an array where:
* array[0] = Hello world
* array[1] = Hello world 11
    
This is just an example of how you can use UDFs to further modify and analyze your data, even live as it's being streamed in!

### 4. Use SparkSQL

Next, we'll use SparkSQL to perform various functions on the data stored in our DataFrame. It's common to combine UDFs and SparkSQL so that we can apply a UDF to each row of our DataFrame.

```CSharp
DataFrame arrayDF = lines.Select(Explode(udfArray(lines["value"])));
```

In the above code snippet from [StructuredNetworkCharacterCount.cs](StructuredNetworkCharacterCount.cs), we apply *udfArray* to each value in our DataFrame (which represents each string read in from our netcat terminal). We then apply the SparkSQL method `Explode` to put each entry of our array in its own row. Finally, we use `Select` to place the columns we've produced in the new DataFrame *arrayDF.*

### 5. Display Your Stream

We can use `DataFrame.WriteStream()` to establish characteristics of our output, such as printing our results to the console and only displaying the most recent output and not all of our previous output as well. 

```CSharp
StreamingQuery query = arrayDf
    .WriteStream()
    .Format("console")
    .Start();
```

### 6. Running Your Code

Structured streaming in Spark processes data through a series of small **batches.** 
When you run your program, the command prompt where we established the netcat will allow you to start typing.
In our example, when you hit *enter* after entering data in the command prompt, Spark will consider that a batch and run the UDF. 

![StreamingOutput](https://github.com/bamurtaugh/spark/blob/StreamingLog/examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/stream2.png)

Check out the directions for building and running this app on [Windows](../../../../../../docs/building/windows-instructions.md) or [Ubuntu](../../../../../../docs/building/ubuntu-instructions.md).

#### Windows Example:

After starting a new netcat session, open a new terminal and run your `spark-submit` command, similar to the following:

```powershell
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local /path/to/microsoft-spark-<version>.jar Microsoft.Spark.CSharp.Examples.exe Sql.Streaming.StructuredNetworkCharacterCount localhost 9999
```

> **Note:** Be sure to update the above command with the actual path to your Microsoft Spark jar file. The above command also assumes your netcat server is running on localhost port 9999.

## Additional Resources

To learn more about structured streaming with .NET for Apache Spark, check out [this video](https://channel9.msdn.com/Series/NET-for-Apache-Spark-101/Structured-Streaming-with-NET-for-Apache-Spark) from the .NET for Apache Spark 101 video series to see a streaming demo coded and ran live.

You can also [check out the demos and explanation](https://youtu.be/ZWsYMQ0Sw1o) from the .NET for Apache Spark session at .NET Conf 2019!
