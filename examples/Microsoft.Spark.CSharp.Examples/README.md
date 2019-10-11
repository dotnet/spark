# .NET for Apache Spark C# Samples

[.NET for Apache Spark](https://dot.net/spark) is a free, open-source, and cross-platform big data analytics framework.

In the **Microsoft.Spark.CSharp.Examples** folder, we provide C# samples which will help you get started with .NET for Apache Spark
and demonstrate how to infuse big data analytics into existing and new .NET apps. 

There are three main types of samples/apps in the repo:

* **[Batch](Batch):** .NET for Apache Spark apps that analyze batch data, or data that has already been produced/stored.

* **[Streaming](Streaming):** .NET for Apache Spark apps that analyze structured streaming data, or data that is currently being produced live.

* **[Machine Learning](MachineLearning):** .NET for Apache Spark apps infused with Machine Learning models based on [ML.NET](http://dot.net/ml),
an open source and cross-platform machine learning framework.

<table >
  <tr>
    <td align="middle" colspan="2"><b>Batch Processing</td>
  </tr>
  <tr>
  <td align="middle"><a href="Batch/Basic.cs"><b>Basic.cs</a></b><br>A simple example demonstrating basic Spark SQL features.<br></td>
  <td align="middle"><a href="Batch/Datasource.cs"><b>Datasource.cs</a></b><br>Example demonstrating reading from various data sources.<br></td>
  </tr>
  <tr>
    <td align="middle"><a href="Batch/GitHubProjects.cs"><b>GitHubProjects.cs</a></b><br>Example analyzing GitHub projects data.<br></td>
    <td align="middle"><a href="Batch/Logging.cs"><b>Logging.cs</a></b><br>Example demonstrating log processing.<br></td>
  </tr>
  <tr>
    <td align="middle"><a href="Batch/VectorUdfs.cs"><b>VectorUdfs.cs</a></b><br>Example using vectorized UDFs to improve query performance.<br></td>
  </tr>
</table>

<br>

<table >
  <tr>
    <td align="middle" colspan="2"><b>Structured Streaming</td>
  </tr>
  <tr>
    <td align="middle"><a href="Streaming/StructuredNetworkWordCount.cs"><b>StructuredNetworkWordCount.cs</a></b><br>Simple word count app that connects to and analyzes a live data stream (like netcat).<br></td>
    <td align="middle"><a href="Streaming/StructuredNetworkWordCountWindowed.cs"><b>StructuredNetworkWordCountWindowed.cs</a></b><br>Windowed word count app.<br></td>
  </tr>
  <tr>
    <td align="middle"><a href="Streaming/StructuredKafkaWordCount.cs"><b>StructuredKafkaWordCount.cs</a></b><br>Word count on data from Kafka.<br></td>
  </tr>
</table>

<br>

<table >
  <tr>
    <td align="middle" colspan="2"><b>Machine Learning</td>
  </tr>
  <tr>
    <td align="middle"><a href="MachineLearning/SentimentAnalysis.cs"><b>SentimentAnalysis.cs</a></b><br>Determine if a batch of online reviews are positive or negative, using ML.NET.<br></td>
    <td align="middle"><a href="MachineLearning/SentimentAnalysisStream.cs"><b>SentimentAnalysisStream.cs</a></b><br>Determine if statements being produced live are positive or negative, using ML.NET.<br></td>
  </tr>
</table>

### Other Files in the Folder

Beyond the sample apps, there are a few other files in the **Microsoft.Spark.CSharp.Examples** folder:

* **IExample.cs:** A common interface each sample implements to help provide consistency when creating/running sample apps.

* **Microsoft.Spark.CSharp.Examples.csproj:** The C# project file necessary for building/running all sample apps. It includes target
frameworks, assembly information, and references to other C# project files references in the sample apps.

* **Program.cs:** A common entry-point when running our sample apps (it contains the Main method). Helps us print error messages in cases such as a project lacking the necessary arguments.

* **README.md:** The doc you are currently reading.
