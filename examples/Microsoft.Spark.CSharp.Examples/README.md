# .NET for Apache Spark Samples

[.NET for Apache Spark](https://dot.net/spark) is a free, open-source, and cross-platform big data analytics framework.

In the **Microsoft.Spark.CSharp.Examples** folder, we provide samples which will help you get started with .NET for Apache Spark
and demonstrate how to infuse big data analytics into existing and new .NET apps. 

There are three main types of samples/apps in the repo:

* **Batch:** .NET for Apache Spark apps that analyze batch data, or data that has already been produced/stored.

* **Streaming:** .NET for Apache Spark apps that analyze structured streaming data, or data that is currently being produced live.

* **Machine Learning:** .NET for Apache Spark apps infused with Machine Learning models based on [ML.NET](http://dot.net/ml),
an open source and cross-platform machine learning framework.

**Note:** Each of these folders falls under the **Sql** folder since each app utilizes Spark SQL, a Spark module for structured data processing.

<table >
  <tr>
    <td align="middle" colspan="2"><b>Batch Processing</td>
  </tr>
  <tr>
  <td align="middle"><a href="Sql/Batch/Basic.cs"><b>Basic.cs</a></b><br>A simple example demonstrating basic Spark SQL features.<br></td>
  <td align="middle"><a href="Sql/Batch/Datasource.cs"><b>Datasource.cs</a></b><br>Example demonstrating reading from various data sources.<br></td>
  </tr>
  <tr>
    <td align="middle"><a href="Sql/Batch/GitHubProjects.cs"><b>GitHubProjects.cs</a></b><br>Example analyzing GitHub projects data.<br></td>
    <td align="middle"><a href="Sql/Batch/Logging.cs"><b>Logging.cs</a></b><br>Example demonstrating log processing.<br></td>
  </tr>
  <tr>
    <td align="middle"><a href="Sql/Batch/VectorUdfs.cs"><b>VectorUdfs.cs</a></b><br>Example using vectorized UDFs to improve query performance.<br></td>
  </tr>
</table>

<br>

<table >
  <tr>
    <td align="middle" colspan="2"><b>Structured Streaming</td>
  </tr>
  <tr>
    <td align="middle"><a href="Sql/Streaming/StructuredNetworkWordCount.cs"><b>StructuredNetworkWordCount.cs</a></b><br>Simple word count app that connects to and analyzes a live data stream (like netcat).<br></td>
    <td align="middle"><a href="Sql/Streaming/StructuredNetworkWordCountWindowed.cs"><b>StructuredNetworkWordCountWindowed.cs</a></b><br>Windowed word count app.<br></td>
  </tr>
  <tr>
    <td align="middle"><a href="Sql/Streaming/StructuredKafkaWordCount.cs"><b>StructuredKafkaWordCount.cs</a></b><br>Word count on data from Kafka.<br></td>
  </tr>
</table>

<br>

<table >
  <tr>
    <td align="middle" colspan="2"><b>Machine Learning</td>
  </tr>
  <tr>
    <td align="middle"><a href="Sql/MachineLearning/SentimentAnalysis.cs"><b>SentimentAnalysis.cs</a></b><br>Determine if a batch of online reviews are positive or negative, using ML.NET.<br></td>
    <td align="middle"><a href="Sql/MachineLearning/SentimentAnalysisStream.cs"><b>SentimentAnalysisStream.cs</a></b><br>Determine if statements being produced live are positive or negative, using ML.NET.<br></td>
  </tr>
</table>
