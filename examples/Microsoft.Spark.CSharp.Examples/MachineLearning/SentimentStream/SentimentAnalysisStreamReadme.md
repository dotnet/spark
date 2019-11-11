# Sentiment Analysis with Big Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
and [ML.NET](https://dotnet.microsoft.com/apps/machinelearning-ai/ml-dotnet) to determine if 
statements are positive or negative, a task known as **sentiment analysis**.

## Problem

Our goal here is to determine if statements typed into a console are positive or negative. We'll be using .NET for Apache Spark to process statements in real-time and ML.NET to perform **binary classification** since categorizing text involves choosing one of two groups: positive or negative. You can read more about the problem through the [ML.NET documentation](https://docs.microsoft.com/en-us/dotnet/machine-learning/tutorials/sentiment-analysis).

## Dataset

We'll be using a set of **Yelp reviews** as the input training data for this example. 

For the specific ML training/predictions in this app (i.e. when using Model Builder), it helps to have a header for the data. We introduced the column header **ReviewText,** which holds the review itself, and **Sentiment,** which holds either a 0 to indicate negative sentiment or a 1 to indicate positive sentiment. You can view the resulting data set in [yelptrain.csv](./Resources/yelptrain.csv).

You can [download the original Yelp data](https://archive.ics.uci.edu/ml/machine-learning-databases/00331/sentiment%20labelled%20sentences.zip) from the [UCI Sentiment Labeled Sentences Dataset]( https://archive.ics.uci.edu/ml/datasets/Sentiment+Labelled+Sentences). 

## Solution

We'll first train an ML model using ML.NET, and then we'll create a new application that uses both .NET for Apache Spark and ML.NET.

## ML.NET

### 1. Download Dataset

We'll be using a set of amazon reviews to train our model. It comes from the [UCI Sentiment Labeled Sentences](https://archive.ics.uci.edu/ml/machine-learning-databases/00331/sentiment%20labelled%20sentences.zip). 

### 2. Build and Train Your Model

Use ML.NET to build and train a model. You can use Model Builder to easily train and use ML models in Visual Studio. Follow the [Model Builder Getting Started Guide](https://dotnet.microsoft.com/learn/machinelearning-ai/ml-dotnet-get-started-tutorial/intro) to train your model using the sentiment analysis scenario.

In the last step, you'll produce a zip file containing the trained ML.NET model.

![ML.NET Zip and Files](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/modelbuilder5proj.PNG)

You'll also generate C# code you can use to consume your model in other .NET apps, like the Spark app we'll be creating.

![Generated Code](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/modelbuilder5code.PNG)

### 3. Add ML.NET to .NET for Apache Spark App

Make sure you've downloaded the [Microsoft.ML NuGet Package](https://www.nuget.org/packages/Microsoft.ML). Either use Model Builder's "Add Projects" feature, or in a new C# console app, add a reference the Microsoft.ML API and the .csproj file of your trained ML.NET model.

![CSProject](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/SparkMLPic.PNG)

As we create the logic for our Spark app, we'll paste in the code generated from Model Builder and include some other class definitions.

## Spark.NET

### 1. Create a Spark Session

In any Spark application, we need to establish a new SparkSession, which is the entry point to programming Spark with the Dataset and 
DataFrame API.

```CSharp
SparkSession spark = SparkSession
       .Builder()
       .AppName("Sentiment Analysis Streaming")
       .GetOrCreate();
```

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
### 3. Use UDF to Access ML.NET

A UDF is a *user-defined function.* We can use UDFs in Spark applications to perform calculations and analysis on our data. We create a User Defined Function (UDF) that calls the *Sentiment* method on each yelp review.

```CSharp
spark.Udf().Register<string, bool>("MLudf", (text) => Sentiment(text));
```

The Sentiment method is where we'll call our ML.NET code that was generated from the final step of Model Builder.

```CSharp
MLContext mlContext = new MLContext();
ITransformer mlModel = mlContext.Model.Load("MLModel.zip", out var modelInputSchema);
var predEngine = mlContext.Model.CreatePredictionEngine<Review, ReviewPrediction>(mlModel);
```
You may notice the use of *Review* and *ReviewPrediction.* These are classes we define in our project to represent the review data we're evaluating. 

```CSharp
public class Review
{
      [LoadColumn(0)]
      public string Column1;

      [LoadColumn(1), ColumnName("Column2")]
      public bool Column2;
}
```

```CSharp
public class ReviewPrediction : Review
{

       [ColumnName("PredictedLabel")]
       public bool Prediction { get; set; }

       public float Probability { get; set; }

       public float Score { get; set; }
} 
```

### 4. Spark SQL

Now that you've read in your data and incorporated ML, use Spark SQL to call the UDF that will run sentiment analysis on each row of your DataFrame:

```CSharp
DataFrame sqlDf = spark.Sql("SELECT _c0, MLudf(_c0) FROM Reviews");
```

### 5. Display Your Stream

We can use ```DataFrame.WriteStream()``` to establish characteristics of our output, such as printing our results to the console and only displaying the most recent output and not all of our previous output as well. 

```CSharp
var query = sqlDf
      .WriteStream()
      .Format("console")
      .Start();
```

## How to Run

Checkout the [full coding example](../SentimentAnalysisStream.cs).

Since there are several distinct steps of setup for building and running a .NET for Apache Spark + ML.NET app, it's recommended
to create a new console app and complete the Model Builder and ML.NET reference steps (from above) in that app. 

You can then add the code from the SentimentAnalysis.cs app to your console app. You can finally `spark-submit` your new console app.

In a separate terminal, you'll need to establish a netcat terminal (as described earlier in this doc).

Structured streaming in Spark processes data through a series of small **batches**. When you run your program, the command prompt where we established the netcat will allow you to start typing.

In our example, when you hit *enter* after entering data in the command prompt, Spark will consider that a batch and run the UDF. 

![StreamingOutput](https://github.com/bamurtaugh/spark/blob/StreamingLog/examples/Microsoft.Spark.CSharp.Examples/Sql/Streaming/streamingnc.PNG)

**Note:** In order to `spark-submit` an app that includes an additional Nuget (like the ML.NET nuget), you'll need to copy the ML.NET
dll's into your app's main directory.

## Alternative Approach: Batch Sentiment Analysis

Rather than performing real-time processing, we can adapt our Spark + ML.NET app to instead perform batch processing (analyzing data that's already been stored).

Checkout [SentimentAnalysis.cs](../SentimentAnalysis.cs) to see the adapted version of the sentiment analysis program that will determine the sentiment of text from a set of online reviews.
