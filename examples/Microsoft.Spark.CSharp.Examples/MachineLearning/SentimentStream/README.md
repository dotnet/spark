# Streaming Sentiment Analysis with Big Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
and [ML.NET](https://dotnet.microsoft.com/apps/machinelearning-ai/ml-dotnet) to determine if 
statements are positive or negative, a task known as **sentiment analysis**.

## Problem

Our goal here is to determine if statements typed into a console are positive or negative. We'll be using .NET for Apache Spark to process statements in real-time and ML.NET to perform **binary classification** since categorizing text involves choosing one of two groups: positive or negative. You can read more about the problem through the [ML.NET documentation](https://docs.microsoft.com/en-us/dotnet/machine-learning/tutorials/sentiment-analysis).

## Dataset

We'll be using a set of **Yelp reviews** as the input training data for this example. 

For the specific ML training/predictions in this app (i.e. when using Model Builder), it helps to have a header for the data. We introduced the column header **ReviewText,** which holds the review itself, and **Sentiment,** which holds either a 0 to indicate negative sentiment or a 1 to indicate positive sentiment. You can view the resulting data set in [yelptrain.csv](../Sentiment/Resources/yelptrain.csv).

You can [download the original Yelp data](https://archive.ics.uci.edu/ml/machine-learning-databases/00331/sentiment%20labelled%20sentences.zip) from the [UCI Sentiment Labeled Sentences Dataset]( https://archive.ics.uci.edu/ml/datasets/Sentiment+Labelled+Sentences). 

## Solution

We'll first train an ML model using ML.NET, and then we'll create a new application that uses both .NET for Apache Spark and ML.NET.

> **Note:** All of the necessary files (trained ML model, .NET for Spark + ML.NET application code, training dataset) have been included in this project. You can follow the steps below to understand how the project/files were developed, recreate them yourself, and then adapt the steps to future applications.

## ML.NET

[ML.NET](dot.net/ml) is an open source and cross-platform machine learning framework that allows .NET developers to easily integrate ML into .NET apps without any prior ML experience. 

### 1. Download Model Builder

We'll use ML.NET to build and train a model through [Model Builder](https://dotnet.microsoft.com/apps/machinelearning-ai/ml-dotnet/model-builder), a Visual Studio extension that provides an easy to understand visual interface to build, train, and deploy machine learning models. Model Builder can be downloaded [here](https://marketplace.visualstudio.com/items?itemName=MLNET.07). 

![Model Builder](https://mlnet.gallerycdn.vsassets.io/extensions/mlnet/07/16.0.1909.2101/1569301315962/add-machine-learning.gif)

> **Note:** You can also develop a model without Model Builder. Model Builder just provides an easier way to develop a model that doesn't require prior ML experience. 

### 2. Build and Train Your Model

Follow the [Model Builder Getting Started Guide](https://dotnet.microsoft.com/learn/machinelearning-ai/ml-dotnet-get-started-tutorial/intro) to train your model using the sentiment analysis scenario. 

Follow the steps to:

* Create a new C# Console App
* Pick the **Sentiment Analysis** scenario
* Train using the **yelptrain.csv** dataset

![Sentiment Analysis Model Builder](https://dotnet.microsoft.com/static/images/model-builder-vs.png?v=9On8qwmGIXdAyX_-zAmATwYU7fd7tzem-_ojnv1G7XI)

### 3. Generate Model and Code

In the last step of using Model Builder, you'll produce a zip file containing the trained ML.NET model. In the image below, you can see it's contained in **MLModel.zip.**

![ML.NET Zip and Files](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/modelbuilder5proj.PNG)

You'll also generate C# code you can use to consume your model in other .NET apps, like the Spark app we'll be creating.

![Generated Code](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/modelbuilder5code.PNG)

### 4. Add ML.NET to .NET for Apache Spark App

You have a few options to start creating a .NET for Apache Spark app that uses this ML.NET code and trained model. Make sure that in any app you develop, you've added the [Microsoft.ML NuGet Package](https://www.nuget.org/packages/Microsoft.ML).

Depending upon the algorithm Model Builder chooses for your ML model, you may need to add an additional nuget reference in [Microsoft.Spark.CSharp.Examples.csproj](../../Microsoft.Spark.CSharp.Examples.csproj). For instance, if you get an error message that Microsoft.ML.FastTree cannot be found when running your Spark app, you need to add that nuget to your csproj file:

`<PackageReference Include="Microsoft.ML.FastTree" Version="1.3.1" />`

#### Option 1: Add Projects

One option is to use Model Builder's *Add Projects* feature, which will result in 3 projects:

* Your original app (**myMLApp**)
* A console app that allows you to build/train/test the model (**myMLAppML.ConsoleApp**)
* A .NET Standard class library that contains model input/output and your trained model in a zip file (**myMLAppML.Model**)

You would begin writing your .NET for Apache Spark code (and paste in the code generated from Model Builder, shown in step 3 above) in the original app **myMLApp.**

![Model Builder Result](https://dotnet.microsoft.com/static/images/model-builder-generated-code.png?v=iC-r8k3zpKUwQVoNOH34D903IhXhIb4CsX003484s7c)

#### Option 2: Create a new console app (shown in this repo)

Rather than working with the projects/files produced by Model Builder's Add Projects, you can create a new, separate C# console app. You just need to copy over your model's zip file to a directory your new console app can access. In this repo, a trained model **MLModel.zip** has [already been included for you](../Sentiment/Resources).

As we create the logic for our Spark app, we'll paste in the code generated from Model Builder and include some other class definitions.

## .NET for Spark

Now that we've trained an ML.NET model for sentiment analysis, we can begin writing the .NET for Spark code that will read in a stream of data typed into a console, pass each piece of text from the stream to our ML.NET model, and predict whether statements are positive or negative.

### 1. Create a Spark Session

In any Spark application, we need to establish a new SparkSession, which is the entry point to programming Spark with the Dataset and 
DataFrame API.

```CSharp
SparkSession spark = SparkSession
    .Builder()
    .AppName("Streaming Sentiment Analysis")
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

The `ReadStream()` method returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`. We'll include the host and port information so that our Spark app knows where to expect its streaming data.

```CSharp
DataFrame words = spark
    .ReadStream()
    .Format("socket")
    .Option("host", hostname)
    .Option("port", port)
    .Load();
```

### 3. Register a UDF to Access ML.NET

A UDF is a *user-defined function.* We can use UDFs in Spark applications to perform calculations and analysis on our data. We create a User Defined Function (UDF) that calls the *Sentiment* method on each piece of text from our stream.

```CSharp
spark.Udf().Register<string, bool>("MLudf", input => Sentiment(input));
```

*Sentiment* is the method where we'll call our ML.NET code that was generated from the final step of Model Builder.

```CSharp
MLContext mlContext = new MLContext();

ITransformer mlModel = mlContext
    .Model
    .Load(modelPath, out var modelInputSchema);

PredictionEngine<Review, ReviewPrediction> predEngine = mlContext
    .Model
    .CreatePredictionEngine<Review, ReviewPrediction>(mlModel);
```

You may notice the use of *Review* and *ReviewPrediction.* These are classes we define in our project to represent the review data we're evaluating. 

```CSharp
public class Review
{
      // Represents the streamed input text
      [LoadColumn(0)]
      public string ReviewText;
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
DataFrame sqlDf = spark.Sql("SELECT WordsSentiment.value, MLudf(WordsSentiment.value) FROM WordsSentiment");
```

### 5. Display Your Stream

We can use `DataFrame.WriteStream()` to establish characteristics of our output, such as printing our results to the console and only displaying the most recent output and not all of our previous output as well. 

```CSharp
StreamingQuery query = sqlDf
    .WriteStream()
    .Format("console")
    .Start();
```

## Running Your App

There are a few steps you'll need to follow to build and run your app:

* Start a new netcat server
* Open a new terminal (separate from the netcat server), move to your app's root folder (i.e. *SentimentStream*)
* Clean and publish your app
* Move to your app's `publish` folder
* `spark-submit` your app from within the `publish` folder

Structured streaming in Spark processes data through a series of small **batches.** 
When you run your program, the command prompt where we established netcat will allow you to start typing.
In our example, when you hit *enter* after entering data in the command prompt, Spark will consider that a batch and run the UDF. 

#### Windows Example:

After starting a new netcat connection in one command prompt, open a new one and run a command similar to the following: 

```powershell
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local /path/to/microsoft-spark-<version>.jar Microsoft.Spark.CSharp.Examples.exe MachineLearning.SentimentStream.Program localhost 9999 /path/to/MLModel.zip
```

> **Note:** Be sure to update the above command with the actual streaming host/port information and paths to your Microsoft Spark jar file and MLModel.zip.

## Next Steps

Check out the [full coding example](./Program.cs). You can also view a live video explanation of ML.NET + .NET for Spark in the [Bringing Big Data Analytics through Apache Spark to .NET](https://youtu.be/ZWsYMQ0Sw1o?t=1358) session from **.NET Conf 2019.**

Rather than performing real-time processing, we can adapt our Spark + ML.NET app to instead perform batch processing (analyzing data that's already been stored).

Check out [Sentiment](../Sentiment) to see the adapted version of the sentiment analysis program that will determine the sentiment of text from a batch of online reviews.

## Citations

**UCI Machine Learning Repository citation:** Dua, D. and Graff, C. (2019). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml]. Irvine, CA: University of California, School of Information and Computer Science.

**Sentiment Labelled Sentences Data Set citation:** 'From Group to Individual Labels using Deep Features', Kotzias et. al,. KDD 2015
