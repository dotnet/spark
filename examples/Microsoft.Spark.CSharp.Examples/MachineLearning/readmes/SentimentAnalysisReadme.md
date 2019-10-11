# Sentiment Analysis with Big Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
and [ML.NET](https://dotnet.microsoft.com/apps/machinelearning-ai/ml-dotnet) to determine if 
statements are positive or negative, a task known as **sentiment analysis**.

## Problem

Our goal here is to determine if online reviews are positive or negative. We'll be using .NET for Apache Spark to read in a dataset of reviews and ML.NET to perform **binary classification** since categorizing reviews involves choosing one of two groups: positive or negative. You can read more about the problem through the [ML.NET documentation](https://docs.microsoft.com/en-us/dotnet/machine-learning/tutorials/sentiment-analysis).

## Solution

We'll first train an ML model using ML.NET, and then we'll create a new application that uses both .NET for Apache Spark and ML.NET.

## ML.NET

### 1. Download Datasets

We'll be using a set of amazon reviews to train our model and a set of yelp reviews for testing in our Spark app. These can be found in the Datasets folder, and they come from the [UCI Sentiment Labeled Sentences](https://archive.ics.uci.edu/ml/machine-learning-databases/00331/sentiment%20labelled%20sentences.zip). 

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
       .AppName("Apache User Log Processing")
       .GetOrCreate();
```

### 2. Read Input File into a DataFrame

We trained our model with the amazon data, so let's test how well the model performs by testing it with the yelp dataset. 

```CSharp
DataFrame df = spark.Read().Csv(<Path to yelp data set>);
```

### 3. Use UDF to Access ML.NET

We create a User Defined Function (UDF) that calls the *Sentiment* method on each yelp review.

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

### 4. Spark SQL and Running Your Code

Now that you've read in your data and incorporated ML, use Spark SQL to call the UDF that will run sentiment analysis on each row of your DataFrame:

```CSharp
DataFrame sqlDf = spark.Sql("SELECT _c0, MLudf(_c0) FROM Reviews");
```

Run your code, and you'll be performing sentiment analysis with ML.NET and Spark.NET!

## Alternative Approach: Real-Time Sentiment Analysis

Rather than performing batch processing (analyzing data that's already been stored), we can adapt our Spark + ML.NET app to instead perform real-time processing with structured streaming.

Checkout [SentimentAnalysisStream.cs](../SentimentAnalysisStream.cs) to see the adapted version of the sentiment analysis program that will determine the sentiment of text live as it's typed into a terminal.
