# Sentiment Analysis with Big Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
and [ML.NET](https://dotnet.microsoft.com/apps/machinelearning-ai/ml-dotnet) to determine if 
statements are positive or negative, a task known as **sentiment analysis**.

## Problem

Our goal here is to determine if online reviews are positive or negative. We'll be using .NET for Apache Spark to read in a dataset of reviews and ML.NET to perform **binary classification** since categorizing reviews involves choosing one of two groups: positive or negative. You can read more about the problem through the [ML.NET documentation](https://docs.microsoft.com/en-us/dotnet/machine-learning/tutorials/sentiment-analysis).

## Dataset

We'll be using a set of Amazon reviews to train our model and a set of Yelp reviews for testing in our Spark + ML app. You can [download the original data](https://archive.ics.uci.edu/ml/machine-learning-databases/00331/sentiment%20labelled%20sentences.zip) from the [UCI Sentiment Labeled Sentences Dataset]( https://archive.ics.uci.edu/ml/datasets/Sentiment+Labelled+Sentences).

For the specific ML training/predictions in this app, it helps to have a header for the data. Versions of the Amazon and Yelp datasets with headers can be found in the [Resources](./Resources) folder.

## Solution

We'll first train an ML model using ML.NET, and then we'll create a new application that uses both .NET for Apache Spark and ML.NET.

## ML.NET

[ML.NET](dot.net/ml) is an open source and cross-platform machine learning framework that allows .NET developers to easily integrate ML into .NET apps without any prior ML experience. 

### 1. Download Model Builder

We'll use ML.NET to build and train a model through [Model Builder](https://dotnet.microsoft.com/apps/machinelearning-ai/ml-dotnet/model-builder), a Visual Studio extension that provides an easy to understand visual interface to build, train, and deploy machine learning models. Model Builder can be downloaded [here](https://marketplace.visualstudio.com/items?itemName=MLNET.07). 

![Model Builder](https://mlnet.gallerycdn.vsassets.io/extensions/mlnet/07/16.0.1909.2101/1569301315962/add-machine-learning.gif)

**Note:** You can also develop a model without Model Builder. Model Builder just provides an easier way to develop a model that doesn't require prior ML experience. 

### 2. Build and Train Your Model

Follow the [Model Builder Getting Started Guide](https://dotnet.microsoft.com/learn/machinelearning-ai/ml-dotnet-get-started-tutorial/intro) to train your model using the sentiment analysis scenario. 

Follow the steps to:

* Create a new C# Console App
* Pick the **Sentiment Analysis** scenario
* Train using the **Amazon.csv** dataset

![Sentiment Analysis Model Builder](https://dotnet.microsoft.com/static/images/model-builder-vs.png?v=9On8qwmGIXdAyX_-zAmATwYU7fd7tzem-_ojnv1G7XI)

### 3. Generate Model and Code

In the last step of using Model Builder, you'll produce a zip file containing the trained ML.NET model. In the image below, you can see it's contained in **MLModel.zip.**

![ML.NET Zip and Files](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/modelbuilder5proj.PNG)

You'll also generate C# code you can use to consume your model in other .NET apps, like the Spark app we'll be creating.

![Generated Code](https://github.com/bamurtaugh/spark/blob/SparkMLNet/examples/Microsoft.Spark.CSharp.Examples/MachineLearning/images/modelbuilder5code.PNG)

### 4. Add ML.NET to .NET for Apache Spark App

You have a few options to start creating a .NET for Apache Spark app that uses this ML.NET code and trained model. Make sure that in any app you develop, you've downloaded the [Microsoft.ML NuGet Package](https://www.nuget.org/packages/Microsoft.ML)

#### Option 1: Add Projects

One option is to use Model Builder's *Add Projects* feature, which will result in 3 projects:

* Your original app (**myMLApp**)
* A console app that allows you to build/train/test the model (**myMLAppML.ConsoleApp**)
* A .NET Standard class library that contains model input/output and your trained model in a zip file (**myMLAppML.Model**)

![Model Builder Result](https://dotnet.microsoft.com/static/images/model-builder-generated-code.png?v=iC-r8k3zpKUwQVoNOH34D903IhXhIb4CsX003484s7c)

#### Option 2: Create a new console app (shown in this repo)

Rather than working with the projects/files produced by Model Builder's Add Projects, you can create a new, separate C# console app. You just need to copy over your model's zip file to a directory your new console app can access. In this repo, a trained model **MLModel.zip** has already been included for you in the *Resources* folder.

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

Once you run your code, you'll be performing sentiment analysis with ML.NET and Spark.NET!

## How to Run

Checkout the [full coding example](../SentimentAnalysis.cs).

Since there are several distinct steps of setup for building and running a .NET for Apache Spark + ML.NET app, it's recommended
to create a new console app and complete the Model Builder and ML.NET reference steps (from above) in that app. 

You can then add the code from the SentimentAnalysis.cs app to your console app. You can then `spark-submit` your new console app.

**Note:** In order to `spark-submit` an app that includes an additional Nuget (like the ML.NET nuget), you'll need to copy the ML.NET
dll's into your app's main directory.

## Alternative Approach: Real-Time Sentiment Analysis

Rather than performing batch processing (analyzing data that's already been stored), we can adapt our Spark + ML.NET app to instead perform real-time processing with structured streaming.

Checkout SentimentAnalysisStream.cs to see the adapted version of the sentiment analysis program that will determine the sentiment of text live as it's typed into a terminal.
