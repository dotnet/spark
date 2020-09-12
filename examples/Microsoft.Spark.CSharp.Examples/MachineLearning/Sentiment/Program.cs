// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Examples.MachineLearning.Sentiment
{
    /// <summary>
    /// Example of using ML.NET + .NET for Apache Spark
    /// for sentiment analysis.
    /// </summary>
    internal sealed class Program : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine(
                    "Usage: <path to yelptest.csv> <path to MLModel.zip>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName("Sentiment Analysis using .NET for Apache Spark")
                .GetOrCreate();

            // Read in and display Yelp reviews
            DataFrame df = spark
                .Read()
                .Option("header", true)
                .Option("inferSchema", true)
                .Csv(args[0]);
            df.Show();

            // Use ML.NET in a UDF to evaluate each review 
            spark.Udf().Register<string, bool>(
                "MLudf",
                (text) => Sentiment(text, args[1]));

            // Use Spark SQL to call ML.NET UDF
            // Display results of sentiment analysis on reviews
            df.CreateOrReplaceTempView("Reviews");
            DataFrame sqlDf = spark.Sql("SELECT ReviewText, MLudf(ReviewText) FROM Reviews");
            sqlDf.Show();

            // Print out first 20 rows of data
            // Prevent data getting cut off by setting truncate = 0
            sqlDf.Show(20, 0, false);

            spark.Stop();
        }

        // Method to call ML.NET code for sentiment analysis
        // Code primarily comes from ML.NET Model Builder
        public static bool Sentiment(string text, string modelPath)
        {
            var mlContext = new MLContext();

            ITransformer mlModel = mlContext
                .Model
                .Load(modelPath, out DataViewSchema _);

            PredictionEngine<Review, ReviewPrediction> predEngine = mlContext
                .Model
                .CreatePredictionEngine<Review, ReviewPrediction>(mlModel);

            ReviewPrediction result = predEngine.Predict(
                new Review { ReviewText = text });

            // Returns true for positive review, false for negative
            return result.Prediction;
        }

        // Class to represent each review
        public class Review
        {
            // Column name must match input file
            [LoadColumn(0)]
            public string ReviewText;
        }

        // Class resulting from ML.NET code including predictions about review
        public class ReviewPrediction : Review
        {
            [ColumnName("PredictedLabel")]
            public bool Prediction { get; set; }

            public float Probability { get; set; }

            public float Score { get; set; }
        }
    }
}
