// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;

namespace Microsoft.Spark.Examples.MachineLearning.SentimentStream
{
    /// <summary>
    /// Example of using ML.NET + .NET for Apache Spark
    /// for sentiment analysis of streaming data.
    /// </summary>
    internal sealed class Program : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 3)
            {
                Console.Error.WriteLine(
                    "Usage: SentimentAnalysisStream <host> <port> <model path>");
                Environment.Exit(1);
            }

            // Create Spark Session
            SparkSession spark = SparkSession
                .Builder()
                .AppName("Streaming Sentiment Analysis")
                .GetOrCreate();

            // Setup stream connection info
            string hostname = args[0];
            string port = args[1];

            // Read streaming data into DataFrame
            DataFrame words = spark
                .ReadStream()
                .Format("socket")
                .Option("host", hostname)
                .Option("port", port)
                .Load();

            // Use ML.NET in a UDF to evaluate each incoming entry
            spark.Udf().Register<string, bool>(
                "MLudf",
                input => Sentiment(input, args[2]));

            // Use Spark SQL to call ML.NET UDF
            // Display results of sentiment analysis on each entry
            words.CreateOrReplaceTempView("WordsSentiment");
            DataFrame sqlDf = spark
                .Sql("SELECT WordsSentiment.value, MLudf(WordsSentiment.value) FROM WordsSentiment");

            // Handle data continuously as it arrives
            StreamingQuery query = sqlDf
                .WriteStream()
                .Format("console")
                .Start();

            query.AwaitTermination();
        }

        // Method to call ML.NET code for sentiment analysis
        // Code primarily comes from ML.NET Model Builder
        public static bool Sentiment(string text, string modelPath)
        {
            var mlContext = new MLContext();

            ITransformer mlModel = mlContext
                .Model
                .Load(modelPath, out var modelInputSchema);

            PredictionEngine<Review, ReviewPrediction> predEngine = mlContext
                .Model
                .CreatePredictionEngine<Review, ReviewPrediction>(mlModel);

            ReviewPrediction result = predEngine.Predict(
                new Review { ReviewText = text });

            // Returns true for positive, false for negative
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
