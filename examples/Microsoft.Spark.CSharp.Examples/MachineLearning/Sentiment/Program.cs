// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Examples
{
    /// <summary>
    /// Example of using ML.NET + .NET for Apache Spark
    /// for sentiment analysis.
    /// </summary>
    public class Program
    {
        public static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET for Apache Spark Sentiment Analysis")
                .GetOrCreate();

            // Read in and display Yelp reviews
            DataFrame df = spark
                .Read()
                .Option("header", true)
                .Option("inferSchema", true)
                .Csv(@"Resources\yelp.csv");
            df.Show();

            // Use ML.NET in a UDF to evaluate each review 
            spark.Udf().Register<string, bool>(
                "MLudf",
                (text) => Sentiment(text));

            // Use Spark SQL to call ML.NET UDF
            // Display results of sentiment analysis on reviews
            df.CreateOrReplaceTempView("Reviews");
            DataFrame sqlDf = spark.Sql("SELECT Column1, MLudf(Column1) FROM Reviews");
            sqlDf.Show();

            // Print out first 20 rows of data
            // Prevents data getting cut off (as it is when we print a DF)
            IEnumerable<Row> rows = sqlDf.Take(20);
            foreach (Row row in rows)
            {
                Console.WriteLine(row);
            }

            spark.Stop();
        }

        // Method to call ML.NET code for sentiment analysis
        // Code primarily comes from ML.NET Model Builder
        public static bool Sentiment(string text)
        {
            MLContext mlContext = new MLContext();

            ITransformer mlModel = mlContext
                .Model
                .Load(@"Resources\MLModel.zip", out var modelInputSchema);

            var predEngine = mlContext
               .Model
               .CreatePredictionEngine<Review, ReviewPrediction>(mlModel);

            var result = predEngine.Predict(
                new Review { Column1 = text });

            // Returns true for positive review, false for negative
            return result.Prediction;
        }

        // Class to represent each review
        public class Review
        {
            // Column names must match input file
            // Column1 is review
            [LoadColumn(0)]
            public string Column1;
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
