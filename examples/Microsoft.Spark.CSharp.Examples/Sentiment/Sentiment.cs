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
    class SentimentAnalysis
    {
        public void Main(string[] args)
        {
            // Change this flag once you've set up
            // the ML.NET dependencies described in the README:
            // Update url to location of MLModel.zip (line 90)
            // Copy ML.NET dlls to your project's folder
            int dependenciesDone = 1;

            /*if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: SentimentAnalysis <path to review dataset>");

                Environment.Exit(1);
            }*/

            if (dependenciesDone == 0)
            {
                Console.Error.WriteLine(
                    "You need to complete setting ML.NET dependencies");

                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET for Apache Spark Sentiment Analysis")
                .GetOrCreate();

            DataFrame df = spark
                .Read()
                .Option("header", true)
                .Option("inferSchema", true)
                .Csv("yelp.csv");
            df.Show();

            // Use ML.NET to evaluate each review 
            spark.Udf().Register<string, bool>(
                "MLudf",
                (text) => Sentiment(text));

            df.CreateOrReplaceTempView("Reviews");
            DataFrame sqlDf = spark.Sql("SELECT Column1, MLudf(Column1) FROM Reviews");
            sqlDf.Show();

            // Print out first 20 rows of data
            // Prevents data getting cut off (as it is when we print a DF)
            IEnumerable<Row> rows = sqlDf.Collect();
            int counter = 0;
            foreach (Row row in rows)
            {
                counter++;
                if (counter < 20)
                    Console.WriteLine(row);
                else
                    break;
            }

            spark.Stop();
        }

        // To use ML.NET sure have ProjectReference in .csproj file:
        // Include="<path to sentimentML.Model.csproj>"
        // See project's README to learn more
        public static bool Sentiment(string text)
        {
            MLContext mlContext = new MLContext();

            // Remember to change "MLModel.zip" to accurate model location
            ITransformer mlModel = mlContext
                .Model
                .Load("MLSparkVideoML.Model/MLModel.zip", out var modelInputSchema);

            var predEngine = mlContext
               .Model
               .CreatePredictionEngine<Review, ReviewPrediction>(mlModel);

            var result = predEngine.Predict(
                new Review { Column1 = text });

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
