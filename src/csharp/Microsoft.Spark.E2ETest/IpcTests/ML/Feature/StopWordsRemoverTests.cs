using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class StopWordsRemoverTests : FeatureBaseTests<StopWordsRemoverTests>
    {
        private readonly SparkSession _spark;

        public StopWordsRemoverTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// 
        /// </summary>
        [Fact]
        public void TestStopWordsRemover()
        {
            var stopWordsRemover = new StopWordsRemover();

            DataFrame source = _spark.CreateDataFrame(new List<string>() { "hello egy bye" });

            bool expectedCasesensitivity = false;
            string expectedLocale = "en_GB";
            string expectedInputCol = "_c0";
            string expectedOutputCol = "output";

            Assert.IsType<StopWordsRemover>(stopWordsRemover.SetCaseSensitive(expectedCasesensitivity));
            Assert.Equal(expectedCasesensitivity, stopWordsRemover.GetCaseSensitive());

            Assert.IsType<StopWordsRemover>(stopWordsRemover.SetLocale(expectedLocale));
            Assert.Equal(expectedLocale, stopWordsRemover.GetLocale());
            
            Assert.IsType<StopWordsRemover>(stopWordsRemover.SetInputCol(expectedInputCol));
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());

            Assert.IsType<StopWordsRemover>(stopWordsRemover.SetOutputCol(expectedOutputCol));
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());

            Assert.IsType<string[]>(StopWordsRemover.LoadDefaultStopWords("hungarian"));

            Assert.IsType<StructType>(stopWordsRemover.TransformSchema(source.Schema()));
            Assert.IsType<DataFrame>(stopWordsRemover.Transform(source));

            //TestFeatureBase(stopWordsRemover, "caseSensitive", true);
        }
    }
}
