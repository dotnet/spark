// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataStreamReaderTests
    {
        private readonly SparkSession _spark;

        public DataStreamReaderTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            DataStreamReader dsr = _spark.ReadStream();

            Assert.IsType<DataStreamReader>(dsr.Format("parquet"));
            Assert.IsType<DataStreamReader>(dsr.Schema("columnName bigint"));
            Assert.IsType<DataStreamReader>(dsr.Option("key", "value"));
            Assert.IsType<DataStreamReader>(dsr.Option("key", false));
            Assert.IsType<DataStreamReader>(dsr.Option("key", long.MaxValue));
            Assert.IsType<DataStreamReader>(dsr.Option("key", double.MaxValue));
            Assert.IsType<DataStreamReader>(dsr.Options(new Dictionary<string, string>()));

            DataFrame data = _spark.Range(0, 5);
            using (var tempDirectory = new TemporaryDirectory())
            {
                string path = Path.Combine(tempDirectory.Path, Guid.NewGuid().ToString());

                data.Write().Json(path);
                Assert.IsType<DataFrame>(dsr.Format("json").Option("path", path).Load());
                Assert.IsType<DataFrame>(dsr.Format("json").Load(path));
                Assert.IsType<DataFrame>(dsr.Json(path));

                data.Write().Mode("overwrite").Csv(path);
                Assert.IsType<DataFrame>(dsr.Csv(path));

                data.Write().Mode("overwrite").Orc(path);
                Assert.IsType<DataFrame>(dsr.Orc(path));

                data.Write().Mode("overwrite").Parquet(path);
                Assert.IsType<DataFrame>(dsr.Parquet(path));
            }

            // Text is a special case, because we can't use Range() data.
            string textFilePath = Path.GetFullPath("Resources/people.txt");
            Assert.IsType<DataFrame>(dsr.Text(textFilePath));
        }
    }
}
