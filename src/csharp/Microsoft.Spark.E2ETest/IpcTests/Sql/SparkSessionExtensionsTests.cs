// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using Microsoft.Spark.Experimental.Sql;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class SparkSessionExtensionsTests
    {
        private readonly SparkSession _spark;

        public SparkSessionExtensionsTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestVersion()
        {
            DataFrame versionDf = _spark.GetAssemblyInfo();
            Row[] versionRows = versionDf.Collect().ToArray();
            Assert.Equal(2, versionRows.Length);

            Assert.Equal(
                new string[] { "Microsoft.Spark", "Microsoft.Spark.Worker" },
                versionRows.Select(r => r.GetAs<string>("AssemblyName")));
            for (int i = 0; i < 2; ++i)
            {
                Assert.False(
                    string.IsNullOrWhiteSpace(versionRows[i].GetAs<string>("AssemblyVersion")));
                Assert.False(
                    string.IsNullOrWhiteSpace(versionRows[i].GetAs<string>("HostName")));
            }
        }
    }
}
