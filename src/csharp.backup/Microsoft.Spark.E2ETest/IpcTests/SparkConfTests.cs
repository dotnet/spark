// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class SparkConfTests
    {
        [Fact]
        public void TestSparkConf()
        {
            var sparkConf = new SparkConf(false);

            sparkConf.SetMaster("master");
            sparkConf.SetAppName("test");
            sparkConf.SetSparkHome("test home");
            sparkConf.Set("key_string", "value");
            sparkConf.Set("key_int", "100");

            var expectedConfigs = new Dictionary<string, string>()
            {
                { "spark.master", "master" },
                { "spark.app.name", "test" },
                { "spark.home", "test home" },
                { "key_string", "value" },
                { "key_int", "100" }
            };

            foreach (KeyValuePair<string, string> kv in expectedConfigs)
            {
                Assert.Equal(kv.Value, sparkConf.Get(kv.Key, string.Empty));
            }

            Assert.Equal(100, sparkConf.GetInt("key_int", 0));

            // Validate GetAll().
            Dictionary<string, string> actualAllConfigs = 
                sparkConf.GetAll().ToDictionary(x => x.Key, x => x.Value);

            Assert.Equal(expectedConfigs, actualAllConfigs);
        }
    }
}
