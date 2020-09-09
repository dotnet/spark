// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class SerDeTests
    {
        private readonly SparkSession _spark;

        public SerDeTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestUnicode()
        {
            string expected =
                "①Ⅻㄨㄩ 啊阿鼾齄丂丄狚狛狜狝﨨﨩ˊˋ˙–⿻〇㐀㐁㐃㐄䶴䶵U1[]U2[]U3[]";

            RuntimeConfig conf = _spark.Conf();
            string key = "SerDeTests.TestUnicode";
            conf.Set(key, expected);

            string actual = conf.Get(key);
            Assert.Equal(expected, actual);
        }
    }
}
