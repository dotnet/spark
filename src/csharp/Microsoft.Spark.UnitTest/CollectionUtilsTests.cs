using System.IO;
using System.Linq;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class CollectionUtilsTests
    {
        [Fact]
        public void TestArrayEquals()
        {
            Assert.False(CollectionUtils.ArrayEquals(new int[] { 1 }, null));
            Assert.False(CollectionUtils.ArrayEquals(null, new int[] { 1 }));
            Assert.False(CollectionUtils.ArrayEquals(new int[] { }, new int[] { 1 }));
            Assert.False(CollectionUtils.ArrayEquals(new int[] { 1 }, new int[] { }));
            Assert.False(CollectionUtils.ArrayEquals(new int[] { 1 }, new int[] { 1, 2 }));
            Assert.False(CollectionUtils.ArrayEquals(new int[] { 1 }, new int[] { 2 }));

            Assert.True(CollectionUtils.ArrayEquals<object>(null, null));
            Assert.True(CollectionUtils.ArrayEquals(new int[] { 1 }, new int[] { 1 }));
        }
    }
}
