
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    public class FeatureBaseTests<T>
    {
        internal static void TestBase(
            FeatureBase<T> testObject, 
            string paramName, 
            object paramValue)
        {
            Assert.NotEmpty(testObject.ExplainParams());
            
            Param handleInvalidParam = testObject.GetParam(paramName);
            Assert.NotEmpty(handleInvalidParam.Doc);
            Assert.NotEmpty(handleInvalidParam.Name);
            Assert.Equal(handleInvalidParam.Parent, testObject.Uid());

            Assert.NotEmpty(testObject.ExplainParam(handleInvalidParam));
            testObject.Set(handleInvalidParam, paramValue);
            Assert.IsAssignableFrom<Identifiable>(testObject.Clear(handleInvalidParam));

            Assert.IsType<string>(testObject.Uid());
        }
    }
}
