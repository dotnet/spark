// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    public static class FeatureBaseTests<T>
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
