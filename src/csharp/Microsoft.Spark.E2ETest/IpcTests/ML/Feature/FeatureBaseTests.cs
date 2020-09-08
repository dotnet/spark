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
            
            Param param = testObject.GetParam(paramName);
            Assert.NotEmpty(param.Doc);
            Assert.NotEmpty(param.Name);
            Assert.Equal(param.Parent, testObject.Uid());

            Assert.NotEmpty(testObject.ExplainParam(param));
            testObject.Set(param, paramValue);
            Assert.IsAssignableFrom<Identifiable>(testObject.Clear(param));

            Assert.IsType<string>(testObject.Uid());
        }
    }
}
