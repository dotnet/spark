// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    public class FeatureBaseTests<T>
    {
        private readonly SparkSession _spark;

        protected FeatureBaseTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }
        
        /// <summary>
        /// Tests the common functionality across all ML.Feature classes.
        /// </summary>
        /// <param name="testObject">The object that implemented FeatureBase</param>
        /// <param name="paramName">The name of a parameter that can be set on this object</param>
        /// <param name="paramValue">A parameter value that can be set on this object</param>
        public void TestFeatureBase(
            Params testObject, 
            string paramName, 
            object paramValue)
        {
            Assert.NotEmpty(testObject.ExplainParams());
            
            Param param = testObject.GetParam(paramName);
            Assert.NotEmpty(param.Doc);
            Assert.NotEmpty(param.Name);
            Assert.Equal(param.Parent, testObject.Uid());

            Assert.NotEmpty(testObject.ExplainParam(param));
            testObject.Set<T>(param, paramValue);
            Assert.IsAssignableFrom<Identifiable>(testObject.Clear<T>(param));

            Assert.IsType<string>(testObject.Uid());
        }
    }
}
