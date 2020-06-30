// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A <see cref="IDFModel"/> that converts the input string to lowercase and then splits it by
    /// white spaces.
    /// </summary>
    public class IDFModel : FeatureBase<IDFModel>, IJvmObjectReferenceProvider
    {
        private static readonly string s_IDFModelClassName = 
            "org.apache.spark.ml.feature.IDFModel";
        
        /// <summary>
        /// Create a <see cref="IDFModel"/> without any parameters
        /// </summary>
        public IDFModel() : base(s_IDFModelClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="IDFModel"/> with a UID that is used to give the
        /// <see cref="IDFModel"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public IDFModel(string uid) : base(s_IDFModelClassName, uid)
        {
        }
        
        internal IDFModel(JvmObjectReference jvmObject) : base(jvmObject, s_IDFModelClassName)
        {
        }
        
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Gets the column that the <see cref="IDFModel"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol()
        {
            return (string)(_jvmObject.Invoke("getInputCol"));
        }
        
        /// <summary>
        /// Sets the column that the <see cref="IDFModel"/> should read from and convert into
        /// buckets
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="IDFModel"/> object</returns>
        public IDFModel SetInputCol(string value)
        {
            return WrapAsIDFModel(_jvmObject.Invoke("setInputCol", value));
        }

        /// <summary>
        /// The <see cref="IDFModel"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol()
        {
            return (string)(_jvmObject.Invoke("getOutputCol"));
        }
        
        /// <summary>
        /// The <see cref="IDFModel"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column which contains the tokens
        /// </param>
        /// <returns>New <see cref="IDFModel"/> object</returns>
        public IDFModel SetOutputCol(string value)
        {
            return WrapAsIDFModel(_jvmObject.Invoke("setOutputCol", value));
        }
        
        /// <summary>
        /// Minimum of documents in which a term should appear for filtering
        /// </summary>
        /// <returns>Minimum number of documents a term should appear</returns>
        public int GetMinDocFreq()
        {
            return (int)_jvmObject.Invoke("getMinDocFreq");
        }
        
        /// <summary>
        /// Executes the <see cref="IDFModel"/> and transforms the <see cref="DataFrame"/> to
        /// include the new column or columns with the tokens.
        /// </summary>
        /// <param name="source">The <see cref="DataFrame"/> to add the tokens to</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the tokens</returns>
        public DataFrame Transform(DataFrame source)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));
        }

        /// <summary>
        /// Loads the <see cref="IDFModel"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="IDFModel"/> was saved to</param>
        /// <returns>New <see cref="IDFModel"/> object, loaded from path</returns>
        public static IDFModel Load(string path)
        {
            return WrapAsIDFModel(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_IDFModelClassName, "load", path));
        }

        private static IDFModel WrapAsIDFModel(object obj) => 
            new IDFModel((JvmObjectReference)obj);
    }
}
