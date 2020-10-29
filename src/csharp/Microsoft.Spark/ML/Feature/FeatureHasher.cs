// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    public class FeatureHasher: FeatureBase<FeatureHasher>, IJvmObjectReferenceProvider
    {
        private static readonly string s_featureHasherClassName = 
            "org.apache.spark.ml.feature.FeatureHasher";
        
        public FeatureHasher() : base(s_featureHasherClassName)
        {
        }

        public FeatureHasher(string uid) : base(s_featureHasherClassName, uid)
        {
        }

        internal FeatureHasher(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Loads the <see cref="FeatureHasher"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="FeatureHasher"/> was saved to.
        /// </param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public static FeatureHasher Load(string path) =>
            WrapAsFeatureHasher(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_featureHasherClassName,
                    "load",
                    path));
        
        /// <summary>
        /// Gets a list of the columns which have been specified as categorical columns.
        /// </summary>
        /// <returns>List of categorical columns, set by SetCategoricalCols</returns>
        public IEnumerable<string> GetCategoricalCols() => 
            (string[])_jvmObject.Invoke("getCategoricalCols");
        
        /// <summary>
        /// Marks columns as categorical columns.
        /// </summary>
        /// <param name="value">List of column names to mark as categorical columns</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetCategoricalCols(IEnumerable<string> value) => 
            WrapAsFeatureHasher(_jvmObject.Invoke("setCategoricalCols", value));
        
        /// <summary>
        /// Gets the columns that the <see cref="FeatureHasher"/> should read from and convert into
        /// hashes. This would have been set by SetInputCol.
        /// </summary>
        /// <returns>List of the input columns, set by SetInputCols</returns>
        public IEnumerable<string> GetInputCols() => (string[])_jvmObject.Invoke("getInputCols");

        /// <summary>
        /// Sets the columns that the <see cref="FeatureHasher"/> should read from and convert into
        /// hashes.
        /// </summary>
        /// <param name="value">The name of the column to as use the source of the hash</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetInputCols(IEnumerable<string> value) => 
            WrapAsFeatureHasher(_jvmObject.Invoke("setInputCols", value));
        
        /// <summary>
        /// Gets the number of features that should be used. Since a simple modulo is used to
        /// transform the hash function to a column index, it is advisable to use a power of two
        /// as the numFeatures parameter; otherwise the features will not be mapped evenly to the
        /// columns.
        /// </summary>
        /// <returns>The number of features to be used</returns>
        public int GetNumFeatures() => (int)_jvmObject.Invoke("getNumFeatures");

        /// <summary>
        /// Sets the number of features that should be used. Since a simple modulo is used to
        /// transform the hash function to a column index, it is advisable to use a power of two as
        /// the numFeatures parameter; otherwise the features will not be mapped evenly to the
        /// columns.
        /// </summary>
        /// <param name="value">int value of number of features</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetNumFeatures(int value) => 
            WrapAsFeatureHasher(_jvmObject.Invoke("setNumFeatures", value));
        
        /// <summary>
        /// Gets the name of the column the output data will be written to. This is set by
        /// SetInputCol.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)_jvmObject.Invoke("getOutputCol");

        /// <summary>
        /// Sets the name of the new column in the <see cref="DataFrame"/> created by Transform.
        /// </summary>
        /// <param name="value">The name of the new column which will contain the hash</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetOutputCol(string value) => 
            WrapAsFeatureHasher(_jvmObject.Invoke("setOutputCol", value));
        
        /// <summary>
        /// Transforms the input <see cref="DataFrame"/>. It is recommended that you validate that
        /// the transform will succeed by calling TransformSchema.
        /// </summary>
        /// <param name="value">Input <see cref="DataFrame"/> to transform</param>
        /// <returns>Transformed <see cref="DataFrame"/></returns>
        public DataFrame Transform(DataFrame value) => 
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", value));
        
        /// <summary>
        /// Check transform validity and derive the output schema from the input schema.
        /// 
        /// This checks for validity of interactions between parameters during Transform and
        /// raises an exception if any parameter value is invalid.
        ///
        /// Typical implementation should first conduct verification on schema change and parameter
        /// validity, including complex parameter interaction checks.
        /// </summary>
        /// <param name="value">
        /// The <see cref="StructType"/> of the <see cref="DataFrame"/> which will be transformed.
        /// </param>
        /// <returns>
        /// The <see cref="StructType"/> of the output schema that would have been derived from the
        /// input schema, if Transform had been called.
        /// </returns>
        public StructType TransformSchema(StructType value) => 
            new StructType(
                (JvmObjectReference)_jvmObject.Invoke(
                    "transformSchema", 
                    DataType.FromJson(_jvmObject.Jvm, value.Json)));

        private static FeatureHasher WrapAsFeatureHasher(object obj) => 
            new FeatureHasher((JvmObjectReference)obj);
    }
}
