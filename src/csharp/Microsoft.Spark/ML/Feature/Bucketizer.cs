// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="Bucketizer"/> maps a column of continuous features to a column of feature
    /// buckets.
    /// 
    /// <see cref="Bucketizer"/> can map multiple columns at once by setting the inputCols
    /// parameter. Note that when both the inputCol and inputCols parameters are set, an Exception
    /// will be thrown. The splits parameter is only used for single column usage, and splitsArray
    /// is for multiple columns.
    /// </summary>
    public class Bucketizer : IJvmObjectReferenceProvider
    {
        internal Bucketizer(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        
        /// <summary>
        /// Create a <see cref="Bucketizer"/> without any parameters
        /// </summary>
        public Bucketizer()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.Bucketizer");
        }

        /// <summary>
        /// Create a <see cref="Bucketizer"/> with a UID that is used to give the
        /// <see cref="Bucketizer"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Bucketizer(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.Bucketizer", uid);
        }
        
        private readonly JvmObjectReference _jvmObject;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Gets the splits that were set using SetSplits
        /// </summary>
        /// <returns>double[], the splits to be used to bucket the input column</returns>
        public double[] GetSplits()
        {
            return (double[])_jvmObject.Invoke("getSplits");
        }
        
        /// <summary>
        /// Split points for splitting a single column into buckets. To split multiple columns use
        /// SetSplitsArray. You cannot use both SetSplits and SetSplitsArray at the same time
        /// </summary>
        /// <param name="value">
        /// Split points for mapping continuous features into buckets. With n+1 splits, there are n
        /// buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last
        /// bucket, which also includes y. The splits should be of length &gt;= 3 and strictly
        /// increasing. Values outside the splits specified will be treated as errors.
        /// </param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetSplits(double[] value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setSplits", value));
        }

        /// <summary>
        /// Gets the splits that were set by SetSplitsArray
        /// </summary>
        /// <returns>double[][], the splits to be used to bucket the input columns</returns>
        public double[][] GetSplitsArray()
        {
            return (double[][])_jvmObject.Invoke("getSplitsArray");
        } 
        
        /// <summary>
        /// Split points fot splitting multiple columns into buckets. To split a single column use
        /// SetSplits. You cannot use both SetSplits and SetSplitsArray at the same time.
        /// </summary>
        /// <param name="value">
        /// The array of split points for mapping continuous features into buckets for multiple 
        /// columns. For each input column, with n+1 splits, there are n buckets. A bucket defined
        /// by splits x,y holds values in the range [x,y) except the last bucket, which also
        /// includes y. The splits should be of length &gt;= 3 and strictly increasing.
        /// Values outside the splits specified will be treated as errors.</param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetSplitsArray(double[][] value)
        {
            double[][][] wrappedValue = new[] {value};
            return WrapAsBucketizer(_jvmObject.Invoke("setSplitsArray", wrappedValue));
        }

        /// <summary>
        /// Gets the column that the <see cref="Bucketizer"/> should read from and convert into
        /// buckets. This would have been set by SetInputCol
        /// </summary>
        /// <returns>string, the input column</returns>
        public string GetInputCol()
        {
            return (string)_jvmObject.Invoke("getInputCol");
        }
        
        /// <summary>
        /// Sets the column that the <see cref="Bucketizer"/> should read from and convert into
        /// buckets
        /// </summary>
        /// <param name="value">The name of the column to as the source of the buckets</param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetInputCol(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setInputCol", value));
        }
        
        /// <summary>
        /// Gets the columns that <see cref="Bucketizer"/> should read from and convert into
        /// buckets. This is set by SetInputCol
        /// </summary>
       /// <returns>IEnumerable&lt;string&gt;, list of input columns</returns>
        public IEnumerable<string> GetInputCols()
        {
            return ((string[])(_jvmObject.Invoke("getInputCols"))).ToList();
        }

        /// <summary>
        /// Sets the columns that <see cref="Bucketizer"/> should read from and convert into
        /// buckets.
        ///
        /// Each column is one set of buckets so if you have two input columns you can have two
        /// sets of buckets and two output columns.
        /// </summary>
        /// <param name="value">List of input columns to use as sources for buckets</param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetInputCols(IEnumerable<string> value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setInputCols", value));
        }
        
        /// <summary>
        /// Gets the name of the column the output data will be written to. This is set by
        /// SetInputCol
        /// </summary>
        // <returns>string, the output column</returns>
        public string GetOutputCol()
        {
            return (string)_jvmObject.Invoke("getOutputCol");
        }
        
        /// <summary>
        /// The <see cref="Bucketizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column which contains the bucket ID</param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetOutputCol(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setOutputCol", value));
        }

        /// <summary>
        /// The list of columns that the <see cref="Bucketizer"/> will create in the DataFrame.
        /// This is set by SetOutputCols
        /// </summary>
        /// <returns>IEnumerable&lt;string&gt;, list of output columns</returns>
        public IEnumerable<string> GetOutputCols()
        {
            return ((string[])_jvmObject.Invoke("getOutputCols")).ToList();
        }
        
        /// <summary>
        /// The list of columns that the <see cref="Bucketizer"/> will create in the DataFrame.
        /// </summary>
        /// <param name="value">List of column names which will contain the bucket ID</param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetOutputCols(List<string> value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setOutputCols", value));
        }

        /// <summary>
        /// Executes the <see cref="Bucketizer"/> and transforms the DataFrame to include the new
        /// column or columns with the bucketed data.
        /// </summary>
        /// <param name="source">The DataFrame to add the bucketed data to</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the new bucketed
        /// columns</returns>
        public DataFrame Transform(DataFrame source)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));
        }

        /// <summary>
        /// The reference we get back from each call isn't usable unless we wrap it in a new dotnet
        /// <see cref="Bucketizer"/>
        /// </summary>
        /// <param name="obj">The <see cref="JvmObjectReference"/> to convert into a dotnet
        /// <see cref="Bucketizer"/></param>
        /// <returns><see cref="Bucketizer"/></returns>
        private static Bucketizer WrapAsBucketizer(object obj)
        {
            return new Bucketizer((JvmObjectReference)obj);
        }

        /// <summary>
        /// The uid that was used to create the <see cref="Bucketizer"/>. If no UID is passed in
        /// when creating the <see cref="Bucketizer"/> then a random UID is created when the
        /// <see cref="Bucketizer"/> is created.
        /// </summary>
        /// <returns>string UID identifying the <see cref="Bucketizer"/></returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }

        /// <summary>
        /// How should the <see cref="Bucketizer"/> handle invalid data, choices are "skip",
        /// "error" or "keep"
        /// </summary>
        /// <returns>string showing the way Spark will handle invalid data</returns>
        public string GetHandleInvalid()
        {
            return (string)_jvmObject.Invoke("getHandleInvalid");
        }

        /// <summary>
        /// Tells the <see cref="Bucketizer"/> what to do with invalid data.
        ///
        /// Choices are "skip", "error" or "keep". Default is "error"
        /// </summary>
        /// <param name="value">"skip", "error" or "keep"</param>
        /// <returns><see cref="Bucketizer"/></returns>
        public Bucketizer SetHandleInvalid(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setHandleInvalid", value.ToString()));
        }
    }
}
