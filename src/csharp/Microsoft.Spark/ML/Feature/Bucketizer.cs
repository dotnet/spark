// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Param;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// `Bucketizer` maps a column of continuous features to a column of feature buckets.
    /// 
    /// `Bucketizer` can map multiple columns at once by setting the `inputCols` parameter. Note
    /// that when both the `inputCol` and `inputCols` parameters are set, an Exception will be
    /// thrown. The `splits` parameter is only used for single column usage, and `splitsArray` is
    /// for multiple columns.
    /// </summary>
    public class Bucketizer : IJvmObjectReferenceProvider
    {
        internal Bucketizer(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        
        private readonly JvmObjectReference _jvmObject;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Create a `Bucketizer` without any parameters
        /// </summary>
        public Bucketizer()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.Bucketizer");
        }

        /// <summary>
        /// Create a `Bucketizer` with a UID that is used to give the `Bucketizer` a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Bucketizer(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.Bucketizer", uid);
        }
        
        /// <summary>
        /// Split points for splitting a single column into buckets. To split multiple columns use
        /// `SetSplitsArray`. You cannot use both `SetSplits` and `SetSplitsArray` at the same time
        /// </summary>
        /// <param name="value">
        /// Split points for mapping continuous features into buckets. With n+1 splits, there are n
        /// buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last
        /// bucket, which also includes y. The splits should be of length &gt;= 3 and strictly
        /// increasing. Values outside the splits specified will be treated as errors.
        /// </param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetSplits(double[] value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setSplits", value));
        }

        /// <summary>
        /// Split points fot splitting multiple columns into buckets. To split a single column use
        /// `SetSplits`.  You cannot use both `SetSplits` and `SetSplitsArray` at the same time.
        /// </summary>
        /// <param name="value">
        /// The array of split points for mapping continuous features into buckets for multiple 
        /// columns. For each input column, with n+1 splits, there are n buckets. A bucket defined
        /// by splits x,y holds values in the range [x,y) except the last bucket, which also
        /// includes y. The splits should be of length &gt;= 3 and strictly increasing.
        /// Values outside the splits specified will be treated as errors.</param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetSplitsArray(double[][] value)
        {
            DoubleArrayArrayParam doubleArrayArray = new DoubleArrayArrayParam(_jvmObject,
                "setSplitsArray",
                "wrapper for double[][] from csharp", value);

            return WrapAsBucketizer(_jvmObject.Invoke("setSplitsArray",
                doubleArrayArray.ReferenceValue));
        }

        /// <summary>
        /// Sets the column that the `Bucketizer` should read from and convert into buckets
        /// </summary>
        /// <param name="value">The name of the column to as the source of the buckets</param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetInputCol(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setInputCol", value));
        }

        /// <summary>
        /// Sets the columns that `Bucketizer` should read from and convert into buckets.
        ///
        /// Each column is one set of buckets so if you have two input columns you can have two
        ///  sets of buckets and two output columns.
        /// </summary>
        /// <param name="value">List of input columns to use as sources for buckets</param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetInputCols(List<string> value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setInputCols", value));
        }

        /// <summary>
        /// The `Bucketizer` will create a new column in the DataFrame, this is the name of the
        /// new column.
        /// </summary>
        /// <param name="value">The name of the new column which contains the bucket ID</param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetOutputCol(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setOutputCol", value));
        }
        
        /// <summary>
        /// The list of columns that the `Bucketizer` will create in the DataFrame.
        /// </summary>
        /// <param name="value">List of column names which will contain the bucket ID</param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetOutputCols(List<string> value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setOutputCols", value));
        }
        
        /// <summary>
        /// Executes the `Bucketizer` and transforms the DataFrame to include the new column or
        /// columns with the bucketed data.
        /// </summary>
        /// <param name="source">The DataFrame to add the bucketed data to</param>
        /// <returns>`DataFrame` containing the original data and the new bucketed
        ///             columns</returns>
        public DataFrame Transform(DataFrame source)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform"
                , source));
        }

        /// <summary>
        /// The reference we get back from each call isn't usable unless we wrap it in a new dotnet
        ///  `Bucketizer`
        /// </summary>
        /// <param name="obj">The `JvmObjectReference` to convert into a dotnet
        ///                     `Bucketizer`</param>
        /// <returns>`Bucketizer`</returns>
        private static Bucketizer WrapAsBucketizer(object obj)
        {
            return new Bucketizer((JvmObjectReference)obj);
        }

        /// <summary>
        /// The uid that was used to create the `Bucketizer`. If no `UID` is passed in when creating
        ///  the `Bucketizer` then a random `UID` is created when the `Bucketizer` is created.
        /// </summary>
        /// <returns>string `UID` identifying the `Bucketizer`</returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }

        /// <summary>
        /// How should the `Bucketizer` handle invalid data, choices are "skip", "error" or "keep"
        /// </summary>
        /// <returns>`BucketizerInvalidOptions`</returns>
        public BucketizerInvalidOptions GetHandleInvalid()
        {
            string handleInvalid = (string)_jvmObject.Invoke("getHandleInvalid");
            if (BucketizerInvalidOptions.TryParse(handleInvalid, true, 
                out BucketizerInvalidOptions result))
            {
                return result;
            }
            
            return result;
        }

        /// <summary>
        /// Tells the `Bucketizer` what to do with invalid data.
        ///
        /// Choices are "skip", "error" or "keep". Default is "error"
        /// </summary>
        /// <param name="value">`BucketizerInvalidOptions`, "skip", "error" or "keep"</param>
        /// <returns>`Bucketizer`</returns>
        public Bucketizer SetHandleInvalid(BucketizerInvalidOptions value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setHandleInvalid", value.ToString()));
        }
        
        /// <summary>
        /// dotnet version of the options that can be passed to the `Bucketizer` to tell it how to
        ///  handle invalid data.
        /// </summary>
        public enum BucketizerInvalidOptions
        {
            unknown,
            skip,
            error,
            keep
        }
    }
}
