// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Linalg
{
    public class SparseVector : IJvmObjectReferenceProvider
    {
        private static readonly string s_sparseVectorClassName = "org.apache.spark.ml.linalg.Vectors";

        private readonly JvmObjectReference _jvmObject;

        /// <summary>
        /// Represents a numeric vector, whose index type is Int and value type is Double.
        /// <see cref="SparseVector"/> is an efficient way to store large arrays of double where many of the
        /// elements are 0.
        /// </summary>
        /// <param name="length"></param>
        /// <param name="indices"></param>
        /// <param name="values"></param>
        public SparseVector(int length, int[] indices, double[] values) => 
            _jvmObject = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_sparseVectorClassName, "sparse", length, indices, values);
        //Note this calls Vectors.sparse as the SparseVector ctor is private

        internal SparseVector(JvmObjectReference jvmObject) => _jvmObject = jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Gets the value of the ith element.
        /// </summary>
        /// <param name="i">The element to return</param>
        /// <returns>The specific value at the ith element or zero if it is not set</returns>
        public double Apply(int i) => (double)_jvmObject.Invoke("apply", i);

        /// <summary>
        /// Number of active entries. An "active entry" is an element which is explicitly stored, regardless
        /// of its value.
        /// </summary>
        /// <returns>The number of active entries in the <see cref="SparseVector"/></returns>
        public int NumActives() => (int)_jvmObject.Invoke("numActives");

        /// <summary>
        /// Number of nonzero elements.
        /// </summary>
        /// <returns>The number of non zero elements in the <see cref="SparseVector"/></returns>
        public int NumNonZeros() => (int)_jvmObject.Invoke("numNonzeros");

        /// <summary>
        /// Find the index of a maximal element. Returns the first maximal element in case of a tie.
        /// Returns -1 if vector has length 0.
        /// </summary>
        /// <returns>The maximal element</returns>
        public int ArgMax() => (int)_jvmObject.Invoke("argmax");

        /// <summary>
        /// The indices of the <see cref="SparseVector"/>.
        /// </summary>
        /// <returns>The indices of the <see cref="SparseVector"/></returns>
        public int[] Indices() => (int[])_jvmObject.Invoke("indices");

        /// <summary>
        /// The values of the <see cref="SparseVector"/>.
        /// </summary>
        /// <returns>The values of the <see cref="SparseVector"/></returns>
        public double[] Values() => (double[])_jvmObject.Invoke("values");

        /// <summary>
        /// Converts the instance to a double array. This will return every element of the
        /// <see cref="SparseVector"/> whether it is a 0 or not. 
        /// </summary>
        /// <returns>Array of every element in the <see cref="SparseVector"/></returns>
        public double[] ToArray() => (double[])_jvmObject.Invoke("toArray");

        /// <summary>
        /// Returns the JVM toString value rather than the .NET ToString default
        /// </summary>
        /// <returns>JVM toString() value</returns>
        public override string ToString() => (string)_jvmObject.Invoke("toString");

        /// <summary>
        /// This is a helper function for use with user defined functions (UDF) + when calling .Collect().
        ///
        /// When Apache Spark calls a UDF in .NET for Apache Spark with a <see cref="SparseVector"/> the
        /// function receives a row that is an ArrayList of four elements. The four elements are the vector
        /// type (0 = sparse, 1 = dense), the vector length, the indices and finally the values. You can
        /// iterate the values of the vector using the indices and the values directly or you can pass the
        /// values back to Apache Spark which will allow you to use the convenience methods such as Apply
        /// which will enumerate the indices for you and return the correct value.
        ///
        /// Note that if you pass the vector back to Apache Spark you are effectively causing the data to be
        /// written over the pickling interface twice so could have an impact on performance.
        /// </summary>
        /// <example>
        /// <code>
        /// //Using the <see cref="SparseVector"/>.FromRow() helper
        /// var udf = Functions.Udf&lt;Row, double&gt;(udfRow =>
        /// {
        ///     SparseVector udfVector = SparseVector.FromRow(udfRow);
        ///     return udfVector.Apply(1000);
        /// });
        /// </code>
        /// </example>
        /// <example>
        /// <code>
        /// // Instead of passing the vector back over Apache Spark you can work with the underlying
        /// //<see cref="SparseVector"/> types directly.
        /// var udf = Functions.Udf&lt;Row, double&gt;(udfRow =>
        /// {
        ///     int type = udfRow.GetAs&lt;int&gt;(0);
        ///     int length = udfRow.GetAs&lt;int&gt;(1);
        ///     int[] indices = GetAs&lt;int[]&gt;(2);
        ///     double[] values = GetAs&lt;double[]&gt;(3);
        ///     
        ///     for(int i=0;i&lt;indices.Length;i++){
        ///         if(indices[i] == 1000){
        ///             return values[i];
        ///         }
        ///     }
        ///     
        ///     return 0.0;
        /// });
        /// </code>
        /// </example>
        /// <param name="row">Row which contains a raw <see cref="SparseVector"/></param>
        /// <returns><see cref="SparseVector"/></returns>
        public static SparseVector FromRow(Row row)
        {
            if (row.Values.Length != 4)
            {
                throw new ArgumentException(
                    $"SparseVector expected 4 items but received {row.Values.Length}");
            }

            int type = row.GetAs<int>(0);

            if (type != 0)
            {
                new ArgumentException($"SparseVector expected type 0 but received: {type}");
            }

            int length = row.GetAs<int>(1);
            int[] indices = (int[])row.GetAs<ArrayList>(2).ToArray(typeof(int));
            double[] values = (double[])row.GetAs<ArrayList>(3).ToArray(typeof(double));

            return new SparseVector(length, indices, values);
        }
    }



}
