// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Microsoft.Spark.Sql.Types;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Extension methods for <see cref="RelationalGroupedDataset"/>.
    /// </summary>
    public static class RelationalGroupedDatasetExtensions
    {
        /// <summary>
        /// Maps each group of the current DataFrame using a UDF and
        /// returns the result as a DataFrame.
        /// 
        /// The user-defined function should take an <see cref="FxDataFrame"/>
        /// and return another <see cref="FxDataFrame"/>. For each group, all
        /// columns are passed together as an <see cref="FxDataFrame"/> to the user-function and
        /// the returned FxDataFrame are combined as a DataFrame.
        ///
        /// The returned <see cref="FxDataFrame"/> can be of arbitrary length and its schema must match
        /// <paramref name="returnType"/>.
        /// </summary>
        /// <param name="dataset">
        /// The <see cref="RelationalGroupedDataset"/> containing grouped data.
        /// </param>
        /// <param name="returnType">
        /// The <see cref="StructType"/> that represents the shape of the return data set.
        /// </param>
        /// <param name="func">A grouped map user-defined function.</param>
        /// <returns>New DataFrame object with the UDF applied.</returns>
        public static DataFrame Apply(
            this RelationalGroupedDataset dataset,
            StructType returnType, 
            Func<FxDataFrame, FxDataFrame> func)
        {
            return dataset.Apply(returnType, func);
        }

        /// <summary>
        /// Maps each group of the current DataFrame using a UDF and
        /// returns the result as a DataFrame.
        /// 
        /// The user-defined function should take an Apache Arrow RecordBatch
        /// and return another Apache Arrow RecordBatch. For each group, all
        /// columns are passed together as a RecordBatch to the user-function and
        /// the returned RecordBatch are combined as a DataFrame.
        ///
        /// The returned RecordBatch can be of arbitrary length and its schema must match
        /// <paramref name="returnType"/>.
        /// </summary>
        /// <param name="dataset">
        /// The <see cref="RelationalGroupedDataset"/> containing grouped data.
        /// </param>
        /// <param name="returnType">
        /// The <see cref="StructType"/> that represents the shape of the return data set.
        /// </param>
        /// <param name="func">A grouped map user-defined function.</param>
        /// <returns>New DataFrame object with the UDF applied.</returns>
        public static DataFrame Apply(
            this RelationalGroupedDataset dataset,
            StructType returnType, 
            Func<RecordBatch, RecordBatch> func)
        {
            return dataset.Apply(returnType, func);
        }
    }
}
