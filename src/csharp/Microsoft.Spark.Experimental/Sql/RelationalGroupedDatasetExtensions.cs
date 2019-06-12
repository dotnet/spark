// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Extension methods for <see cref="RelationalGroupedDataset"/>.
    /// </summary>
    public static class RelationalGroupedDatasetExtensions
    {
        public static DataFrame Apply(
            this RelationalGroupedDataset dataset,
            StructType returnType, 
            Func<RecordBatch, RecordBatch> func)
        {
            return dataset.Apply(returnType, func);
        }
    }
}
