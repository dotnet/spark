// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// 
    /// </summary>
    internal sealed class ArrowGroupedMapUdfWrapper
    {
        private readonly Func<RecordBatch, RecordBatch> _func;

        internal ArrowGroupedMapUdfWrapper(Func<RecordBatch, RecordBatch> func)
        {
            _func = func;
        }

        internal RecordBatch Execute(RecordBatch input)
        {
            return _func(input);
        }
    }
}
