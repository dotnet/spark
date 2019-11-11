// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Wraps the given Func object, which represents a Grouped Map UDF.
    /// </summary>
    /// <remarks>
    /// UDF serialization requires a "wrapper" object in order to serialize/deserialize.
    /// </remarks>
    internal sealed class ArrowGroupedMapUdfWrapper
    {
        private readonly Func<FxDataFrame, FxDataFrame> _func;

        internal ArrowGroupedMapUdfWrapper(Func<FxDataFrame, FxDataFrame> func)
        {
            _func = func;
        }

        internal FxDataFrame Execute(FxDataFrame input)
        {
            return _func(input);
        }
    }
}
