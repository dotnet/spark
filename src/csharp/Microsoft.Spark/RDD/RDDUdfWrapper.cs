// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;

namespace Microsoft.Spark.RDD
{
    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    [UdfWrapper]
    internal class RDDUdfWrapper
    {
        private readonly Func<int, IEnumerable<object>, IEnumerable<object>> _func;

        internal RDDUdfWrapper(Func<int, IEnumerable<object>, IEnumerable<object>> func)
        {
            _func = func;
        }

        internal IEnumerable<object> Execute(int splitId, IEnumerable<object> input)
        {
            return _func(splitId, input);
        }
    }
}
