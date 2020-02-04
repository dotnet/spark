// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Razorvine.Pickle;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Custom pickler for Row objects.
    /// </summary>
    internal class RowPickler : IObjectPickler
    {
        public void pickle(object o, Stream outs, Pickler currentPickler)
        {
            currentPickler.save(((Row)o).Values);
        }
    }
}
