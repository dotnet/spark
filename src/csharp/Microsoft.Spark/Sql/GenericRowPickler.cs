// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Razorvine.Pickle;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Custom pickler for GenericRow objects.
    /// </summary>
    internal class GenericRowPickler : IObjectPickler
    {
        public void pickle(object o, Stream outs, Pickler currentPickler)
        {
            GenericRow row = (GenericRow)o;
            currentPickler.save(row.Values);
        }
    }
}
