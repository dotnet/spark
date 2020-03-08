// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.Sql.Types;
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

    /// <summary>
    /// Custom pickler for GenericRow objects.
    /// </summary>
    internal class GenericRowPickler : IObjectPickler
    {
        public void pickle(object o, Stream outs, Pickler currentPickler)
        {
            currentPickler.save(((GenericRow)o).Values);
        }
    }

    /// <summary>
    /// Custom pickler for Date objects.
    /// </summary>
    internal class DatePickler : IObjectPickler
    {
        public void pickle(object o, Stream outs, Pickler currentPickler)
        {
            currentPickler.save(((Date)o).GetInterval());
        }
    }

    /// <summary>
    /// Custom pickler for Timestamp objects.
    /// </summary>
    internal class TimestampPickler : IObjectPickler
    {
        public void pickle(object o, Stream outs, Pickler currentPickler)
        {
            currentPickler.save(((Timestamp)o).GetInterval());
        }
    }
}
