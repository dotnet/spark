// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// RowCollector collects Row objects from a socket.
    /// </summary>
    internal sealed class RowCollector
    {
        /// <summary>
        /// Collects pickled row objects from the given socket.
        /// </summary>
        /// <param name="socket">Socket the get the stream from</param>
        /// <returns>Collection of row objects</returns>
        public IEnumerable<Row> Collect(ISocketWrapper socket)
        {
            Stream inputStream = socket.InputStream;

            int? length;
            while (((length = SerDe.ReadBytesLength(inputStream)) != null) && (length.GetValueOrDefault() > 0))
            {
                object[] unpickledObjects = PythonSerDe.GetUnpickledObjects(inputStream, length.Value);

                foreach (object unpickled in unpickledObjects)
                {
                    yield return (unpickled as RowConstructor).GetRow();
                }
            }
        }
    }
}
