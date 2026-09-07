// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Diagnostics;
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
        /// <param name="socket">Socket the get the stream from.</param>
        /// <returns>Collection of row objects.</returns>
        public IEnumerable<Row> Collect(ISocketWrapper socket)
        {
            Stream inputStream = socket.InputStream;

            int? length;
            while (((length = SerDe.ReadBytesLength(inputStream)) != null) &&
                (length.GetValueOrDefault() > 0))
            {
                object[] unpickledObjects =
                    PythonSerDe.GetUnpickledObjects(inputStream, length.GetValueOrDefault());

                foreach (object unpickled in unpickledObjects)
                {
                    yield return unpickled as Row;
                }
            }
        }

        /// <summary>
        /// Collects pickled row objects from the given socket. Collects rows in partitions
        /// by leveraging <see cref="Collect(ISocketWrapper)"/>.
        /// </summary>
        /// <param name="socket">Socket the get the stream from.</param>
        /// <param name="server">The JVM socket auth server.</param>
        /// <returns>Collection of row objects.</returns>
        public IEnumerable<Row> Collect(ISocketWrapper socket, JvmObjectReference server)
        {
            // This follows PySpark's _local_iterator_from_socket protocol.
            Stream inputStream = socket.InputStream;
            Stream outputStream = socket.OutputStream;

            while (true)
            {
                // Request the next partition. Response 0 means fully consumed;
                // -1 means the JVM failed while collecting the partition.
                SerDe.Write(outputStream, 1);
                outputStream.Flush();
                int readStatus = SerDe.ReadInt32(inputStream);
                if (readStatus != 1)
                {
                    if (readStatus == -1)
                    {
                        server.Invoke("getResult");
                    }
                    else
                    {
                        Debug.Assert(readStatus == 0);
                    }

                    yield break;
                }

                using IEnumerator<Row> partitionRows = Collect(socket).GetEnumerator();
                bool partitionConsumed = false;
                try
                {
                    while (partitionRows.MoveNext())
                    {
                        yield return partitionRows.Current;
                    }

                    partitionConsumed = true;
                }
                finally
                {
                    if (!partitionConsumed)
                    {
                        try
                        {
                            // Java writes the whole partition before reading the next request.
                            // Drain it before sending stop, while the caller still owns the socket.
                            while (partitionRows.MoveNext())
                            {
                            }

                            SerDe.Write(outputStream, 0);
                            outputStream.Flush();
                        }
                        catch
                        {
                            // Do not mask an iteration failure. The caller closes the socket
                            // even if the connection no longer permits a graceful stop.
                        }
                    }
                }
            }
        }
    }
}
