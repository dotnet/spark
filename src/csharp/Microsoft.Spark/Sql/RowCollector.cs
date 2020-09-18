// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
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
        public IEnumerable<Row> Collect(ISocketWrapper socket, JvmObjectReference server) =>
            new LocalIteratorFromSocket(socket, server);

        /// <summary>
        /// LocalIteratorFromSocket creates a synchronous local iterable over
        /// a socket.
        /// 
        /// Note that the implementation mirrors _local_iterator_from_socket in
        /// PySpark: spark/python/pyspark/rdd.py
        /// </summary>
        private class LocalIteratorFromSocket : IEnumerable<Row>
        {
            private readonly ISocketWrapper _socket;
            private readonly JvmObjectReference _server;

            private int _readStatus = 1;
            private IEnumerable<Row> _currentPartitionRows = null;

            internal LocalIteratorFromSocket(ISocketWrapper socket, JvmObjectReference server)
            {
                _socket = socket;
                _server = server;
            }

            ~LocalIteratorFromSocket()
            {
                // If iterator is not fully consumed.
                if ((_readStatus == 1) && (_currentPartitionRows != null))
                {
                    try
                    {
                        // Finish consuming partition data stream.
                        foreach (Row _ in _currentPartitionRows)
                        {
                        }

                        // Tell Java to stop sending data and close connection.
                        Stream outputStream = _socket.OutputStream;
                        SerDe.Write(outputStream, 0);
                        outputStream.Flush();
                    }
                    catch
                    {
                        // Ignore any errors, socket may be automatically closed
                        // when garbage-collected.
                    }
                }
            }

            public IEnumerator<Row> GetEnumerator()
            {
                Stream inputStream = _socket.InputStream;
                Stream outputStream = _socket.OutputStream;

                while (_readStatus == 1)
                {
                    // Request next partition data from Java.
                    SerDe.Write(outputStream, 1);
                    outputStream.Flush();

                    // If response is 1 then there is a partition to read, if 0 then
                    // fully consumed.
                    _readStatus = SerDe.ReadInt32(inputStream);
                    if (_readStatus == 1)
                    {
                        // Load the partition data from stream and read each item.
                        _currentPartitionRows = new RowCollector().Collect(_socket);
                        foreach (Row row in _currentPartitionRows)
                        {
                            yield return row;
                        }
                    }
                    else if (_readStatus == -1)
                    {
                        // An error occurred, join serving thread and raise any exceptions from
                        // the JVM. The exception stack trace will appear in the driver logs.
                        _server.Invoke("getResult");
                    }
                    else
                    {
                        Debug.Assert(_readStatus == 0);
                    }
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}
