// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
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
            while (((length = SerDe.ReadBytesLength(inputStream)) != null) &&
                (length.GetValueOrDefault() > 0))
            {
                object[] unpickledObjects =
                    PythonSerDe.GetUnpickledObjects(inputStream, length.GetValueOrDefault());

                foreach (object unpickled in unpickledObjects)
                {
                    yield return (unpickled as RowConstructor).GetRow();
                }
            }
        }

        /// <summary>
        /// Synchronously collects pickled row objects from the given socket.
        /// </summary>
        /// <param name="socket">Socket the get the stream from</param>
        /// <param name="server">The jvm socket auth server</param>
        /// <returns>Collection of row objects</returns>
        public IEnumerable<Row> SynchronousCollect(ISocketWrapper socket, JvmObjectReference server) =>
            new SynchronousRowCollector(socket, server);

        /// <summary>
        /// SynchronousRowCollector synchronously collects Row objects from a socket.
        /// </summary>
        private class SynchronousRowCollector : IEnumerable<Row>
        {
            private readonly ISocketWrapper _socket;
            private readonly Stream _inputStream;
            private readonly Stream _outputStream;
            private readonly JvmObjectReference _server;

            private int _readStatus = 1;
            private IEnumerable<Row> _collectEnumerable = null;

            internal SynchronousRowCollector(ISocketWrapper socket, JvmObjectReference server)
            {
                _socket = socket;
                _inputStream = socket.InputStream;
                _outputStream = socket.OutputStream;
                _server = server;
            }

            ~SynchronousRowCollector()
            {
                // If iterator is not fully consumed
                if ((_readStatus == 1) && (_collectEnumerable != null))
                {
                    // Finish consuming partition data stream
                    foreach (Row _ in _collectEnumerable)
                    {
                    }

                    // Tell Java to stop sending data and close connection
                    SerDe.Write(_outputStream, 0);
                    _outputStream.Flush();
                }
            }

            public IEnumerator<Row> GetEnumerator()
            {
                while (_readStatus == 1)
                {
                    // Request next partition data from Java
                    SerDe.Write(_outputStream, 1);
                    _outputStream.Flush();

                    // If response is 1 then there is a partition to read, if 0 then fully consumed
                    _readStatus = SerDe.ReadInt32(_inputStream);
                    if (_readStatus == 1)
                    {
                        // Load the partition data from stream and read each item
                        _collectEnumerable = new RowCollector().Collect(_socket);
                        foreach (Row row in _collectEnumerable)
                        {
                            yield return row;
                        }
                    }
                    else if (_readStatus == -1)
                    {
                        // An error occurred, join serving thread and raise any exceptions from the JVM
                        _server.Invoke("getResult");
                    }
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}
