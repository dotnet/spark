// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using static Microsoft.Spark.Utils.CommandSerDe;

namespace Microsoft.Spark.RDD
{
    /// <summary>
    /// Collector collects objects from a socket.
    /// </summary>
    internal sealed class Collector
    {
        /// <summary>
        /// Collects pickled row objects from the given socket.
        /// </summary>
        /// <param name="stream">Stream object to read from</param>
        /// <param name="serializedMode">Serialized mode for each element</param>
        /// <returns>Collection of row objects</returns>
        public IEnumerable<object> Collect(Stream stream, SerializedMode serializedMode)
        {
            IDeserializer deserializer = GetDeserializer(serializedMode);

            int? length;
            while (((length = SerDe.ReadBytesLength(stream)) != null)
                && (length.GetValueOrDefault() > 0))
            {
                yield return deserializer.Deserialize(stream, length.GetValueOrDefault());
            }
        }

        /// <summary>
        /// Returns a deserializer based on the given serialization mode.
        /// </summary>
        /// <param name="mode">Serialization mode</param>
        /// <returns>A deserializer object</returns>
        internal static IDeserializer GetDeserializer(SerializedMode mode)
        {
            switch (mode)
            {
                case SerializedMode.Byte:
                    return new BinaryDeserializer();
                case SerializedMode.String:
                    return new StringDeserializer();
                case SerializedMode.Row:
                    return new RowDeserializer();
                default:
                    throw new ArgumentException($"Unsupported mode found {mode}");
            }
        }

        /// <summary>
        /// Interface to deserialize an object from a given stream.
        /// </summary>
        internal interface IDeserializer
        {
            object Deserialize(Stream stream, int length);
        }

        /// <summary>
        /// Deserializer using the BinaryFormatter.
        /// </summary>
        private sealed class BinaryDeserializer : IDeserializer
        {
            private readonly BinaryFormatter _formater = new BinaryFormatter();

            public object Deserialize(Stream stream, int length)
            {
                return _formater.Deserialize(stream);
            }
        }

        /// <summary>
        /// Deserializer for UTF-8 strings.
        /// </summary>
        private sealed class StringDeserializer : IDeserializer
        {
            public object Deserialize(Stream stream, int length)
            {
                return SerDe.ReadString(stream, length);
            }
        }

        /// <summary>
        /// Deserializer for Pickled Rows.
        /// </summary>
        private sealed class RowDeserializer : IDeserializer
        {
            public object Deserialize(Stream stream, int length)
            {
                // Refer to the AutoBatchedPickler class in spark/core/src/main/scala/org/apache/
                // spark/api/python/SerDeUtil.scala regarding how the Rows may be batched.
                return PythonSerDe.GetUnpickledObjects(stream, length).Cast<Row>().ToArray();
            }
        }
    }
}
