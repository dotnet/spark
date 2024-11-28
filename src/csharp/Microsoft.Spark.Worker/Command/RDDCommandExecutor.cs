// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// CommandExecutor reads input data from the input stream,
    /// runs commands on them, and writes result to the output stream.
    /// </summary>
    internal class RDDCommandExecutor
    {
        [ThreadStatic]
        private static MemoryStream s_writeOutputStream;
        [ThreadStatic]
#pragma warning disable SYSLIB0011 // Type or member is obsolete
        private static BinaryFormatter s_binaryFormatter;
#pragma warning restore SYSLIB0011 // Type or member is obsolete

        /// <summary>
        /// Executes the commands on the input data read from input stream
        /// and writes results to the output stream.
        /// </summary>
        /// <param name="inputStream">Input stream to read data from</param>
        /// <param name="outputStream">Output stream to write results to</param>
        /// <param name="splitIndex">Split index for this task</param>
        /// <param name="command">Contains the commands to execute</param>
        /// <returns>Statistics captured during the Execute() run</returns>
        internal CommandExecutorStat Execute(
            Stream inputStream,
            Stream outputStream,
            int splitIndex,
            RDDCommand command)
        {
            var stat = new CommandExecutorStat();

            CommandSerDe.SerializedMode serializerMode = command.SerializerMode;
            CommandSerDe.SerializedMode deserializerMode = command.DeserializerMode;

            RDD.WorkerFunction.ExecuteDelegate func = command.WorkerFunction.Func;
            foreach (object output in func(
                splitIndex,
                GetInputIterator(inputStream, deserializerMode)))
            {
                WriteOutput(outputStream, serializerMode, output);

                ++stat.NumEntriesProcessed;
            }

            return stat;
        }

        /// <summary>
        /// Create input iterator from the given input stream.
        /// </summary>
        /// <param name="inputStream">Stream to read from</param>
        /// <param name="deserializerMode">Mode for deserialization</param>
        /// <returns></returns>
        private IEnumerable<object> GetInputIterator(
            Stream inputStream,
            CommandSerDe.SerializedMode deserializerMode)
        {
            RDD.Collector.IDeserializer deserializer =
                RDD.Collector.GetDeserializer(deserializerMode);

            int messageLength;
            while ((messageLength = SerDe.ReadInt32(inputStream)) !=
                (int)SpecialLengths.END_OF_DATA_SECTION)
            {
                if ((messageLength > 0) || (messageLength == (int)SpecialLengths.NULL))
                {
                    yield return deserializer.Deserialize(inputStream, messageLength);
                }
            }
        }

        /// <summary>
        /// Writes the given message to the stream.
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        /// <param name="serializerMode">Mode for serialization</param>
        /// <param name="message">Message to write to</param>
        private void WriteOutput(
            Stream stream,
            CommandSerDe.SerializedMode serializerMode,
            object message)
        {
            MemoryStream writeOutputStream = s_writeOutputStream ??= new MemoryStream();
            writeOutputStream.Position = 0;
            Serialize(serializerMode, message, writeOutputStream);
            SerDe.Write(stream, (int)writeOutputStream.Position);
            SerDe.Write(stream, writeOutputStream.GetBuffer(), (int)writeOutputStream.Position);
        }

        /// <summary>
        /// Serialize a row based on the given serializer mode.
        /// </summary>
        /// <param name="serializerMode"></param>
        /// <param name="message"></param>
        /// <param name="stream"></param>
        private void Serialize(
            CommandSerDe.SerializedMode serializerMode,
            object message,
            MemoryStream stream)
        {
            switch (serializerMode)
            {
                case CommandSerDe.SerializedMode.Byte:
#pragma warning disable SYSLIB0011 // Type or member is obsolete
                    BinaryFormatter formatter = s_binaryFormatter ??= new BinaryFormatter();
                    // TODO: Replace BinaryFormatter with a new, secure serializer.
                    formatter.Serialize(stream, message);
#pragma warning restore SYSLIB0011 // Type or member is obsolete
                    break;
                case CommandSerDe.SerializedMode.None:
                case CommandSerDe.SerializedMode.String:
                case CommandSerDe.SerializedMode.Pair:
                default:
                    throw new NotImplementedException(
                        $"Unsupported serializerMode: {serializerMode}");
            }
        }
    }
}
