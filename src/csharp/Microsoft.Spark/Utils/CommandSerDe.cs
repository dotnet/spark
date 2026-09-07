// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using MessagePack;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// CommandSerDe provides functionality to serialize/deserialize WorkerFunction
    /// along with other information.
    /// </summary>
    internal static class CommandSerDe
    {
        internal enum SerializedMode
        {
            None,
            String,
            Byte,
            Pair,
            Row
        }

        /// <summary>
        /// The function name of any UDF wrappers that wrap the UDF.
        /// ex) <see cref="RDD{T}.MapUdfWrapper{I, O}.Execute(int, IEnumerable{object})"/>
        /// </summary>
        private const string UdfWrapperMethodName = "Execute";

        private const sbyte TypelessExtensionCode = 100;
        private const int MaxSpark40TypeNameBytes = 4096;
        private const int MaxSpark40MessagePackDepth = 500;

        private static readonly UTF8Encoding s_strictUtf8 =
            new UTF8Encoding(false, true);

        /// <summary>
        /// Captures the information about the UDF wrapper.
        /// Example classes for wrapping UDF are:
        ///  - SQL: * <see cref="ArrowUdfWrapper{T, TResult}"/>
        ///         * <see cref="PicklingUdfWrapper{TResult}"/>
        ///  - RDD: * <see cref="RDD{T}.MapUdfWrapper{I, O}"/>
        ///         * <see cref="RDD{T}.FlatMapUdfWrapper{I, O}"/>
        ///         * <see cref="RDD{T}.MapPartitionsUdfWrapper{I, O}"/>
        ///         * <see cref="RDD.WorkerFunction.WorkerFuncChainHelper"/>
        /// </summary>
        [Serializable]
        private sealed class UdfWrapperNode
        {
            /// <summary>
            /// Type name of the UDF wrapper.
            /// </summary>
            internal string TypeName { get; set; }

            /// <summary>
            /// Number of children (UDF wrapper or UDF) this node is associated with.
            /// Note that there can be up to two children and if the child is an UDF,
            /// this will be set to one.
            /// </summary>
            internal int NumChildren { get; set; }

            /// <summary>
            /// True if the child is an UDF.
            /// </summary>
            internal bool HasUdf { get; set; }
        }

        /// <summary>
        /// UdfWrapperData represents the flattened tree structure.
        /// For example:
        ///                        WorkerChainHelper#1
        ///                         /                \
        ///           WorkerChainHelper#2        MapUdfWrapper#3
        ///               /          \                  \
        ///  MapUdfWrapper#1   MapUdfWrapper#2         UDF#3
        ///         |                 |
        ///       UDF#1             UDF#2
        /// 
        /// will be translated into:
        /// UdfWrapperNodes: (WorkerChainHelper(WCH), MapUdfWrapper(MUW))
        ///    [ WCH#1(2, false), WCH#2(2, false), MUW#1(1, true), MUW#2(1, true), MUW#3(1, true) ]
        ///    where WCH#1(2, false) means the node has two children and HasUdf is false.
        /// Udfs:
        ///    [ UDF#1, UDF#2, UDF#3 ]
        /// 
        /// </summary>
        [Serializable]
        private sealed class UdfWrapperData
        {
            /// <summary>
            /// Flattened UDF wrapper nodes.
            /// </summary>
            internal UdfWrapperNode[] UdfWrapperNodes { get; set; }

            /// <summary>
            /// Serialized UDF data.
            /// </summary>
            internal UdfSerDe.UdfData[] Udfs { get; set; }
        }

        internal static byte[] Serialize(
            Delegate func,
            SerializedMode deserializerMode = SerializedMode.Byte,
            SerializedMode serializerMode = SerializedMode.Byte)
        {
            // TODO: Rework on the following List<Byte[]> to use MemoryStream!

            var commandPayloadBytesList = new List<byte[]>();

            // Add serializer mode.
            byte[] modeBytes = Encoding.UTF8.GetBytes(serializerMode.ToString());
            int length = modeBytes.Length;
            byte[] lengthAsBytes = BitConverter.GetBytes(length);
            Array.Reverse(lengthAsBytes);
            commandPayloadBytesList.Add(lengthAsBytes);
            commandPayloadBytesList.Add(modeBytes);

            // Add deserializer mode.
            modeBytes = Encoding.UTF8.GetBytes(deserializerMode.ToString());
            length = modeBytes.Length;
            lengthAsBytes = BitConverter.GetBytes(length);
            Array.Reverse(lengthAsBytes);
            commandPayloadBytesList.Add(lengthAsBytes);
            commandPayloadBytesList.Add(modeBytes);

            // Add run mode:
            // N - normal
            // R - repl
            string runMode = Environment.GetEnvironmentVariable("SPARK_NET_RUN_MODE") ?? "N";
            byte[] runModeBytes = Encoding.UTF8.GetBytes(runMode);
            lengthAsBytes = BitConverter.GetBytes(runModeBytes.Length);
            Array.Reverse(lengthAsBytes);
            commandPayloadBytesList.Add(lengthAsBytes);
            commandPayloadBytesList.Add(runModeBytes);

            if ("R".Equals(runMode, StringComparison.InvariantCultureIgnoreCase))
            {
                // add compilation dump directory
                byte[] compilationDumpDirBytes = Encoding.UTF8.GetBytes(
                    Environment.GetEnvironmentVariable("SPARK_NET_SCRIPT_COMPILATION_DIR") ?? ".");
                lengthAsBytes = BitConverter.GetBytes(compilationDumpDirBytes.Length);
                Array.Reverse(lengthAsBytes);
                commandPayloadBytesList.Add(lengthAsBytes);
                commandPayloadBytesList.Add(compilationDumpDirBytes);
            }

            // Serialize the UDFs.
            var udfWrapperNodes = new List<UdfWrapperNode>();
            var udfs = new List<UdfSerDe.UdfData>();
            SerializeUdfs(func, null, udfWrapperNodes, udfs);

            // Run through UdfSerDe.Serialize once more to get serialization info
            // on the actual UDF.
            var udfWrapperData = new UdfWrapperData()
            {
                UdfWrapperNodes = udfWrapperNodes.ToArray(),
                Udfs = udfs.ToArray()
            };

            using (var stream = new MemoryStream())
            {
                BinarySerDe.Serialize(stream, udfWrapperData);

                byte[] udfBytes = stream.ToArray();
                byte[] udfBytesLengthAsBytes = BitConverter.GetBytes(udfBytes.Length);
                Array.Reverse(udfBytesLengthAsBytes);
                commandPayloadBytesList.Add(udfBytesLengthAsBytes);
                commandPayloadBytesList.Add(udfBytes);
            }

            return commandPayloadBytesList.SelectMany(byteArray => byteArray).ToArray();
        }

        internal static void PreflightSpark40(byte[] command, int expectedArity)
        {
            ArraySegment<byte> serializedUdf = ParseSpark40Envelope(command);
            try
            {
                var reader = new MessagePackReader(
                    new ReadOnlyMemory<byte>(
                        serializedUdf.Array,
                        serializedUdf.Offset,
                        serializedUdf.Count));
                string rootTypeName = ReadSpark40RootTypelessExtension(
                    ref reader,
                    out string wrapperTypeName);
                if (!reader.End || !IsExpectedSpark40RootTypeName(rootTypeName))
                {
                    throw new InvalidDataException(
                        "Invalid Spark 4 serialized UDF root.");
                }

                ValidateSpark40WrapperArityName(
                    wrapperTypeName,
                    expectedArity);
            }
            catch (InvalidDataException)
            {
                throw;
            }
            catch (Exception ex) when (
                ex is EndOfStreamException ||
                ex is MessagePackSerializationException ||
                ex is DecoderFallbackException ||
                ex is InsufficientExecutionStackException)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 serialized UDF payload.",
                    ex);
            }
        }

        internal static T DeserializeSpark40<T>(
            byte[] command,
            int expectedArity,
            out SerializedMode serializerMode,
            out SerializedMode deserializerMode) where T : Delegate
        {
            ArraySegment<byte> serializedUdf = ParseSpark40Envelope(command);
            UdfWrapperData udfWrapperData;
            try
            {
                using var stream = new MemoryStream(
                    serializedUdf.Array,
                    serializedUdf.Offset,
                    serializedUdf.Count,
                    writable: false);
                udfWrapperData = BinarySerDe.Deserialize<UdfWrapperData>(stream);
                if (stream.Position != stream.Length)
                {
                    throw new InvalidDataException(
                        "Spark 4 serialized UDF was not fully consumed.");
                }
            }
            catch (InvalidDataException)
            {
                throw;
            }
            catch (Exception ex) when (
                ex is EndOfStreamException ||
                ex is MessagePackSerializationException ||
                ex is InvalidCastException)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 serialized UDF payload.",
                    ex);
            }

            ValidateSpark40WrapperArity(udfWrapperData, expectedArity);

            int nodeIndex = 0;
            int udfIndex = 0;
            T udf = (T)DeserializeUdfs<T>(
                udfWrapperData,
                ref nodeIndex,
                ref udfIndex);
            if (nodeIndex != udfWrapperData.UdfWrapperNodes.Length ||
                udfIndex != udfWrapperData.Udfs.Length)
            {
                throw new InvalidDataException(
                    "Spark 4 serialized UDF contains trailing wrapper data.");
            }

            serializerMode = SerializedMode.Row;
            deserializerMode = SerializedMode.Row;
            return udf;
        }

        private static ArraySegment<byte> ParseSpark40Envelope(byte[] command)
        {
            if (command == null)
            {
                throw new ArgumentNullException(nameof(command));
            }

            int offset = 0;
            ReadExpectedAscii(command, ref offset, "Row", "serializer");
            ReadExpectedAscii(command, ref offset, "Row", "deserializer");

            int runModeLength = ReadSpark40Int32(command, ref offset);
            if (runModeLength != 1 || command.Length - offset < runModeLength)
            {
                throw new InvalidDataException("Invalid Spark 4 run mode.");
            }

            byte runMode = command[offset++];
            if (runMode == (byte)'R')
            {
                throw new NotSupportedException(
                    "Spark 4 REPL commands are not supported.");
            }

            if (runMode != (byte)'N')
            {
                throw new InvalidDataException("Invalid Spark 4 run mode.");
            }

            int serializedUdfLength = ReadSpark40Int32(command, ref offset);
            if (serializedUdfLength <= 0 ||
                serializedUdfLength != command.Length - offset)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 serialized UDF length.");
            }

            return new ArraySegment<byte>(command, offset, serializedUdfLength);
        }

        private static void ReadExpectedAscii(
            byte[] command,
            ref int offset,
            string expected,
            string fieldName)
        {
            int length = ReadSpark40Int32(command, ref offset);
            if (length != expected.Length || command.Length - offset < length)
            {
                throw new InvalidDataException(
                    $"Invalid Spark 4 {fieldName} mode.");
            }

            for (int i = 0; i < expected.Length; ++i)
            {
                if (command[offset + i] != (byte)expected[i])
                {
                    throw new InvalidDataException(
                        $"Invalid Spark 4 {fieldName} mode.");
                }
            }

            offset += length;
        }

        private static int ReadSpark40Int32(byte[] command, ref int offset)
        {
            if (offset < 0 || command.Length - offset < sizeof(int))
            {
                throw new InvalidDataException(
                    "Incomplete Spark 4 command envelope.");
            }

            int value = BinaryPrimitives.ReadInt32BigEndian(
                command.AsSpan(offset, sizeof(int)));
            offset += sizeof(int);
            return value;
        }

        private static string ReadTypelessExtension(
            ref MessagePackReader reader,
            int depth)
        {
            if (depth > MaxSpark40MessagePackDepth ||
                reader.NextMessagePackType != MessagePackType.Extension)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 typeless extension.");
            }

            ExtensionResult extension = reader.ReadExtensionFormat();
            if (extension.TypeCode != TypelessExtensionCode)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 typeless extension.");
            }

            var extensionReader = new MessagePackReader(extension.Data);
            string typeName = ReadStrictTypeName(ref extensionReader);
            ScanMessagePackValue(ref extensionReader, depth + 1);
            if (!extensionReader.End)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 typeless extension payload.");
            }

            return typeName;
        }

        private static string ReadSpark40RootTypelessExtension(
            ref MessagePackReader reader,
            out string wrapperTypeName)
        {
            if (reader.NextMessagePackType != MessagePackType.Extension)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 serialized UDF root.");
            }

            ExtensionResult extension = reader.ReadExtensionFormat();
            if (extension.TypeCode != TypelessExtensionCode)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 serialized UDF root.");
            }

            var extensionReader = new MessagePackReader(extension.Data);
            string typeName = ReadStrictTypeName(ref extensionReader);
            wrapperTypeName = ReadSpark40RootWrapperTypeName(
                ref extensionReader,
                depth: 1);
            if (!extensionReader.End)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 serialized UDF root payload.");
            }

            return typeName;
        }

        private static string ReadSpark40RootWrapperTypeName(
            ref MessagePackReader reader,
            int depth)
        {
            if (depth > MaxSpark40MessagePackDepth)
            {
                throw new InvalidDataException(
                    "Spark 4 serialized UDF is too deeply nested.");
            }

            if (reader.NextMessagePackType == MessagePackType.Map)
            {
                int count = reader.ReadMapHeader();
                string wrapperTypeName = null;
                for (int i = 0; i < count; ++i)
                {
                    string key = ReadStrictMessagePackString(
                        ref reader,
                        MaxSpark40TypeNameBytes,
                        "Spark 4 serialized UDF property");
                    if (key == "UdfWrapperNodes")
                    {
                        if (wrapperTypeName != null)
                        {
                            throw new InvalidDataException(
                                "Duplicate Spark 4 UDF wrapper data.");
                        }

                        wrapperTypeName = ReadFirstSpark40WrapperTypeName(
                            ref reader,
                            depth + 1);
                    }
                    else
                    {
                        ScanMessagePackValue(ref reader, depth + 1);
                    }
                }

                return wrapperTypeName ?? throw new InvalidDataException(
                    "Missing Spark 4 UDF wrapper data.");
            }

            if (reader.NextMessagePackType == MessagePackType.Array)
            {
                int count = reader.ReadArrayHeader();
                if (count < 1)
                {
                    throw new InvalidDataException(
                        "Missing Spark 4 UDF wrapper data.");
                }

                string wrapperTypeName = ReadFirstSpark40WrapperTypeName(
                    ref reader,
                    depth + 1);
                for (int i = 1; i < count; ++i)
                {
                    ScanMessagePackValue(ref reader, depth + 1);
                }

                return wrapperTypeName;
            }

            throw new InvalidDataException(
                "Invalid Spark 4 serialized UDF root data.");
        }

        private static string ReadFirstSpark40WrapperTypeName(
            ref MessagePackReader reader,
            int depth)
        {
            if (depth > MaxSpark40MessagePackDepth ||
                reader.NextMessagePackType != MessagePackType.Array)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper collection.");
            }

            int count = reader.ReadArrayHeader();
            if (count < 1)
            {
                throw new InvalidDataException(
                    "Missing Spark 4 UDF wrapper.");
            }

            string wrapperTypeName = ReadSpark40WrapperNodeTypeName(
                ref reader,
                depth + 1);
            for (int i = 1; i < count; ++i)
            {
                ScanMessagePackValue(ref reader, depth + 1);
            }

            return wrapperTypeName;
        }

        private static string ReadSpark40WrapperNodeTypeName(
            ref MessagePackReader reader,
            int depth)
        {
            if (depth > MaxSpark40MessagePackDepth ||
                reader.NextMessagePackType != MessagePackType.Map)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper node.");
            }

            int count = reader.ReadMapHeader();
            string typeName = null;
            for (int i = 0; i < count; ++i)
            {
                string key = ReadStrictMessagePackString(
                    ref reader,
                    MaxSpark40TypeNameBytes,
                    "Spark 4 UDF wrapper property");
                if (key == "TypeName")
                {
                    if (typeName != null)
                    {
                        throw new InvalidDataException(
                            "Duplicate Spark 4 UDF wrapper type.");
                    }

                    typeName = ReadStrictMessagePackString(
                        ref reader,
                        MaxSpark40TypeNameBytes,
                        "Spark 4 UDF wrapper type");
                }
                else
                {
                    ScanMessagePackValue(ref reader, depth + 1);
                }
            }

            return typeName ?? throw new InvalidDataException(
                "Missing Spark 4 UDF wrapper type.");
        }

        private static void ScanMessagePackValue(
            ref MessagePackReader reader,
            int depth)
        {
            if (depth > MaxSpark40MessagePackDepth)
            {
                throw new InvalidDataException(
                    "Spark 4 serialized UDF is too deeply nested.");
            }

            switch (reader.NextMessagePackType)
            {
                case MessagePackType.Array:
                    int arrayCount = reader.ReadArrayHeader();
                    for (int i = 0; i < arrayCount; ++i)
                    {
                        ScanMessagePackValue(ref reader, depth + 1);
                    }
                    break;

                case MessagePackType.Map:
                    int mapCount = reader.ReadMapHeader();
                    for (int i = 0; i < mapCount; ++i)
                    {
                        ScanMessagePackValue(ref reader, depth + 1);
                        ScanMessagePackValue(ref reader, depth + 1);
                    }
                    break;

                case MessagePackType.Extension:
                    ExtensionResult extension = reader.ReadExtensionFormat();
                    if (extension.TypeCode == TypelessExtensionCode)
                    {
                        var extensionReader = new MessagePackReader(extension.Data);
                        _ = ReadStrictTypeName(ref extensionReader);
                        ScanMessagePackValue(ref extensionReader, depth + 1);
                        if (!extensionReader.End)
                        {
                            throw new InvalidDataException(
                                "Invalid Spark 4 typeless extension payload.");
                        }
                    }
                    break;

                default:
                    reader.Skip();
                    break;
            }
        }

        private static string ReadStrictTypeName(ref MessagePackReader reader)
        {
            return ReadStrictMessagePackString(
                ref reader,
                MaxSpark40TypeNameBytes,
                "Spark 4 typeless type name");
        }

        private static string ReadStrictMessagePackString(
            ref MessagePackReader reader,
            int maximumLength,
            string fieldName)
        {
            ReadOnlySequence<byte>? sequence = reader.ReadStringSequence();
            if (!sequence.HasValue ||
                sequence.Value.Length < 1 ||
                sequence.Value.Length > maximumLength)
            {
                throw new InvalidDataException(
                    $"Invalid {fieldName}.");
            }

            var bytes = new byte[(int)sequence.Value.Length];
            int offset = 0;
            foreach (ReadOnlyMemory<byte> segment in sequence.Value)
            {
                segment.Span.CopyTo(bytes.AsSpan(offset));
                offset += segment.Length;
            }

            try
            {
                return s_strictUtf8.GetString(bytes);
            }
            catch (DecoderFallbackException ex)
            {
                throw new InvalidDataException(
                    $"Invalid {fieldName} encoding.",
                    ex);
            }
        }

        private static void ValidateSpark40WrapperArityName(
            string wrapperTypeName,
            int expectedArity)
        {
            const string Prefix = "Microsoft.Spark.Sql.PicklingUdfWrapper`";
            if (expectedArity < 0 || expectedArity > 10 ||
                wrapperTypeName == null ||
                !wrapperTypeName.StartsWith(Prefix, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper arity.");
            }

            int position = Prefix.Length;
            int genericArity = 0;
            int genericArityDigits = 0;
            while (position < wrapperTypeName.Length &&
                wrapperTypeName[position] >= '0' &&
                wrapperTypeName[position] <= '9')
            {
                if (genericArityDigits == 2)
                {
                    throw new InvalidDataException(
                        "Invalid Spark 4 UDF wrapper arity.");
                }

                genericArity = (genericArity * 10) +
                    (wrapperTypeName[position] - '0');
                ++position;
                ++genericArityDigits;
            }

            if (genericArity < 1 || genericArity > 11 ||
                genericArity - 1 != expectedArity ||
                position >= wrapperTypeName.Length ||
                wrapperTypeName[position] != '[')
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper arity.");
            }

            int bracketDepth = 0;
            int genericTypeEnd = -1;
            for (int i = position; i < wrapperTypeName.Length; ++i)
            {
                if (wrapperTypeName[i] == '[')
                {
                    ++bracketDepth;
                }
                else if (wrapperTypeName[i] == ']')
                {
                    --bracketDepth;
                    if (bracketDepth == 0)
                    {
                        genericTypeEnd = i + 1;
                        break;
                    }
                }
            }

            const string AssemblyPrefix = ", Microsoft.Spark";
            if (genericTypeEnd < 0)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper type.");
            }

            if (genericTypeEnd == wrapperTypeName.Length)
            {
                return;
            }

            if (wrapperTypeName.Length - genericTypeEnd < AssemblyPrefix.Length ||
                string.CompareOrdinal(
                    wrapperTypeName,
                    genericTypeEnd,
                    AssemblyPrefix,
                    0,
                    AssemblyPrefix.Length) != 0)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper type.");
            }

            int assemblyEnd = genericTypeEnd + AssemblyPrefix.Length;
            if (assemblyEnd < wrapperTypeName.Length &&
                wrapperTypeName[assemblyEnd] != ',')
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper type.");
            }
        }

        private static bool IsExpectedSpark40RootTypeName(string typeName)
        {
            Type expectedType = typeof(UdfWrapperData);
            string shortAssemblyQualifiedName =
                $"{expectedType.FullName}, {expectedType.Assembly.GetName().Name}";
            return string.Equals(
                    typeName,
                    expectedType.AssemblyQualifiedName,
                    StringComparison.Ordinal) ||
                string.Equals(
                    typeName,
                    shortAssemblyQualifiedName,
                    StringComparison.Ordinal);
        }

        private static void ValidateSpark40WrapperArity(
            UdfWrapperData udfWrapperData,
            int expectedArity)
        {
            if (expectedArity < 0 || expectedArity > 10 ||
                udfWrapperData?.UdfWrapperNodes == null ||
                udfWrapperData.UdfWrapperNodes.Length == 0 ||
                udfWrapperData.Udfs == null)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper data.");
            }

            string rootTypeName = udfWrapperData.UdfWrapperNodes[0].TypeName;
            Type rootType;
            try
            {
                rootType = Type.GetType(rootTypeName, throwOnError: false);
            }
            catch (Exception ex) when (
                ex is ArgumentException ||
                ex is FileLoadException ||
                ex is FileNotFoundException)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper type.",
                    ex);
            }

            if (!TryGetPicklingWrapperArity(rootType, out int actualArity) ||
                actualArity != expectedArity)
            {
                throw new InvalidDataException(
                    "Invalid Spark 4 UDF wrapper arity.");
            }
        }

        private static bool TryGetPicklingWrapperArity(
            Type type,
            out int arity)
        {
            arity = -1;
            if (type == null || !type.IsGenericType)
            {
                return false;
            }

            Type definition = type.GetGenericTypeDefinition();
            int genericArity = type.GetGenericArguments().Length;
            bool isPicklingWrapper = genericArity switch
            {
                1 => definition == typeof(PicklingUdfWrapper<>),
                2 => definition == typeof(PicklingUdfWrapper<,>),
                3 => definition == typeof(PicklingUdfWrapper<,,>),
                4 => definition == typeof(PicklingUdfWrapper<,,,>),
                5 => definition == typeof(PicklingUdfWrapper<,,,,>),
                6 => definition == typeof(PicklingUdfWrapper<,,,,,>),
                7 => definition == typeof(PicklingUdfWrapper<,,,,,,>),
                8 => definition == typeof(PicklingUdfWrapper<,,,,,,,>),
                9 => definition == typeof(PicklingUdfWrapper<,,,,,,,,>),
                10 => definition == typeof(PicklingUdfWrapper<,,,,,,,,,>),
                11 => definition == typeof(PicklingUdfWrapper<,,,,,,,,,,>),
                _ => false
            };

            if (isPicklingWrapper)
            {
                arity = genericArity - 1;
            }

            return isPicklingWrapper;
        }

        private static void SerializeUdfs(
            Delegate func,
            UdfWrapperNode parent,
            List<UdfWrapperNode> udfWrapperNodes,
            List<UdfSerDe.UdfData> udfs)
        {
            UdfSerDe.UdfData udfData = UdfSerDe.Serialize(func);
            if ((udfData.MethodName != UdfWrapperMethodName) ||
                !Attribute.IsDefined(func.Target.GetType(), typeof(UdfWrapperAttribute)))
            {
                // Found the actual UDF.
                if (parent != null)
                {
                    parent.HasUdf = true;
                    Debug.Assert(parent.NumChildren == 1);
                }

                udfs.Add(udfData);
                return;
            }

            UdfSerDe.FieldData[] fields = udfData.TargetData.Fields;
            if ((fields.Length == 0) || (fields.Length > 2))
            {
                throw new Exception(
                    $"Invalid number of children ({fields.Length}) for {udfData.TypeData.Name}");
            }

            var curNode = new UdfWrapperNode
            {
                TypeName = udfData.TypeData.Name,
                NumChildren = fields.Length,
                HasUdf = false
            };

            udfWrapperNodes.Add(curNode);

            foreach (UdfSerDe.FieldData field in fields)
            {
                SerializeUdfs((Delegate)field.Value, curNode, udfWrapperNodes, udfs);
            }
        }

        internal static object DeserializeArrowOrDataFrameUdf(
            Stream stream,
            out SerializedMode serializerMode,
            out SerializedMode deserializerMode,
            out string runMode)
        {
            UdfWrapperData udfWrapperData = GetUdfWrapperDataFromStream(
                stream,
                out serializerMode,
                out deserializerMode,
                out runMode);

            int nodeIndex = 0;
            int udfIndex = 0;
            UdfWrapperNode node = udfWrapperData.UdfWrapperNodes[nodeIndex];
            Type nodeType = Type.GetType(node.TypeName);
            Delegate udf;
            if (nodeType == typeof(DataFrameGroupedMapUdfWrapper))
            {
                udf = (DataFrameGroupedMapWorkerFunction.ExecuteDelegate)DeserializeUdfs<DataFrameGroupedMapWorkerFunction.ExecuteDelegate>(
                        udfWrapperData,
                        ref nodeIndex,
                        ref udfIndex);
            }
            else if (nodeType == typeof(DataFrameWorkerFunction) || nodeType.IsSubclassOf(typeof(DataFrameUdfWrapper)))
            {
                udf = (DataFrameWorkerFunction.ExecuteDelegate)DeserializeUdfs<DataFrameWorkerFunction.ExecuteDelegate>(
                        udfWrapperData,
                        ref nodeIndex,
                        ref udfIndex);
            }
            else if (nodeType == typeof(ArrowGroupedMapUdfWrapper))
            {
                udf = (ArrowGroupedMapWorkerFunction.ExecuteDelegate)DeserializeUdfs<ArrowGroupedMapWorkerFunction.ExecuteDelegate>(
                        udfWrapperData,
                        ref nodeIndex,
                        ref udfIndex);
            }
            else
            {
                udf = (ArrowWorkerFunction.ExecuteDelegate)
                    DeserializeUdfs<ArrowWorkerFunction.ExecuteDelegate>(
                        udfWrapperData,
                        ref nodeIndex,
                        ref udfIndex);
            }

            // Check all the data is consumed.
            Debug.Assert(nodeIndex == udfWrapperData.UdfWrapperNodes.Length);
            Debug.Assert(udfIndex == udfWrapperData.Udfs.Length);

            return udf;
        }

        private static UdfWrapperData GetUdfWrapperDataFromStream(
            Stream stream,
            out SerializedMode serializerMode,
            out SerializedMode deserializerMode,
            out string runMode)
        {
            if (!Enum.TryParse(SerDe.ReadString(stream), out serializerMode))
            {
                throw new InvalidDataException("Serializer mode is not valid.");
            }

            if (!Enum.TryParse(SerDe.ReadString(stream), out deserializerMode))
            {
                throw new InvalidDataException("Deserializer mode is not valid.");
            }

            runMode = SerDe.ReadString(stream);

            byte[] serializedCommand = SerDe.ReadBytes(stream);

            var ms = new MemoryStream(serializedCommand, false);

            return BinarySerDe.Deserialize<UdfWrapperData>(ms);
        }

        internal static T Deserialize<T>(
            Stream stream,
            out SerializedMode serializerMode,
            out SerializedMode deserializerMode,
            out string runMode) where T : Delegate
        {
            UdfWrapperData udfWrapperData = GetUdfWrapperDataFromStream(
                stream,
                out serializerMode,
                out deserializerMode,
                out runMode);
            int nodeIndex = 0;
            int udfIndex = 0;
            T udf = (T)DeserializeUdfs<T>(udfWrapperData, ref nodeIndex, ref udfIndex);

            // Check all the data is consumed.
            Debug.Assert(nodeIndex == udfWrapperData.UdfWrapperNodes.Length);
            Debug.Assert(udfIndex == udfWrapperData.Udfs.Length);

            return udf;
        }

        /// <summary>
        /// Deserializes a non-UDF command from the stream.
        /// This method handles both RDD commands and Raw commands, detecting the type
        /// from the serialized wrapper information.
        ///
        /// Raw UDFs provide direct access to input/output streams for high-performance
        /// scenarios where standard row-by-row processing is not efficient enough.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="serializerMode">Output serialization mode</param>
        /// <param name="deserializerMode">Output deserialization mode</param>
        /// <param name="runMode">Output run mode</param>
        /// <returns>Either RDD.WorkerFunction.ExecuteDelegate or RawWorkerFunction.ExecuteDelegate</returns>
        internal static object DeserializeNonUdf(
            Stream stream,
            out SerializedMode serializerMode,
            out SerializedMode deserializerMode,
            out string runMode)
        {
            UdfWrapperData udfWrapperData = GetUdfWrapperDataFromStream(
                stream,
                out serializerMode,
                out deserializerMode,
                out runMode);
            int nodeIndex = 0;
            int udfIndex = 0;
            UdfWrapperNode node = udfWrapperData.UdfWrapperNodes[nodeIndex];
            Type nodeType = Type.GetType(node.TypeName);
            Delegate udf;
            if (nodeType == typeof(RawUdfWrapper))
            {
                udf = DeserializeUdfs<RawWorkerFunction.ExecuteDelegate>(
                    udfWrapperData,
                    ref nodeIndex,
                    ref udfIndex);
            }
            else
            {
                udf = DeserializeUdfs<RDD.WorkerFunction.ExecuteDelegate>(
                    udfWrapperData,
                    ref nodeIndex,
                    ref udfIndex);
            }

            // Check all the data is consumed.
            Debug.Assert(nodeIndex == udfWrapperData.UdfWrapperNodes.Length);
            Debug.Assert(udfIndex == udfWrapperData.Udfs.Length);

            return udf;
        }

        private static Delegate DeserializeUdfs<T>(
            UdfWrapperData data,
            ref int nodeIndex,
            ref int udfIndex)
        {
            UdfWrapperNode node = data.UdfWrapperNodes[nodeIndex++];
            Type nodeType = Type.GetType(node.TypeName);

            if (node.HasUdf)
            {
                var udfs = new object[node.NumChildren];
                for (int i = 0; i < node.NumChildren; ++i)
                {
                    udfs[i] = UdfSerDe.Deserialize(data.Udfs[udfIndex++]);
                }

                return CreateUdfWrapperDelegate<T>(nodeType, udfs);
            }

            var udfWrappers = new object[node.NumChildren];
            for (int i = 0; i < node.NumChildren; ++i)
            {
                udfWrappers[i] = DeserializeUdfs<T>(data, ref nodeIndex, ref udfIndex);
            }

            return CreateUdfWrapperDelegate<T>(nodeType, udfWrappers);
        }

        private static Delegate CreateUdfWrapperDelegate<T>(Type type, object[] parameters)
        {
            BindingFlags bindingFlags = BindingFlags.Instance |
                BindingFlags.Static |
                BindingFlags.NonPublic |
                BindingFlags.Public;

            object udfWrapper = Activator.CreateInstance(
                type,
                bindingFlags,
                null,
                parameters,
                null);

            return Delegate.CreateDelegate(
                typeof(T),
                udfWrapper,
                UdfWrapperMethodName);
        }
    }
}
