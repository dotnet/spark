// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
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

            var formatter = new BinaryFormatter();
            using (var stream = new MemoryStream())
            {
                formatter.Serialize(stream, udfWrapperData);

                byte[] udfBytes = stream.ToArray();
                byte[] udfBytesLengthAsBytes = BitConverter.GetBytes(udfBytes.Length);
                Array.Reverse(udfBytesLengthAsBytes);
                commandPayloadBytesList.Add(udfBytesLengthAsBytes);
                commandPayloadBytesList.Add(udfBytes);
            }

            return commandPayloadBytesList.SelectMany(byteArray => byteArray).ToArray();
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

            var bf = new BinaryFormatter();
            var ms = new MemoryStream(serializedCommand, false);

            return (UdfWrapperData)bf.Deserialize(ms);
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
