// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Razorvine.Pickle;
using static Microsoft.Spark.Utils.UdfUtils;

namespace Microsoft.Spark.Worker.UnitTest
{
    /// <summary>
    /// Command stores data necessary to create a payload for a single command,
    /// which can have chained UDFs. The reason Microsoft.Spark.Worker.Command
    /// cannot be used is because it stores WorkerFunction which already abstracts
    /// out the chained UDFs.
    /// </summary>
    internal sealed class Command
    {
        internal Delegate[] ChainedUdfs { get; set; }

        internal int[] ArgOffsets { get; set; }

        internal CommandSerDe.SerializedMode SerializerMode { get; set; }

        internal CommandSerDe.SerializedMode DeserializerMode { get; set; }
    }

    /// <summary>
    /// CommandPayload stores data necessary to create a payload for multiple commands.
    /// </summary>
    internal sealed class CommandPayload
    {
        internal PythonEvalType EvalType { get; set; }

        internal Command[] Commands { get; set; }
    }

    ///////////////////////////////////////////////////////////////////////////
    // TaskContext writer for different Spark versions.
    ///////////////////////////////////////////////////////////////////////////

    internal interface ITaskContextWriter
    {
        void Write(Stream stream, TaskContext taskContext);
    }

    /// <summary>
    /// TaskContextWriter for version 2.4.*.
    /// </summary>
    internal sealed class TaskContextWriterV2_4_X : ITaskContextWriter
    {
        public void Write(Stream stream, TaskContext taskContext)
        {
            SerDe.Write(stream, taskContext.IsBarrier);
            SerDe.Write(stream, taskContext.Port);
            SerDe.Write(stream, taskContext.Secret);

            SerDe.Write(stream, taskContext.StageId);
            SerDe.Write(stream, taskContext.PartitionId);
            SerDe.Write(stream, taskContext.AttemptNumber);
            SerDe.Write(stream, taskContext.AttemptId);

            SerDe.Write(stream, taskContext.LocalProperties.Count);
            foreach (KeyValuePair<string, string> kv in taskContext.LocalProperties)
            {
                SerDe.Write(stream, kv.Key);
                SerDe.Write(stream, kv.Value);
            }
        }
    }

    /// <summary>
    /// TaskContextWriter for version 3.0.*.
    /// </summary>
    internal sealed class TaskContextWriterV3_0_X : ITaskContextWriter
    {
        public void Write(Stream stream, TaskContext taskContext)
        {
            SerDe.Write(stream, taskContext.IsBarrier);
            SerDe.Write(stream, taskContext.Port);
            SerDe.Write(stream, taskContext.Secret);

            SerDe.Write(stream, taskContext.StageId);
            SerDe.Write(stream, taskContext.PartitionId);
            SerDe.Write(stream, taskContext.AttemptNumber);
            SerDe.Write(stream, taskContext.AttemptId);

            SerDe.Write(stream, taskContext.Resources.Count());
            foreach (TaskContext.Resource resource in taskContext.Resources)
            {
                SerDe.Write(stream, resource.Key);
                SerDe.Write(stream, resource.Value);
                SerDe.Write(stream, resource.Addresses.Count());
                foreach (string address in resource.Addresses)
                {
                    SerDe.Write(stream, address);
                }
            }

            SerDe.Write(stream, taskContext.LocalProperties.Count);
            foreach (KeyValuePair<string, string> kv in taskContext.LocalProperties)
            {
                SerDe.Write(stream, kv.Key);
                SerDe.Write(stream, kv.Value);
            }
        }
    }

    /// <summary>
    /// TaskContextWriter for version 3.3.*.
    /// </summary>
    internal sealed class TaskContextWriterV3_3_X : ITaskContextWriter
    {
        public void Write(Stream stream, TaskContext taskContext)
        {
            SerDe.Write(stream, taskContext.IsBarrier);
            SerDe.Write(stream, taskContext.Port);
            SerDe.Write(stream, taskContext.Secret);

            SerDe.Write(stream, taskContext.StageId);
            SerDe.Write(stream, taskContext.PartitionId);
            SerDe.Write(stream, taskContext.AttemptNumber);
            SerDe.Write(stream, taskContext.AttemptId);
            // Add CPUs field for spark 3.3.x
            SerDe.Write(stream, taskContext.CPUs);

            SerDe.Write(stream, taskContext.Resources.Count());
            foreach (TaskContext.Resource resource in taskContext.Resources)
            {
                SerDe.Write(stream, resource.Key);
                SerDe.Write(stream, resource.Value);
                SerDe.Write(stream, resource.Addresses.Count());
                foreach (string address in resource.Addresses)
                {
                    SerDe.Write(stream, address);
                }
            }

            SerDe.Write(stream, taskContext.LocalProperties.Count);
            foreach (KeyValuePair<string, string> kv in taskContext.LocalProperties)
            {
                SerDe.Write(stream, kv.Key);
                SerDe.Write(stream, kv.Value);
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // BroadcastVariable writer for different Spark versions.
    ///////////////////////////////////////////////////////////////////////////

    internal interface IBroadcastVariableWriter
    {
        void Write(Stream stream, BroadcastVariables broadcastVars);
    }

    /// <summary>
    /// BroadcastVariableWriter for version 2.4.*.
    /// </summary>
    internal sealed class BroadcastVariableWriterV2_4_X : IBroadcastVariableWriter
    {
        public void Write(Stream stream, BroadcastVariables broadcastVars)
        {
            SerDe.Write(stream, broadcastVars.DecryptionServerNeeded);
            SerDe.Write(stream, broadcastVars.Count);

            Debug.Assert(broadcastVars.Count == 0);

            if (broadcastVars.DecryptionServerNeeded)
            {
                SerDe.Write(stream, broadcastVars.DecryptionServerPort);
                SerDe.Write(stream, broadcastVars.Secret);
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Command writer for different Spark versions.
    ///////////////////////////////////////////////////////////////////////////

    internal interface ICommandWriter
    {
        void Write(Stream stream, CommandPayload commandPayload);
    }

    /// <summary>
    /// Provides a functionality to write Command[].
    /// </summary>
    internal abstract class CommandWriterBase
    {
        public void Write(Stream stream, Command[] commands)
        {
            SerDe.Write(stream, commands.Length);
            foreach (Command command in commands)
            {
                SerDe.Write(stream, command.ArgOffsets.Length);
                foreach (int argOffset in command.ArgOffsets)
                {
                    SerDe.Write(stream, argOffset);
                }

                SerDe.Write(stream, command.ChainedUdfs.Length);
                foreach (Delegate udf in command.ChainedUdfs)
                {
                    byte[] serializedCommand = CommandSerDe.Serialize(
                        udf,
                        CommandSerDe.SerializedMode.Row,
                        CommandSerDe.SerializedMode.Row);

                    SerDe.Write(stream, serializedCommand.Length);
                    SerDe.Write(stream, serializedCommand);
                }
            }
        }
    }

    /// <summary>
    /// CommandWriter for version 2.4.*.
    /// </summary>
    internal sealed class CommandWriterV2_4_X : CommandWriterBase, ICommandWriter
    {
        public void Write(Stream stream, CommandPayload commandPayload)
        {

            if (commandPayload.EvalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF ||
                commandPayload.EvalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF ||
                commandPayload.EvalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF ||
                commandPayload.EvalType == PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF)
            {
                SerDe.Write(stream, 1);
                for (int i = 0; i < 1; ++i)
                {
                    SerDe.Write(stream, "unused key");
                    SerDe.Write(stream, "unused value");
                }
            }

            SerDe.Write(stream, (int)commandPayload.EvalType);

            Write(stream, commandPayload.Commands);
        }
    }

    /// <summary>
    /// Payload writer that supports different Spark versions.
    /// </summary>
    internal sealed class PayloadWriter
    {
        private readonly ITaskContextWriter _taskContextWriter;
        private readonly IBroadcastVariableWriter _broadcastVariableWriter;
        private readonly ICommandWriter _commandWriter;

        internal PayloadWriter(
            Version version,
            ITaskContextWriter taskContextWriter,
            IBroadcastVariableWriter broadcastVariableWriter,
            ICommandWriter commandWriter)
        {
            Version = version;
            _taskContextWriter = taskContextWriter;
            _broadcastVariableWriter = broadcastVariableWriter;
            _commandWriter = commandWriter;
        }

        internal Version Version { get; }

        internal void Write(
            Stream stream,
            Payload payload,
            CommandPayload commandPayload)
        {
            SerDe.Write(stream, payload.SplitIndex);
            SerDe.Write(stream, payload.Version);
            _taskContextWriter.Write(stream, payload.TaskContext);
            SerDe.Write(stream, payload.SparkFilesDir);
            Write(stream, payload.IncludeItems);
            _broadcastVariableWriter.Write(stream, payload.BroadcastVariables);
            _commandWriter.Write(stream, commandPayload);
        }

        public void WriteTestData(Stream stream)
        {
            Payload payload = TestData.GetDefaultPayload();
            CommandPayload commandPayload = TestData.GetDefaultCommandPayload();

            Write(stream, payload, commandPayload);

            // Write 10 rows to the output stream.
            var pickler = new Pickler();
            for (int i = 0; i < 10; ++i)
            {
                byte[] pickled = pickler.dumps(
                    new[] { new object[] { i.ToString(), i, i } });
                SerDe.Write(stream, pickled.Length);
                SerDe.Write(stream, pickled);
            }

            // Signal the end of data and stream.
            SerDe.Write(stream, (int)SpecialLengths.END_OF_DATA_SECTION);
            SerDe.Write(stream, (int)SpecialLengths.END_OF_STREAM);
            stream.Flush();
        }

        private static void Write(Stream stream, IEnumerable<string> includeItems)
        {
            if (includeItems is null)
            {
                SerDe.Write(stream, 0);
                return;
            }

            SerDe.Write(stream, includeItems.Count());
            foreach (string includeItem in includeItems)
            {
                SerDe.Write(stream, includeItem);
            }
        }
    }

    /// <summary>
    /// Factory class for creating a PayloadWriter given a version.
    /// </summary>
    internal sealed class PayloadWriterFactory
    {
        internal PayloadWriter Create(Version version = null)
        {
            if (version == null)
            {
                version = new Version(Versions.V2_4_0);
            }

            switch (version.ToString())
            {
                case Versions.V2_4_0:
                    return new PayloadWriter(
                        version,
                        new TaskContextWriterV2_4_X(),
                        new BroadcastVariableWriterV2_4_X(),
                        new CommandWriterV2_4_X());
                case Versions.V3_0_0: 
                case Versions.V3_2_0:
                    return new PayloadWriter(
                        version,
                        new TaskContextWriterV3_0_X(),
                        new BroadcastVariableWriterV2_4_X(),
                        new CommandWriterV2_4_X());
                case Versions.V3_3_0:
                case Versions.V3_5_1:
                    return new PayloadWriter(
                        version,
                        new TaskContextWriterV3_3_X(),
                        new BroadcastVariableWriterV2_4_X(),
                        new CommandWriterV2_4_X());
                default:
                    throw new NotSupportedException($"Spark {version} is not supported.");
            }
        }
    }
}
