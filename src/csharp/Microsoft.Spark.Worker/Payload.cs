// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Worker
{
    /// <summary>
    /// TaskContext stores information related to a task.
    /// </summary>
    internal class TaskContext
    {
        internal int StageId { get; set; }

        internal int PartitionId { get; set; }

        internal int AttemptNumber { get; set; }

        internal long AttemptId { get; set; }

        internal bool IsBarrier { get; set; }

        internal int Port { get; set; }

        internal string Secret { get; set; }

        internal IEnumerable<Resource> Resources { get; set; } = new List<Resource>();

        internal Dictionary<string, string> LocalProperties { get; set; } =
            new Dictionary<string, string>();

        public override bool Equals(object obj)
        {
            if (!(obj is TaskContext other))
            {
                return false;
            }

            return (StageId == other.StageId) &&
                (PartitionId == other.PartitionId) &&
                (AttemptNumber == other.AttemptNumber) &&
                (AttemptId == other.AttemptId) &&
                Resources.SequenceEqual(other.Resources) &&
                (LocalProperties.Count == other.LocalProperties.Count) &&
                !LocalProperties.Except(other.LocalProperties).Any();
        }

        public override int GetHashCode()
        {
            return StageId;
        }

        internal class Resource
        {
            internal string Key { get; set; }
            internal string Value { get; set; }
            internal IEnumerable<string> Addresses { get; set; } = new List<string>();

            public override bool Equals(object obj)
            {
                if (!(obj is Resource other))
                {
                    return false;
                }

                return (Key == other.Key) &&
                    (Value == other.Value) &&
                    Addresses.SequenceEqual(Addresses);
            }

            public override int GetHashCode()
            {
                return Key.GetHashCode();
            }
        }
    }

    /// <summary>
    /// BroadcastVariables stores information on broadcast variables.
    /// </summary>
    internal class BroadcastVariables
    {
        internal bool DecryptionServerNeeded { get; set; } = false;

        internal int DecryptionServerPort { get; set; }

        internal string Secret { get; set; }

        // Broadcast variables are currently not supported. Default to 0.
        internal int Count { get; } = 0;

        public override bool Equals(object obj)
        {
            if (!(obj is BroadcastVariables other))
            {
                return false;
            }

            return (DecryptionServerNeeded == other.DecryptionServerNeeded) &&
                (DecryptionServerPort == other.DecryptionServerPort) &&
                (Secret == other.Secret) &&
                (Count == other.Count);
        }

        public override int GetHashCode()
        {
            return Secret?.GetHashCode() ?? 0;
        }
    }

    /// <summary>
    /// Base class for capturing command information.
    /// </summary>
    internal abstract class CommandBase
    {
        // Note that the following modes are embedded in the command payload by
        // CommandSerDe.Serialize() when the payload is registered as UDF.
        internal CommandSerDe.SerializedMode SerializerMode { get; set; }
        internal CommandSerDe.SerializedMode DeserializerMode { get; set; }
    }

    /// <summary>
    /// SqlCommand stores UDF-related information for SQL.
    /// </summary>
    internal sealed class SqlCommand : CommandBase
    {
        internal int[] ArgOffsets { get; set; }

        // Note that WorkerFunction will be chained, and this will
        // be used only for the logging purpose.
        internal int NumChainedFunctions { get; set; }

        internal Sql.WorkerFunction WorkerFunction { get; set; }
    }

    /// <summary>
    /// RDDCommand stores UDF-related information for RDD.
    /// </summary>
    internal sealed class RDDCommand : CommandBase
    {
        internal RDD.WorkerFunction WorkerFunction { get; set; }
    }

    /// <summary>
    /// CommandPayload stores information on multiple commands.
    /// </summary>
    internal class CommandPayload
    {
        internal UdfUtils.PythonEvalType EvalType { get; set; }

        internal CommandBase[] Commands { get; set; }
    }

    /// <summary>
    /// Payload stores information sent to the worker from JVM.
    /// </summary>
    internal class Payload
    {
        internal int SplitIndex { get; set; }

        internal string Version { get; set; }

        internal TaskContext TaskContext { get; set; }

        internal string SparkFilesDir { get; set; }

        internal IEnumerable<string> IncludeItems { get; set; }

        internal BroadcastVariables BroadcastVariables { get; set; }

        internal CommandPayload Command { get; set; }
    }
}
