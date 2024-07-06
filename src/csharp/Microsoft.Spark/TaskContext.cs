// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Spark
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

        internal int CPUs { get; set; }

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

    // TaskContextHolder contains the TaskContext for the current Thread.
    internal static class TaskContextHolder
    {
        // Multiple Tasks can be assigned to a Worker process. Each
        // Task will run in its own thread until completion. Therefore
        // we set this field as a thread local variable, where each
        // thread will have its own copy of the TaskContext.
        [ThreadStatic]
        internal static TaskContext s_taskContext;

        internal static TaskContext Get() => s_taskContext;

        internal static void Set(TaskContext tc) => s_taskContext = tc;
    }
}
