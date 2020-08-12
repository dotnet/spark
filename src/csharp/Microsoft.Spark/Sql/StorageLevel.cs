// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Net;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Flags for controlling the storage of an RDD. Each StorageLevel records whether to use
    /// memory, whether to drop the RDD to disk if it falls out of memory, whether to keep the
    /// data in memory in a JAVA-specific serialized format, and whether to replicate the RDD
    /// partitions on multiple nodes.Also contains static constants for some commonly used storage
    /// levels, MEMORY_ONLY. Since the data is always serialized on the Python side, all the
    /// constants use the serialized formats.
    /// </summary>
    public sealed class StorageLevel : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;
        private readonly bool _useDisk;
        private readonly bool _useMemory;
        private readonly bool _useOffHeap;
        private readonly bool _deserialized;
        private readonly int _replication;

        internal StorageLevel(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        public StorageLevel(
            bool useDisk,
            bool useMemory,
            bool useOffHeap,
            bool deserialized,
            int replication = 1)
        {
            _useDisk = useDisk;
            _useMemory = useMemory;
            _useOffHeap = useOffHeap;
            _deserialized = deserialized;
            _replication = replication;
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.storage.StorageLevel",
                _useDisk,
                _useMemory,
                _useOffHeap,
                _deserialized,
                _replication);
        }

        public string Description() => (string)_jvmObject.Invoke("description");

        public override string ToString() => (string)_jvmObject.Invoke("toString");

        // TODO: expose these as static APIs
        //public StorageLevel.DISK_ONLY => new StorageLevel(true, false, false, false);
        //public StorageLevel.DISK_ONLY_2 => new StorageLevel(true, false, false, false, 2);
        //public StorageLevel.MEMORY_ONLY => new StorageLevel(false, true, false, false);
        //public StorageLevel.MEMORY_ONLY_2 => new StorageLevel(false, true, false, false, 2);
        //public StorageLevel.MEMORY_AND_DISK => new StorageLevel(true, true, false, false);
        //public StorageLevel.MEMORY_AND_DISK_2 => new StorageLevel(true, true, false, false, 2);
        //public StorageLevel.OFF_HEAP => new StorageLevel(true, true, true, false, 1);
    }

}
