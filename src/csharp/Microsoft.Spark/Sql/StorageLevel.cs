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

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        public bool UseDisk { get; private set; }
        public bool UseMemory { get; private set; }
        public bool UseOffHeap { get; private set; }
        public bool Deserialized { get; private set; }
        public int Replication { get; private set; }

        internal StorageLevel(JvmObjectReference jvmObject)
        {
            UseDisk = (bool)jvmObject.Invoke("useDisk");
            UseMemory = (bool)jvmObject.Invoke("useMemory");
            UseOffHeap = (bool)jvmObject.Invoke("useOffHeap");
            Deserialized = (bool)jvmObject.Invoke("deserialized");
            Replication = (int)jvmObject.Invoke("replication");
            _jvmObject = jvmObject;
        }

        public StorageLevel(
            bool useDisk,
            bool useMemory,
            bool useOffHeap,
            bool deserialized,
            int replication = 1)
        {
            UseDisk = useDisk;
            UseMemory = useMemory;
            UseOffHeap = useOffHeap;
            Deserialized = deserialized;
            Replication = replication;
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.storage.StorageLevel",
                UseDisk,
                UseMemory,
                UseOffHeap,
                Deserialized,
                Replication);
        }

        public string Description() => (string)_jvmObject.Invoke("description");

        public override string ToString() => (string)_jvmObject.Invoke("toString");

        public static StorageLevel DISK_ONLY { get; } = 
            new StorageLevel(true, false, false, false);
        public static StorageLevel DISK_ONLY_2 { get; } = 
            new StorageLevel(true, false, false, false, 2);
        public static StorageLevel MEMORY_ONLY { get; } = 
            new StorageLevel(false, true, false, false);
        public static StorageLevel MEMORY_ONLY_2 { get; } = 
            new StorageLevel(false, true, false, false, 2);
        public static StorageLevel MEMORY_AND_DISK { get; } = 
            new StorageLevel(true, true, false, false);
        public static StorageLevel MEMORY_AND_DISK_2 { get; } = 
            new StorageLevel(true, true, false, false, 2);
        public static StorageLevel OFF_HEAP { get; } = 
            new StorageLevel(true, true, true, false, 1);
    }

}


