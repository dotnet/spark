// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Flags for controlling the storage of an RDD. Each StorageLevel records whether to use
    /// memory, whether to drop the RDD to disk if it falls out of memory, whether to keep the
    /// data in memory in a JAVA-specific serialized format, and whether to replicate the RDD
    /// partitions on multiple nodes. Also contains static properties for some commonly used
    /// storage levels, MEMORY_ONLY.
    /// </summary>
    public sealed class StorageLevel : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;
        private static readonly string s_storageLevelClassName =
            "org.apache.spark.storage.StorageLevel";
        private static StorageLevel s_diskOnly;
        private static StorageLevel s_diskOnly2;
        private static StorageLevel s_memoryOnly;
        private static StorageLevel s_memoryOnly2;
        private static StorageLevel s_memoryAndDisk;
        private static StorageLevel s_memoryAndDisk2;
        private static StorageLevel s_offHeap;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

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
                s_storageLevelClassName,
                UseDisk,
                UseMemory,
                UseOffHeap,
                Deserialized,
                Replication);
        }

        public bool UseDisk { get; private set; }
        public bool UseMemory { get; private set; }
        public bool UseOffHeap { get; private set; }
        public bool Deserialized { get; private set; }
        public int Replication { get; private set; }

        public static StorageLevel DISK_ONLY
        {
            get => s_diskOnly ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "DISK_ONLY"));
        }

        public static StorageLevel DISK_ONLY_2
        {
            get => s_diskOnly2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "DISK_ONLY_2"));
        }

        public static StorageLevel MEMORY_ONLY
        {
            get => s_memoryOnly ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_ONLY"));
        }

        public static StorageLevel MEMORY_ONLY_2
        {
            get => s_memoryOnly2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_ONLY_2"));
        }

        public static StorageLevel MEMORY_AND_DISK
        {
            get => s_memoryAndDisk ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_AND_DISK"));
        }

        public static StorageLevel MEMORY_AND_DISK_2
        {
            get => s_memoryAndDisk2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_AND_DISK_2"));
        }

        public static StorageLevel OFF_HEAP
        {
            get => s_offHeap ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "OFF_HEAP"));
        }

        public string Description() => (string)_jvmObject.Invoke("description");

        public override string ToString() => (string)_jvmObject.Invoke("toString");
    }
}

