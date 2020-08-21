// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

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
        private static readonly string s_storageLevelClassName =
            "org.apache.spark.storage.StorageLevel";
        private static StorageLevel s_none;
        private static StorageLevel s_diskOnly;
        private static StorageLevel s_diskOnly2;
        private static StorageLevel s_memoryOnly;
        private static StorageLevel s_memoryOnly2;
        private static StorageLevel s_memoryOnlySer;
        private static StorageLevel s_memoryOnlySer2;
        private static StorageLevel s_memoryAndDisk;
        private static StorageLevel s_memoryAndDisk2;
        private static StorageLevel s_memoryAndDiskSer;
        private static StorageLevel s_memoryAndDiskSer2;
        private static StorageLevel s_offHeap;
        private readonly JvmObjectReference _jvmObject;

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
            _jvmObject = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_storageLevelClassName,
                "apply",
                UseDisk,
                UseMemory,
                UseOffHeap,
                Deserialized,
                Replication);
        }

        /// <summary>
        /// Returns the StorageLevel object with all parameters set to false.
        /// </summary>
        public static StorageLevel NONE =>
            s_none ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "NONE"));

        /// <summary>
        /// Returns the StorageLevel to Disk, serialized and replicated once.
        /// </summary>
        public static StorageLevel DISK_ONLY =>
            s_diskOnly ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "DISK_ONLY"));

        /// <summary>
        /// Returns the StorageLevel to Disk, serialized and replicated twice.
        /// </summary>
        public static StorageLevel DISK_ONLY_2 =>
            s_diskOnly2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "DISK_ONLY_2"));

        /// <summary>
        /// Returns the StorageLevel to Memory, deserialized and replicated once.
        /// </summary>
        public static StorageLevel MEMORY_ONLY =>
            s_memoryOnly ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_ONLY"));

        /// <summary>
        /// Returns the StorageLevel to Memory, deserialized and replicated twice.
        /// </summary>
        public static StorageLevel MEMORY_ONLY_2 =>
            s_memoryOnly2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_ONLY_2"));

        /// <summary>
        /// Returns the StorageLevel to Memory, serialized and replicated once.
        /// </summary>
        public static StorageLevel MEMORY_ONLY_SER =>
            s_memoryOnlySer ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_ONLY_SER"));

        /// <summary>
        /// Returns the StorageLevel to Memory, serialized and replicated twice.
        /// </summary>
        public static StorageLevel MEMORY_ONLY_SER_2 =>
            s_memoryOnlySer2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_ONLY_SER_2"));

        /// <summary>
        /// Returns the StorageLevel to Disk and Memory, deserialized and replicated once.
        /// </summary>
        public static StorageLevel MEMORY_AND_DISK =>
            s_memoryAndDisk ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_AND_DISK"));

        /// <summary>
        /// Returns the StorageLevel to Disk and Memory, deserialized and replicated twice.
        /// </summary>
        public static StorageLevel MEMORY_AND_DISK_2 =>
            s_memoryAndDisk2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_AND_DISK_2"));

        /// <summary>
        /// Returns the StorageLevel to Disk and Memory, serialized and replicated once.
        /// </summary>
        public static StorageLevel MEMORY_AND_DISK_SER =>
            s_memoryAndDiskSer ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_AND_DISK_SER"));

        /// <summary>
        /// Returns the StorageLevel to Disk and Memory, serialized and replicated twice.
        /// </summary>
        public static StorageLevel MEMORY_AND_DISK_SER_2 =>
            s_memoryAndDiskSer2 ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "MEMORY_AND_DISK_SER_2"));

        /// <summary>
        /// Returns the StorageLevel to Disk, Memory and Offheap, serialized and replicated once.
        /// </summary>
        public static StorageLevel OFF_HEAP =>
            s_offHeap ??= new StorageLevel(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_storageLevelClassName,
                    "OFF_HEAP"));

        /// <summary>
        /// Returns bool value of UseDisk of this StorageLevel.
        /// </summary>
        public bool UseDisk { get; private set; }

        /// <summary>
        /// Returns bool value of UseMemory of this StorageLevel.
        /// </summary>
        public bool UseMemory { get; private set; }

        /// <summary>
        /// Returns bool value of UseOffHeap of this StorageLevel.
        /// </summary>
        public bool UseOffHeap { get; private set; }

        /// <summary>
        /// Returns bool value of Deserialized of this StorageLevel.
        /// </summary>
        public bool Deserialized { get; private set; }

        /// <summary>
        /// Returns int value of Replication of this StorageLevel.
        /// </summary>
        public int Replication { get; private set; }

        /// <summary>
        /// Returns the description string of this StorageLevel.
        /// </summary>
        /// <returns>Description as string.</returns>
        public string Description() => (string)_jvmObject.Invoke("description");

        /// <summary>
        /// Returns the string representation of this StorageLevel.
        /// </summary>
        /// <returns>representation as string value.</returns>
        public override string ToString() => (string)_jvmObject.Invoke("toString");

        /// <summary>
        /// Checks if the given object is same as the current object.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj)
        {
            //Check for null and compare run-time types.
            if ((obj == null) || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            else
            {
                StorageLevel s = (StorageLevel)obj;
                return (s.UseDisk == UseDisk) && (s.UseMemory == UseMemory) &&
                    (s.UseOffHeap == UseOffHeap) && (s.Deserialized == Deserialized) &&
                    (s.Replication == Replication);
            }
        }

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => base.GetHashCode();
    }
}

