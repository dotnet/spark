using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Microsoft.Spark
{
    /// <summary>
    /// A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
    /// cached on each machine rather than shipping a copy of it with tasks. They can be used, for
    /// example, to give every node a copy of a large input dataset in an efficient manner. Spark
    /// also attempts to distribute broadcast variables using efficient broadcast algorithms to
    /// reduce communication cost.
    /// </summary>
    [Serializable]
    public sealed class Broadcast<T>: IJvmObjectReferenceProvider
    {
        [NonSerialized]
        private readonly JvmObjectReference _jvmObject;
        private readonly long _bid;

        internal Broadcast(SparkContext sc,
            object value,
            string localDir,
            JvmObjectReference sparkContext)
        {
            var tempDir = Path.Combine(localDir, "sparkdotnet");
            string path = Path.Combine(tempDir, Path.GetRandomFileName());
            Version version = SparkEnvironment.SparkVersion;

            // For Spark versions 2.3.0 and 2.3.1, Broadcast variable is created through different
            // functions.
            _jvmObject = (version.Major, version.Minor) switch
            {
                (2, 3) => (version.Build) switch
                {
                    0 => CreateBroadcast_V2_3_1_Below(path, tempDir, sparkContext, value),
                    1 => CreateBroadcast_V2_3_1_Below(path, tempDir, sparkContext, value),
                    2 => CreateBroadcast_V2_3_2_Above(sc, path, tempDir, sparkContext, value),
                    3 => CreateBroadcast_V2_3_2_Above(sc, path, tempDir, sparkContext, value),
                    4 => CreateBroadcast_V2_3_2_Above(sc, path, tempDir, sparkContext, value),
                    _ => throw new Exception($"Incorrect Spark version: {version}")
                },
                (2, 4) => CreateBroadcast_V2_3_2_Above(sc, path, tempDir, sparkContext, value),
                (3, 0) => CreateBroadcast_V2_3_2_Above(sc, path, tempDir, sparkContext, value),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };

            _bid = (long)_jvmObject.Invoke("id");
            JvmBroadcastRegistry.Add(_jvmObject);
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Get the broadcasted value.
        /// </summary>
        /// <returns>The broadcasted value</returns>
        public T Value()
        {
            return (T)BroadcastRegistry.Get(_bid);
        }

        /// <summary>
        /// Asynchronously delete cached copies of this broadcast on the executors.
        /// If the broadcast is used after this is called, it will need to be re-sent to each
        /// executor.
        /// </summary>
        public void Unpersist()
        {
            _jvmObject.Invoke("unpersist");
        }

        /// <summary>
        /// Delete cached copies of this broadcast on the executors. If the broadcast is used after
        /// this is called, it will need to be re-sent to each executor.
        /// </summary>
        /// <param name="blocking">Whether to block until unpersisting has completed</param>
        public void Unpersist(bool blocking)
        {
            _jvmObject.Invoke("unpersist", blocking);
        }

        /// <summary>
        /// Destroy all data and metadata related to this broadcast variable. Use this with
        /// caution; once a broadcast variable has been destroyed, it cannot be used again.
        /// This method blocks until destroy has completed.
        /// </summary>
        public void Destroy()
        {
            _jvmObject.Invoke("destroy");
        }

        /// <summary>
        /// Function that creates a file to store the broadcast value in the given path.
        /// </summary>
        /// <param name="path">Complete filename with the absolute path</param>
        /// <param name="tempDir">Path where file is to be created</param>
        /// <param name="value">Broadcast value to be written to the file</param>
        private void WriteBroadcastValueToFile(string path, string tempDir, object value)
        {
            Directory.CreateDirectory(tempDir);
            using FileStream f = File.Create(path);
            Dump(value, f);
        }

        /// <summary>
        /// Function that serializes and stores the object passed to the given Stream.
        /// </summary>
        /// <param name="value">Serializable object</param>
        /// <param name="stream">Stream to which the object is serialized</param>
        private void Dump(object value, Stream stream)
        {
            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, value);
        }

        /// <summary>
        /// Calls the necessary functions to create org.apache.spark.broadcast.Broadcast object
        /// for Spark versions 2.3.0 and 2.3.1 and returns the JVMObjectReference object.
        /// </summary>
        /// <param name="path">Complete filename with the absolute path</param>
        /// <param name="tempDir">Path where file is to be created</param>
        /// <param name="sparkContext">JVMObjectReference object of SparkContext</param>
        /// <param name="value">Broadcast value of type object</param>
        /// <returns></returns>
        private JvmObjectReference CreateBroadcast_V2_3_1_Below(
            string path,
            string tempDir,
            JvmObjectReference sparkContext,
            object value)
        {
            WriteBroadcastValueToFile(path, tempDir, value);
            var javaSparkContext = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.java.JavaSparkContext",
                "fromSparkContext",
                sparkContext);
            return (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.python.PythonRDD",
                "readBroadcastFromFile",
                javaSparkContext,
                path);
        }

        /// <summary>
        /// Calls the necessary Spark functions to create org.apache.spark.broadcast.Broadcast
        /// object for Spark versions 2.3.2 and above, and returns the JVMObjectReference object.
        /// </summary>
        /// <param name="sc">SparkContext object</param>
        /// <param name="path">Complete filename with the absolute path</param>
        /// <param name="tempDir">Path where file is to be created</param>
        /// <param name="sparkContext">JVMObjectReference object of SparkContext</param>
        /// <param name="value">Broadcast value of type object</param>
        /// <returns></returns>
        private JvmObjectReference CreateBroadcast_V2_3_2_Above(
            SparkContext sc,
            string path,
            string tempDir,
            JvmObjectReference sparkContext,
            object value)
        {
            var pythonBroadcast = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    path);
            SparkConf sparkConf = sc.GetConf();
            var encryptionEnabled = bool.Parse(
                sparkConf.Get("spark.io.encryption.enabled",
                "false"));

            if (encryptionEnabled)
            {
                throw new NotImplementedException(
                    "Broadcast encryption is not supported yet.");
            }
            else
            {
                WriteBroadcastValueToFile(path, tempDir, value);
            }

            return (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.api.dotnet.SQLUtils",
                "createBroadcast",
                sparkContext,
                pythonBroadcast);
        }
    }

    /// <summary>
    /// Global registry to store the object value of all active broadcast variables from
    /// the workers. This registry is only used on the worker side when Broadcast.Value() is called
    /// through a UDF.
    /// </summary>
    internal static class BroadcastRegistry
    {
        private static ConcurrentDictionary<long, object> s_registry = 
            new ConcurrentDictionary<long, object>();

        /// <summary>
        /// Function to add the value of the broadcast variable to s_registry.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object to add</param>
        /// <param name="value">Value of the Broadcast variable</param>
        public static void Add(long bid, object value)
        {
            s_registry.TryAdd(bid, value);
        }

        /// <summary>
        /// Function to remove the Broadcast variable from s_regitry.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object to remove</param>
        public static void Remove(long bid)
        {
            s_registry.TryRemove(bid, out _);
        }

        public static object Get(long bid)
        {
            return s_registry[bid];
        }
    }

    /// <summary>
    /// Stores the JVMObjectReference object of type org.apache.spark.broadcast.Broadcast for all
    /// active broadcast variables that are sent to the workers through the CreatePythonFunction.
    /// This registry is only used on the driver side.
    /// </summary>
    internal static class JvmBroadcastRegistry
    {
        private static List<JvmObjectReference> s_jvmBroadcastVariables = 
            new List<JvmObjectReference>();

        /// <summary>
        /// Adds a JVMObjectReference object of type <see cref="Broadcast{T}"/> to the list.
        /// </summary>
        /// <param name="broadcastJvmObject">JVMObjectReference of the Broadcast variable</param>
        public static void Add(JvmObjectReference broadcastJvmObject)
        {
            s_jvmBroadcastVariables.Add(broadcastJvmObject);
        }

        /// <summary>
        /// Returns the static member s_jvmBroadcastVariables.
        /// </summary>
        /// <returns></returns>
        public static List<JvmObjectReference> GetAll()
        {
            return s_jvmBroadcastVariables;
        }
    }
}

