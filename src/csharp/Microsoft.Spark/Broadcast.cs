using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

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
    public sealed class Broadcast<T> : IJvmObjectReferenceProvider
    {
        [NonSerialized]
        private readonly JvmObjectReference _jvmObject;
        private readonly long _bid;
        [NonSerialized]
        private readonly string _path;

        internal Broadcast(SparkContext sc, T value)
        {
            SparkConf sparkConf = sc.GetConf();
            var localDir = (string)((IJvmObjectReferenceProvider)sc).Reference.Jvm.
            CallStaticJavaMethod(
               "org.apache.spark.util.Utils",
               "getLocalDir",
               sparkConf);

            var tempDir = Path.Combine(localDir, "sparkdotnet");
            _path = Path.Combine(tempDir, Path.GetRandomFileName());
            Version version = SparkEnvironment.SparkVersion;

            var javaSparkContext = (JvmObjectReference)((IJvmObjectReferenceProvider)sc).Reference
            .Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.java.JavaSparkContext",
                "fromSparkContext",
                ((IJvmObjectReferenceProvider)sc).Reference);

            _jvmObject = (version.Major, version.Minor) switch
            {
                (2, 3) when version.Build == 0 || version.Build == 1 =>
                    CreateBroadcast_V2_3_1_Below(
                        javaSparkContext,
                        tempDir,
                        value),
                (2, 3) => CreateBroadcast_V2_3_2_Above(javaSparkContext,sc, tempDir, value),
                (2, 4) => CreateBroadcast_V2_3_2_Above(javaSparkContext, sc, tempDir, value),
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
            File.Delete(_path);
            JvmBroadcastRegistry.Remove(_jvmObject);
        }

        /// <summary>
        /// Function that creates a file to store the broadcast value in the given path.
        /// </summary>
        /// <param name="tempDir">Path where file is to be created</param>
        /// <param name="value">Broadcast value to be written to the file</param>
        private void WriteBroadcastValueToFile(string tempDir, object value)
        {
            Directory.CreateDirectory(tempDir);
            using FileStream f = File.Create(_path);
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
        /// <param name="javaSparkContext">Java Spark context object</param>
        /// <param name="tempDir">Path where file is to be created</param>
        /// <param name="value">Broadcast value of type object</param>
        /// <returns></returns>
        private JvmObjectReference CreateBroadcast_V2_3_1_Below(
            JvmObjectReference javaSparkContext,
            string tempDir,
            object value)
        {
            WriteBroadcastValueToFile(tempDir, value);
            
            return (JvmObjectReference)javaSparkContext.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.python.PythonRDD",
                "readBroadcastFromFile",
                javaSparkContext,
                _path);
        }

        /// <summary>
        /// Calls the necessary Spark functions to create org.apache.spark.broadcast.Broadcast
        /// object for Spark versions 2.3.2 and above, and returns the JVMObjectReference object.
        /// </summary>
        /// <param name="javaSparkContext">Java Spark context object</param>
        /// <param name="sc">SparkContext object</param>
        /// <param name="tempDir">Path where file is to be created</param>
        /// <param name="value">Broadcast value of type object</param>
        /// <returns></returns>
        private JvmObjectReference CreateBroadcast_V2_3_2_Above(
            JvmObjectReference javaSparkContext,
            SparkContext sc,
            string tempDir,
            object value)
        {
            var pythonBroadcast = (JvmObjectReference)javaSparkContext.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.python.PythonRDD",
                "setupBroadcast",
                _path);
            
            SparkConf sparkConf = sc.GetConf();
            bool encryptionEnabled = bool.Parse(
                sparkConf.Get("spark.io.encryption.enabled", "false"));

            if (encryptionEnabled)
            {
                throw new NotImplementedException("Broadcast encryption is not supported yet.");
            }
            else
            {
                WriteBroadcastValueToFile(tempDir, value);
            }

            return (JvmObjectReference)javaSparkContext.Jvm.CallNonStaticJavaMethod(
                javaSparkContext,
                "broadcast",
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
        private static readonly ConcurrentDictionary<long, object> s_registry =
            new ConcurrentDictionary<long, object>();

        /// <summary>
        /// Function to add the value of the broadcast variable to s_registry.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object to add</param>
        /// <param name="value">Value of the Broadcast variable</param>
        internal static void Add(long bid, object value) => s_registry.TryAdd(bid, value);

        /// <summary>
        /// Function to remove the Broadcast variable from s_registry.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object to remove</param>
        internal static void Remove(long bid) => s_registry.TryRemove(bid, out _);

        internal static object Get(long bid)
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
        private static readonly List<JvmObjectReference> s_jvmBroadcastVariables =
            new List<JvmObjectReference>();

        /// <summary>
        /// Adds a JVMObjectReference object of type <see cref="Broadcast{T}"/> to the list.
        /// </summary>
        /// <param name="broadcastJvmObject">JVMObjectReference of the Broadcast variable</param>
        internal static void Add(JvmObjectReference broadcastJvmObject) =>
            s_jvmBroadcastVariables.Add(broadcastJvmObject);

        /// <summary>
        /// Clears s_jvmBroadcastVariables of all the JVMObjectReference objects of type
        /// <see cref="Broadcast{T}"/>
        /// </summary>
        internal static void Clear() =>
            s_jvmBroadcastVariables.Clear();

        /// <summary>
        /// Clears s_jvmBroadcastVariables of all the JVMObjectReference objects of type
        /// <see cref="Broadcast{T}"/>
        /// </summary>
        internal static void Remove(JvmObjectReference broadcastJvmObject) =>
            s_jvmBroadcastVariables.Remove(broadcastJvmObject);

        /// <summary>
        /// Returns the static member s_jvmBroadcastVariables.
        /// </summary>
        /// <returns></returns>
        internal static List<JvmObjectReference> GetAll()
        {
            return s_jvmBroadcastVariables;
        }
    }
}

