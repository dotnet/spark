using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop;
using System.Collections.Generic;

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

        internal Broadcast(SparkContext sc, object value, JvmObjectReference sparkContext)
        {
            var path = (string)Path.Combine(sc.TempDir, Path.GetRandomFileName());
            Version version = SparkEnvironment.SparkVersion;

            // For Spark versions 2.3.0 and 2.3.1, Broadcast variable is created through different
            // functions.
            if (version.Major == 2 && version.Minor == 3 && (version.Build == 0 || 
                version.Build == 1))
            {
                WriteBroadcastValueToFile(sc, path, value);
                var javaSparkContext = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.java.JavaSparkContext",
                    "fromSparkContext",
                    sparkContext);
                _jvmObject = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "readBroadcastFromFile",
                    javaSparkContext,
                    path);
            }
            else
            {
                SparkConf sparkConf = sc.GetConf();
                sc.EncryptionEnabled = bool.Parse(
                    sparkConf.Get("spark.io.encryption.enabled",
                    "false"));

                var pythonBroadcast = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    path);

                if (sc.EncryptionEnabled)
                {
                    throw new NotImplementedException(
                            "Broadcast encryption is not supported yet.");
                }
                else
                {
                    WriteBroadcastValueToFile(sc, path, value);
                }

                _jvmObject = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.sql.api.dotnet.SQLUtils",
                    "createBroadcast",
                    sparkContext,
                    pythonBroadcast);
            }
            _bid = (long)_jvmObject.Invoke("id");
            JvmBroadcastRegistry.s_jvmBroadcastVariables.Add(_jvmObject);
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Get the broadcasted value.
        /// </summary>
        /// <returns>The broadcasted value</returns>
        public T Value()
        {
            return (T)BroadcastRegistry.s_registry[_bid];
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
        /// <param name="sc">Spark Context object</param>
        /// <param name="path">Path where file is to be created</param>
        /// <param name="value">Broadcast value to be written to the file</param>
        private void WriteBroadcastValueToFile(SparkContext sc, string path, object value)
        {
            Directory.CreateDirectory(sc.TempDir);
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
    }

    /// <summary>
    /// Global registry to store the value of all active broadcast variables. It is populated on
    /// the worker when accessed through BroadcastVariableProcessor. The value is returned whenever
    /// the user invokes Broadcast.Value().
    /// </summary>
    internal static class BroadcastRegistry
    {
        public static ConcurrentDictionary<long, object> s_registry = 
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
    }

    /// <summary>
    /// Stores the JVMObjectReference objects of all active broadcast variables that
    /// are sent to each executor through the CreatePythonFunction.
    /// </summary>
    internal static class JvmBroadcastRegistry
    {
        public static List<JvmObjectReference> s_jvmBroadcastVariables = 
            new List<JvmObjectReference>();

        /// <summary>
        /// Converts Generic.List of JvmObjectReference to ArrayList of JvmObjectReference
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        /// <returns>ArrayList object containing JvmObjectReference objects for all
        /// broadcast variables</returns>
        public static ArrayList ConvertToArrayList(IJvmBridge jvm)
        {
            var arrayList = new ArrayList(jvm);
            foreach (JvmObjectReference broadcastVariable in s_jvmBroadcastVariables)
            {
                arrayList.Add(broadcastVariable);
            }
            return arrayList;
        }
    }
}

