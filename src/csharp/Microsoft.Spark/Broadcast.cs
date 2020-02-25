using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop;

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
    public sealed class Broadcast: IJvmObjectReferenceProvider
    {
        [NonSerialized]
        private readonly JvmObjectReference _jvmObject;
        private readonly long _bid;

        internal Broadcast(SparkContext sc, object value, JvmObjectReference sparkContext)
        {
            var path = (string)Path.Combine(sc._temp_dir, Path.GetRandomFileName());
            Version version = SparkEnvironment.SparkVersion;

            // For Spark versions 2.3.0 and 2.3.1, Broadcast variable is created through different
            // functions.
            if (version.Major == 2 && version.Minor == 3 && (version.Build == 0 || 
                version.Build == 1) )
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
                sc._encryption_enabled = bool.Parse(
                    sparkConf.Get("spark.io.encryption.enabled",
                    "false"));

                var _python_broadcast = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    path);

                if (sc._encryption_enabled)
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
                    _python_broadcast);
            }
            _bid = (long)_jvmObject.Invoke("id");
            BroadcastRegistry.s_listBroadcastVariables.Add(_jvmObject);
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Function that creates a file to store the broadcast value in the given path.
        /// </summary>
        /// <param name="sc">Spark Context object</param>
        /// <param name="path">Path where file is to be created</param>
        /// <param name="value">Broadcast value to be written to the file</param>
        private void WriteBroadcastValueToFile(SparkContext sc, string path, object value)
        {
            Directory.CreateDirectory(sc._temp_dir);
            FileStream f = File.Create(path);
            Dump(value, f);
            f.Close();
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
        /// Get the broadcasted value.
        /// </summary>
        /// <returns>The broadcasted value</returns>
        public object Value()
        {
            return BroadcastRegistry.s_registry[_bid];
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
    }

    /// <summary>
    /// Registry to store the broadcast variables for access through workers.
    /// </summary>
    internal class BroadcastRegistry
    {
        public static ConcurrentDictionary<long, object> s_registry = 
            new ConcurrentDictionary<long, object>();
        public static ArrayList s_listBroadcastVariables;

        public BroadcastRegistry(IJvmBridge jvm)
        {
            s_listBroadcastVariables = new ArrayList(jvm);
        }
    }
}

