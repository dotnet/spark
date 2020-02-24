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
    /// cached on each machine rather than shipping a copy of it with tasks.They can be used, for
    /// example, to give every node a copy of a large input dataset in an efficient manner.Spark 
    /// also attempts to distribute broadcast variables using efficient broadcast algorithms to 
    /// reduce communication cost.
    /// </summary>
    [Serializable]
    public sealed class Broadcast: IJvmObjectReferenceProvider
    {
        [NonSerialized]
        private JvmObjectReference _jvmObject;
        [NonSerialized]
        private readonly SparkContext _sc;
        [NonSerialized]
        private string _path;
        [NonSerialized]
        private JvmObjectReference _python_broadcast;
        private long _bid;

        internal Broadcast(SparkContext sc,
            object value,
            JvmObjectReference sparkContext,
            string path = null
            )
        {
            if (sc != null)
            {
                // We're on the driver
                _sc = sc;
                _path = Path.Combine(_sc._temp_dir, Path.GetRandomFileName());

                Version version = SparkEnvironment.SparkVersion;

                // Spark versions 2.3.0 and 2.3.1 create Broadcast variables using different logic
                if (version.Minor == 3 && (version.Build == 0 || version.Build == 1) )
                {
                    WriteBroadcastValueToFile(value);
                    JvmObjectReference javaSparkContext = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.java.JavaSparkContext",
                    "fromSparkContext",
                    sparkContext);
                    _jvmObject = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "readBroadcastFromFile",
                    javaSparkContext,
                    _path);
                }
                else
                {
                    SparkConf sparkConf = _sc.GetConf();
                    sc._encryption_enabled = bool.Parse(
                        sparkConf.Get("spark.io.encryption.enabled",
                        "false"));

                    _python_broadcast = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                        "org.apache.spark.api.python.PythonRDD",
                        "setupBroadcast",
                        _path);

                    if (sc._encryption_enabled)
                    {
                        throw new NotImplementedException(
                                "Broadcast encryption is not supported yet.");
                    }
                    else
                    {
                        WriteBroadcastValueToFile(value);
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
            else
            {
                // We're on an executor.
                _sc = null;
                _jvmObject = null;
                _python_broadcast = null;
                
                if (path != null)
                {
                    _path = path;
                }
                else
                {
                    throw new SystemException(
                        "broadcast called from executor with path not set.");
                }
            }
        }
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Function that creates a file to store the broadcast value in the given path
        /// </summary>
        /// <param name="value">Broadcast value to be written to the file</param>
        public void WriteBroadcastValueToFile(object value)
        {
            Directory.CreateDirectory(_sc._temp_dir);
            FileStream f = File.Create(_path);
            Dump(value, f);
            f.Close();
        }

        /// <summary>
        /// Function that serializes and stores the object passed to the given Stream
        /// </summary>
        /// <param name="value">Serializable object</param>
        /// <param name="stream">Stream to which the object is serialized</param>
        public void Dump(object value, Stream stream)
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
        public static ConcurrentDictionary<long, object> s_registry = new ConcurrentDictionary<
            long, 
            object>();
        public static ArrayList s_listBroadcastVariables;

        public BroadcastRegistry(IJvmBridge jvm)
        {
            s_listBroadcastVariables = new ArrayList(jvm);
        }
    }
}

