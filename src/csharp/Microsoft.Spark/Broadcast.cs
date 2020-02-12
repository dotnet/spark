using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Interop;

namespace Microsoft.Spark
{
    /// <summary>
    /// 
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
        private object _python_broadcast;

        internal Broadcast(SparkContext sc,
            object value,
            JvmObjectReference sparkContext,
            string path = null,
            string sock_file = null
            )
        {
            if (sc != null)
            {
                // We're on the driver
                _sc = sc;
                _path = Path.Combine(_sc._temp_dir, Path.GetRandomFileName());
                _python_broadcast = sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    _path);
                if (sc._encryption_enabled)
                {
                    throw new NotImplementedException(
                            "Encryption not supported yet");
                }
                else
                {
                    Directory.CreateDirectory(_sc._temp_dir);
                    FileStream f = File.Create(_path);
                    dump(value, f);
                    f.Close();
                }

                _jvmObject = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.sql.api.dotnet.SQLUtils",
                    "createBroadcast",
                    sparkContext,
                    _python_broadcast);
            }
            else
            {
                // We're on an executor.
                _sc = null;
                _jvmObject = null;
                _python_broadcast = null;
                if (sock_file != null)
                {
                    throw new NotImplementedException(
                        "broadcastDecryptionServer is not implemented.");
                }
                else
                {
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
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        public void dump(object value, FileStream fStream)
        {
            var formatter = new BinaryFormatter();
            formatter.Serialize(fStream, value);
        }


        /// <summary>
        /// Get the broadcasted value.
        /// </summary>
        /// <returns>The broadcasted value</returns>
        public object Value()
        {
            return _jvmObject.Invoke("value");
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
    /// Registry for broadcast variables that have been broadcasted
    /// </summary>
    internal static class BroadcastRegistry
    {
        public static Dictionary<long, object> _registry = new Dictionary<long, object>();
    }
  
}

