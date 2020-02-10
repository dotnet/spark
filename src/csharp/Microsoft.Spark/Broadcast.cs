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
        private JvmObjectReference _jvmObject;
        private readonly SparkContext _sc;
        private string _path;
        private object _python_broadcast;
        //private long _bid;
 
        internal Broadcast(SparkContext sc, object value, JvmObjectReference sparkContext)
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

                _jvmObject = (JvmObjectReference)sparkContext.Invoke(
                    "broadcast",
                    _python_broadcast);
                
            }
            else
            {
                // We're on an executor.
                _sc = null;
                _python_broadcast = null;

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
            //BroadcastRegistry broadcastRegistry = new BroadcastRegistry();
            //return broadcastRegistry.GetBroadcastValue(_bid);
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
    /// Thread-local registry for broadcast variables that have been broadcasted
    /// </summary>
    internal class BroadcastRegistry
    {
        private static Dictionary<long, object> _registry = new Dictionary<long, object>();
        internal BroadcastRegistry()
        {
            
        }

        public void AddBroadcastVariable(long id, object value)
        {
            _registry.Add(id, value);
        }

        public void RemoveBroadcastVariable(long id)
        {
            _registry.Remove(id);
        }

        public object GetBroadcastValue(long id)
        {
            return _registry[id];
        }
       
    }
  
}

