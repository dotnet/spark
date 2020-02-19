using System;
using System.IO;
using System.Collections.Concurrent;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Network;

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
        private JvmObjectReference _sparkContext;
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
            string path = null,
            Stream socketStream = null
            )
        {
            if (sc != null)
            {
                // We're on the driver
                _sc = sc;
                _sparkContext = sparkContext;
                _path = Path.Combine(_sc._temp_dir, Path.GetRandomFileName());
                SparkConf sparkConf = _sc.GetConf();
                sc._encryption_enabled = bool.Parse(sparkConf.Get("spark.io.encryption.enabled", "false"));

                _python_broadcast = (JvmObjectReference)sparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    _path);
                
                BroadcastRegistry bRegistry = new BroadcastRegistry(sparkContext.Jvm);
                Array encryptionPortSecret = null;
                if (sc._encryption_enabled)
                {
                    encryptionPortSecret = (Array)_python_broadcast.Invoke("setupEncryptionServer");

                    JvmObjectReference jPort = (JvmObjectReference)encryptionPortSecret.GetValue(0);
                    int port = int.Parse((string)jPort.Invoke("toString"));
                    JvmObjectReference jSecret = (JvmObjectReference)encryptionPortSecret.GetValue(1);
                    string secret = (string)jSecret.Invoke("toString");

                    using (ISocketWrapper socket = SocketFactory.CreateSocket())
                    {
                        Directory.CreateDirectory(_sc._temp_dir);
                        socket.Connect(IPAddress.Loopback, port, secret);
                        dump(value, socket.OutputStream);
                        socket.OutputStream.Flush();
                        _python_broadcast.Invoke("waitTillDataReceived");
                    }
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
                _bid = (long)_jvmObject.Invoke("id");
                BroadcastRegistry.listBroadcastVariables.Add(_jvmObject);
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

        public void dump(object value, Stream stream)
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
            return BroadcastRegistry._registry[_bid];
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
    /// Registry for broadcast variables that have been broadcasted.
    /// </summary>
    internal class BroadcastRegistry
    {
        public static ConcurrentDictionary<long, object> _registry = new ConcurrentDictionary<long, object>();
        public static ArrayList listBroadcastVariables;

        public BroadcastRegistry(IJvmBridge jvm)
        {
            listBroadcastVariables = new ArrayList(jvm);
        }
    }
}

