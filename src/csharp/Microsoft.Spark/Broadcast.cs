using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;

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
        private readonly string _path;
        [NonSerialized]
        private readonly JvmObjectReference _jvmObject;

        private readonly long _bid;

        internal Broadcast(SparkContext sc, T value)
        {
            _path = CreateTempFilePath(sc.GetConf());
            _jvmObject = CreateBroadcast(sc, value);
            _bid = (long)_jvmObject.Invoke("id");
        }

        public JvmObjectReference Reference => _jvmObject;

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
        }

        /// <summary>
        /// Serialization callback function that adds to the JvmBroadcastRegistry when the
        /// Broadcast variable object is being serialized.
        /// </summary>
        /// <param name="context">The current StreaminContext being used</param>
        [OnSerialized]
        internal void OnSerialized(StreamingContext context)
        {
            JvmBroadcastRegistry.Add(_jvmObject);
        }

        /// <summary>
        /// Function that creates a temporary directory inside the given directory and returns the
        /// absolute filepath of temporary file name in that directory.
        /// </summary>
        /// <param name="conf">SparkConf object</param>
        /// <returns>Absolute filepath of the created random file</returns>
        private string CreateTempFilePath(SparkConf conf)
        {
            var localDir = (string)conf.Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.util.Utils",
                "getLocalDir",
                conf);
            string dir = Path.Combine(localDir, "sparkdotnet");
            Directory.CreateDirectory(dir);
            return Path.Combine(dir, Path.GetRandomFileName());
        }

        /// <summary>
        /// Function to create the Broadcast variable (org.apache.spark.broadcast.Broadcast)
        /// </summary>
        /// <param name="sc">SparkContext object of type <see cref="SparkContext"/></param>
        /// <param name="value">Broadcast value of type object</param>
        /// <returns>Returns broadcast variable of type <see cref="JvmObjectReference"/></returns>
        private JvmObjectReference CreateBroadcast(SparkContext sc, T value)
        {
            var javaSparkContext = (JvmObjectReference)sc.Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.java.JavaSparkContext",
                "fromSparkContext",
                sc);

            Version version = SparkEnvironment.SparkVersion;
            return (version.Major, version.Minor) switch
            {
                (2, 4) => CreateBroadcast_V2_4_X(javaSparkContext, sc, value),
                (3, _) => CreateBroadcast_V2_4_X(javaSparkContext, sc, value),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }

        /// <summary>
        /// Calls the necessary Spark functions to create org.apache.spark.broadcast.Broadcast
        /// object for Spark versions 2.4.0 and above, and returns the JVMObjectReference object.
        /// </summary>
        /// <param name="javaSparkContext">Java Spark context object</param>
        /// <param name="sc">SparkContext object</param>
        /// <param name="value">Broadcast value of type object</param>
        /// <returns>Returns broadcast variable of type <see cref="JvmObjectReference"/></returns>
        private JvmObjectReference CreateBroadcast_V2_4_X(
            JvmObjectReference javaSparkContext,
            SparkContext sc,
            object value)
        {
            // Using SparkConf.Get() and passing default value of 'false' instead of  using 
            // PythonUtils.getEncryptionEnabled as the latter is a changing API wrt different
            // Spark versions.
            bool encryptionEnabled = bool.Parse(
                sc.GetConf().Get("spark.io.encryption.enabled", "false"));
            JvmObjectReference _pythonBroadcast;

            // Spark in Databricks is different from OSS Spark and requires to pass the SparkContext object to setupBroadcast.
            if (ConfigurationService.IsDatabricks)
            {
                _pythonBroadcast = (JvmObjectReference)javaSparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    javaSparkContext,
                    _path);
            }
            else
            {
                _pythonBroadcast = (JvmObjectReference)javaSparkContext.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD",
                    "setupBroadcast",
                    _path);
            }

            if (encryptionEnabled)
            {
                var pair = (JvmObjectReference[])_pythonBroadcast.Invoke("setupEncryptionServer");

                using (ISocketWrapper socket = SocketFactory.CreateSocket())
                {
                    socket.Connect(
                        IPAddress.Loopback,
                        (int)pair[0].Invoke("intValue"), // port number
                        (string)pair[1].Invoke("toString")); // secret
                    WriteToStream(value, socket.OutputStream);
                }
                _pythonBroadcast.Invoke("waitTillDataReceived");
            }
            else
            {
                WriteToFile(value);
            }

            return (JvmObjectReference)javaSparkContext.Invoke("broadcast", _pythonBroadcast);
        }

        /// TODO: This is not performant in the case of Broadcast encryption as it writes to stream
        /// only after serializing the whole value, instead of serializing and writing in chunks
        /// like Python.
        /// <summary>
        /// Function to write the broadcast value into the stream.
        /// </summary>
        /// <param name="value">Broadcast value to be written to the stream</param>
        /// <param name="stream">Stream to write value to</param>
        private void WriteToStream(object value, Stream stream)
        {
            using var ms = new MemoryStream();
            Dump(value, ms);
            SerDe.Write(stream, ms.Length);
            ms.WriteTo(stream);
            // -1 length indicates to the receiving end that we're done.
            SerDe.Write(stream, -1);
        }

        /// <summary>
        /// Function that creates a file in _path to store the broadcast value in the given path.
        /// </summary>
        /// <param name="value">Broadcast value to be written to the file</param>
        private void WriteToFile(object value)
        {
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
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(BroadcastRegistry));

        /// <summary>
        /// Function to add the value of the broadcast variable to s_registry.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object to add</param>
        /// <param name="value">Value of the Broadcast variable</param>
        internal static void Add(long bid, object value)
        {
            bool result = s_registry.TryAdd(bid, value);
            if (!result)
            {
                s_logger.LogInfo($"Broadcast {bid} already exists in the registry.");
            }
        }

        /// <summary>
        /// Function to remove the Broadcast variable from s_registry.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object to remove</param>
        internal static void Remove(long bid)
        {
            if (!s_registry.TryRemove(bid, out _))
            {
                s_logger.LogWarn($"Trying to remove a broadcast {bid} that does not exist.");
            }
        }

        /// <summary>
        /// Returns the value of the Broadcast variable object of given Id.
        /// </summary>
        /// <param name="bid">Id of the Broadcast variable object</param>
        /// <returns>Value of the Broadcast variable with given Id</returns>
        internal static object Get(long bid) => s_registry[bid];
    }

    /// <summary>
    /// Stores the JVMObjectReference object of type org.apache.spark.broadcast.Broadcast for all
    /// active broadcast variables that are sent to the workers through the CreatePythonFunction.
    /// This registry is only used on the driver side.
    /// </summary>
    internal static class JvmBroadcastRegistry
    {
        private static ThreadLocal<List<JvmObjectReference>> s_jvmBroadcastVariables = 
            new ThreadLocal<List<JvmObjectReference>>(() => new List<JvmObjectReference>());

        /// <summary>
        /// Adds a JVMObjectReference object of type <see cref="Broadcast{T}"/> to the list.
        /// </summary>
        /// <param name="broadcastJvmObject">JVMObjectReference of the Broadcast variable</param>
        internal static void Add(JvmObjectReference broadcastJvmObject) =>
            s_jvmBroadcastVariables.Value.Add(broadcastJvmObject);

        /// <summary>
        /// Clears s_jvmBroadcastVariables of all the JVMObjectReference objects of type
        /// <see cref="Broadcast{T}"/>.
        /// </summary>
        internal static void Clear() => s_jvmBroadcastVariables.Value.Clear();

        /// <summary>
        /// Returns the static member s_jvmBroadcastVariables.
        /// </summary>
        /// <returns>A list of all broadcast objects of type <see cref="JvmObjectReference"/></returns>
        internal static List<JvmObjectReference> GetAll() => s_jvmBroadcastVariables.Value;
    }
}
