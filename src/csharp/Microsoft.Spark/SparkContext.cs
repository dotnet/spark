// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Hadoop.Conf;
using Microsoft.Spark.Interop.Internal.Scala;
using Microsoft.Spark.Interop.Ipc;
using static Microsoft.Spark.Utils.CommandSerDe;

namespace Microsoft.Spark
{
    /// <summary>
    /// Main entry point for Spark functionality. A SparkContext represents the connection
    /// to a Spark cluster, and can be used to create RDDs, accumulators and broadcast
    /// variables on that cluster.
    /// 
    /// Only one `SparkContext` should be active per JVM. You must `stop()` the
    /// active `SparkContext` before creating a new one.
    /// </summary>
    public sealed class SparkContext : IJvmObjectReferenceProvider
    {
        private readonly SparkConf _conf;

        /// <summary>
        /// Create a SparkContext object with the given config.
        /// </summary>
        /// <param name="conf">a Spark config object describing the application configuration.
        /// Any settings in this config overrides the default configs as well as system properties.
        /// </param>
        public SparkContext(SparkConf conf)
            : this(conf.Reference.Jvm.CallConstructor("org.apache.spark.SparkContext", conf))
        {
        }

        /// <summary>
        /// Create a SparkContext that loads settings from system properties (for instance,
        /// when launching with spark-submit).
        /// </summary>
        public SparkContext()
            : this(new SparkConf())
        {
        }

        /// <summary>
        /// Alternative constructor that allows setting common Spark properties directly.
        /// </summary>
        /// <param name="master">Cluster URL to connect to (e.g. spark://host:port, local)</param>
        /// <param name="appName">A name for the application</param>
        /// <param name="conf">
        /// A <see cref="SparkConf"/> object specifying other Spark parameters
        /// </param>
        public SparkContext(string master, string appName, SparkConf conf)
            : this(GetUpdatedConf(master, appName, null, conf))
        {
        }

        /// <summary>
        /// Initializes a SparkContext instance with a specific master and application name.
        /// </summary>
        /// <param name="master">Cluster URL to connect to (e.g. spark://host:port, local)</param>
        /// <param name="appName">A name for the application</param>
        public SparkContext(string master, string appName)
            : this(GetUpdatedConf(master, appName, null, null))
        {
        }

        /// <summary>
        /// Alternative constructor that allows setting common Spark properties directly.
        /// </summary>
        /// <param name="master">Cluster URL to connect to (e.g. spark://host:port, local)</param>
        /// <param name="appName">A name for the application</param>
        /// <param name="sparkHome">The path that holds spark bits</param>
        public SparkContext(string master, string appName, string sparkHome)
            : this(GetUpdatedConf(master, appName, sparkHome, null))
        {
        }

        /// <summary>
        /// Constructor where SparkContext object is already created.
        /// </summary>
        /// <param name="jvmObject">JVM object reference for this SparkContext object</param>
        internal SparkContext(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            _conf = new SparkConf((JvmObjectReference)Reference.Invoke("getConf"));
        }


        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns SparkConf object associated with this SparkContext object.
        /// Note that modifying the SparkConf object will not have any impact.
        /// </summary>
        /// <returns>SparkConf object</returns>
        public SparkConf GetConf() => _conf;

        /// <summary>
        /// This function may be used to get or instantiate a SparkContext and register it as a
        /// singleton object. Because we can only have one active SparkContext per JVM,
        /// this is useful when applications may wish to share a SparkContext.
        /// </summary>
        /// <param name="conf"><see cref="SparkConf"/> that will be used for creating SparkContext
        /// </param>
        /// <returns>
        /// Current SparkContext (or a new one if it wasn't created before the function call)
        /// </returns>
        public static SparkContext GetOrCreate(SparkConf conf)
        {
            IJvmBridge jvm = conf.Reference.Jvm;
            return new SparkContext(
                (JvmObjectReference)jvm.CallStaticJavaMethod(
                    "org.apache.spark.SparkContext",
                    "getOrCreate",
                    conf));
        }

        /// <summary>
        /// Control our logLevel. This overrides any user-defined log settings.
        /// </summary>
        /// <remarks>
        /// Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
        /// </remarks>
        /// <param name="logLevel">The desired log level as a string.</param>
        public void SetLogLevel(string logLevel)
        {
            Reference.Invoke("setLogLevel", logLevel);
        }

        /// <summary>
        /// Shut down the SparkContext.
        /// </summary>
        public void Stop()
        {
            Reference.Invoke("stop");
        }

        /// <summary>
        /// Default level of parallelism to use when not given by user (e.g. Parallelize()).
        /// </summary>
        public int DefaultParallelism => (int)Reference.Invoke("defaultParallelism");

        /// <summary>
        /// Creates a modified version of <see cref="SparkConf"/> with the parameters that can be
        /// passed separately to SparkContext, to make it easier to write SparkContext's
        /// constructors.
        /// </summary>
        /// <param name="master">Cluster URL to connect to (e.g. spark://host:port, local)</param>
        /// <param name="appName">A name for the application</param>
        /// <param name="sparkHome">The path that holds spark bits</param>
        /// <param name="conf">
        /// A <see cref="SparkConf"/> object specifying other Spark parameters
        /// </param>
        /// <returns>Modified <see cref="SparkConf"/> object.</returns>
        private static SparkConf GetUpdatedConf(
            string master,
            string appName,
            string sparkHome,
            SparkConf conf)
        {
            SparkConf sparkConf = conf ?? new SparkConf();
            if (master != null)
            {
                sparkConf.SetMaster(master);
            }
            if (appName != null)
            {
                sparkConf.SetAppName(appName);
            }
            if (sparkHome != null)
            {
                sparkConf.SetSparkHome(sparkHome);
            }

            return sparkConf;
        }

        /// <summary>
        /// Sets a human readable description of the current job.
        /// </summary>
        /// <param name="value">Description of the current job</param>
        public void SetJobDescription(string value)
        {
            Reference.Invoke("setJobDescription", value);
        }

        /// <summary>
        /// Assigns a group ID to all the jobs started by this thread until the group ID is set to
        /// a different value or cleared.
        /// </summary>
        /// <remarks>
        /// Often, a unit of execution in an application consists of multiple Spark actions or
        /// jobs. Application programmers can use this method to group all those jobs together
        /// and give a group description. Once set, the Spark web UI will associate such jobs
        /// with this group.
        /// </remarks>
        /// <param name="groupId">Group Id</param>
        /// <param name="description">Description on the job group</param>
        /// <param name="interruptOnCancel">
        /// If true, then job cancellation will result in `Thread.interrupt()` being called on the
        /// job's executor threads. 
        /// </param>
        public void SetJobGroup(string groupId, string description, bool interruptOnCancel = false)
        {
            Reference.Invoke("setJobGroup", groupId, description, interruptOnCancel);
        }

        /// <summary>
        /// Clear the current thread's job group ID and its description.
        /// </summary>
        public void ClearJobGroup()
        {
            Reference.Invoke("clearJobGroup");
        }

        /// <summary>
        /// Distribute a local collection to form an RDD.
        /// </summary>
        /// <typeparam name="T">Type of the elements in the collection</typeparam>
        /// <param name="seq">Collection to distribute</param>
        /// <param name="numSlices">Number of partitions to divide the collection into</param>
        /// <returns>RDD representing distributed collection</returns>
        internal RDD<T> Parallelize<T>(IEnumerable<T> seq, int? numSlices = null)
        {
            var formatter = new BinaryFormatter();
            using var memoryStream = new MemoryStream();

            var values = new List<byte[]>();
            foreach (T obj in seq)
            {
                formatter.Serialize(memoryStream, obj);
                values.Add(memoryStream.ToArray());
                memoryStream.SetLength(0);
            }

            return new RDD<T>(
                (JvmObjectReference)Reference.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.api.dotnet.DotnetRDD",
                    "createJavaRDDFromArray",
                    Reference,
                    values,
                    numSlices ?? DefaultParallelism),
                this,
                SerializedMode.Byte);
        }

        /// <summary>
        /// Read a text file from HDFS, a local file system (available on all nodes), or any
        /// Hadoop-supported file system URI, and return it as an RDD of strings.
        /// </summary>
        /// <param name="path">path to the text file on a supported file system</param>
        /// <param name="minPartitions">minimum number of partitions for the resulting RDD</param>
        /// <returns>RDD of lines of the text file</returns>
        internal RDD<string> TextFile(string path, int? minPartitions = null)
        {
            return new RDD<string>(
                WrapAsJavaRDD((JvmObjectReference)Reference.Invoke(
                    "textFile",
                    path,
                    minPartitions ?? DefaultParallelism)),
                this,
                SerializedMode.String);
        }

        /// <summary>
        /// Add a file to be downloaded with this Spark job on every node.
        /// </summary>
        /// <remarks>
        /// If a file is added during execution, it will not be available until the next
        /// TaskSet starts.
        /// 
        /// A path can be added only once. Subsequent additions of the same path are ignored.
        /// </remarks>
        /// <param name="path">
        /// File path can be either a local file, a file in HDFS (or other Hadoop-supported
        /// filesystems), or an HTTP, HTTPS or FTP URI.
        /// </param>
        /// <param name="recursive">
        /// If true, a directory can be given in `path`. Currently directories are supported
        /// only for Hadoop-supported filesystems.
        /// </param>
        public void AddFile(string path, bool recursive = false)
        {
            Reference.Invoke("addFile", path, recursive);
        }

        /// <summary>
        /// Returns a list of file paths that are added to resources.
        /// </summary>
        /// <returns>File paths that are added to resources.</returns>
        public IEnumerable<string> ListFiles() =>
            new Seq<string>((JvmObjectReference)Reference.Invoke("listFiles"));

        /// <summary>
        /// Add an archive to be downloaded and unpacked with this Spark job on every node.
        /// </summary>
        /// <remarks>
        /// If an archive is added during execution, it will not be available until the next
        /// TaskSet starts.
        /// 
        /// A path can be added only once. Subsequent additions of the same path are ignored.
        /// </remarks>
        /// <param name="path">
        /// Archive path can be either a local file, a file in HDFS (or other Hadoop-supported
        /// filesystems), or an HTTP, HTTPS or FTP URI. The given path should be one of .zip,
        /// .tar, .tar.gz, .tgz and .jar.
        /// </param>
        [Since(Versions.V3_1_0)]
        public void AddArchive(string path)
        {
            Reference.Invoke("addArchive", path);
        }

        /// <summary>
        /// Returns a list of archive paths that are added to resources.
        /// </summary>
        /// <returns>Archive paths that are added to resources.</returns>
        [Since(Versions.V3_1_0)]
        public IEnumerable<string> ListArchives() =>
            new Seq<string>((JvmObjectReference)Reference.Invoke("listArchives"));

        /// <summary>
        /// Sets the directory under which RDDs are going to be checkpointed.
        /// </summary>
        /// <param name="directory">
        /// path to the directory where checkpoint files will be stored
        /// </param>
        public void SetCheckpointDir(string directory)
        {
            Reference.Invoke("setCheckpointDir", directory);
        }

        /// <summary>
        /// Return the directory where RDDs are checkpointed.
        /// </summary>
        /// <returns>
        /// The directory where RDDs are checkpointed. Returns `null` if no checkpoint
        /// directory has been set.
        /// </returns>
        public string GetCheckpointDir()
        {
            return (string)new Option((JvmObjectReference)Reference.Invoke("getCheckpointDir")).OrNull();
        }

        /// <summary>
        /// Broadcast a read-only variable to the cluster, returning a Microsoft.Spark.Broadcast
        /// object for reading it in distributed functions. The variable will be sent to each 
        /// executor only once.
        /// </summary>
        /// <typeparam name="T">Type of the variable being broadcast</typeparam>
        /// <param name="value">Value of the broadcast variable</param>
        /// <returns>A Broadcast object of type <see cref="Broadcast{T}(T)"/></returns>
        public Broadcast<T> Broadcast<T>(T value)
        {
            return new Broadcast<T>(this, value);
        }

        /// <summary>
        /// A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
        /// </summary>
        /// <returns>The Hadoop Configuration.</returns>
        public Configuration HadoopConfiguration() =>
            new Configuration((JvmObjectReference)Reference.Invoke("hadoopConfiguration"));

        /// <summary>
        /// Returns JVM object reference to JavaRDD object transformed
        /// from a Scala RDD object.
        /// </summary>
        /// <remarks>
        /// The transformation is for easy reflection on the JVM side.
        /// </remarks>
        /// <param name="rdd">JVM object reference to Scala RDD</param>
        /// <returns>JVM object reference to JavaRDD object</returns>
        private JvmObjectReference WrapAsJavaRDD(JvmObjectReference rdd)
        {
            return (JvmObjectReference)Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.dotnet.DotnetRDD",
                "toJavaRDD",
                rdd);
        }
        /// <summary>
        /// Returns a string that represents the version of Spark on which this application is running.
        /// </summary>
        /// <returns>
        /// A string that represents the version of Spark on which this application is running.
        /// </returns>
        public string Version() => (string)Reference.Invoke("version");
    }
}
