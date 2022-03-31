// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Utils;
using static Microsoft.Spark.Utils.CommandSerDe;

namespace Microsoft.Spark
{
    /// <summary>
    /// A Resilient Distributed Dataset(RDD), the basic abstraction in Spark,
    /// represents an immutable, partitioned collection of elements that can be
    /// operated on in parallel. This class contains the basic operations available
    /// on all RDDs.
    /// </summary>
    /// <typeparam name="T">Type of the elements in the RDD</typeparam>
    internal class RDD<T> : IJvmObjectReferenceProvider
    {
        /// <summary>
        /// The JVM object for this RDD. This can be null if the current
        /// RDD is not materialized.
        /// </summary>
        internal JvmObjectReference _jvmObject = null;

        /// <summary>
        /// The previous RDD object that this RDD references to. This is
        /// used by <see cref="PipelinedRDD{T}"/> to chain RDD operations.
        /// </summary>
        internal readonly JvmObjectReference _prevRddJvmObjRef = null;

        /// <summary>
        /// SparkContext object associated with the RDD.
        /// </summary>
        internal readonly SparkContext _sparkContext = null;

        /// <summary>
        /// Flag that checks if <see cref="Cache"/> is called.
        /// </summary>
        protected bool _isCached = false;

        /// <summary>
        /// Flag that checks if <see cref="Checkpoint"/> is called.
        /// </summary>
        protected bool _isCheckpointed = false;

        /// <summary>
        /// Serialization mode for the current RDD. This will be
        /// translated into serialization mode while creating a serialized command.
        /// </summary>
        internal SerializedMode _serializedMode = SerializedMode.Byte;

        /// <summary>
        /// Serialization mode for the previously pipelined RDD. This will be
        /// translated into deserialization mode while creating a serialized command.
        /// </summary>
        internal SerializedMode _prevSerializedMode = SerializedMode.Byte;

        /// <summary>
        /// Constructor mainly called by SparkContext for creating the first RDD
        /// via <see cref="SparkContext.Parallelize{T}(IEnumerable{T}, int?)"/>, etc.
        /// </summary>
        /// <param name="jvmObject">The reference to the RDD JVM object</param>
        /// <param name="sparkContext">SparkContext object</param>
        /// <param name="serializedMode">Serialization mode for the current RDD</param>
        internal RDD(
            JvmObjectReference jvmObject,
            SparkContext sparkContext,
            SerializedMode serializedMode)
        {
            _jvmObject = jvmObject;
            _sparkContext = sparkContext;
            _serializedMode = serializedMode;
        }

        /// <summary>
        /// Constructor mainly called by <see cref="PipelinedRDD{T}"/>.
        /// </summary>
        /// <param name="prevRddJvmObjRef">
        /// The reference to the RDD JVM object from which pipeline is created
        /// </param>
        /// <param name="sparkContext">SparkContext object</param>
        /// <param name="serializedMode">Serialization mode for the current RDD</param>
        /// <param name="prevSerializedMode">Serialization mode for the previous RDD</param>
        internal RDD(
            JvmObjectReference prevRddJvmObjRef,
            SparkContext sparkContext,
            SerializedMode serializedMode,
            SerializedMode prevSerializedMode)
        {
            // This constructor is called from PipelineRDD constructor
            // where the _jvmObject is not yet created.

            _prevRddJvmObjRef = prevRddJvmObjRef;
            _sparkContext = sparkContext;
            _serializedMode = serializedMode;
            _prevSerializedMode = prevSerializedMode;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Persist this RDD with the default storage level (MEMORY_ONLY).
        /// </summary>
        /// <returns></returns>
        public RDD<T> Cache()
        {
            GetJvmRef().Invoke("cache");
            _isCached = true;
            return this;
        }

        /// <summary>
        /// Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
        /// directory set with <see cref="SparkContext.SetCheckpointDir(string)"/> and all
        /// references to its parent RDDs will be removed. This function must be called before
        /// any job has been executed on this RDD. It is strongly recommended that this RDD is
        /// persisted in memory, otherwise saving it in a file will require re-computation.
        /// </summary>
        public void Checkpoint()
        {
            GetJvmRef().Invoke("checkpoint");
            _isCheckpointed = true;
        }

        /// <summary>
        /// Return a new RDD by applying a function to all elements of this RDD.
        /// </summary>
        /// <typeparam name="U">Type of the new RDD elements</typeparam>
        /// <param name="func">Function to apply</param>
        /// <param name="preservesPartitioning">Flag to preserve partitioning</param>
        /// <returns>New RDD by applying a given function</returns>
        public RDD<U> Map<U>(Func<T, U> func, bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndexInternal<U>(
                new MapUdfWrapper<T, U>(func).Execute,
                preservesPartitioning);
        }

        /// <summary>
        /// Return a new RDD by first applying a function to all elements of this RDD,
        /// and then flattening the results.
        /// </summary>
        /// <typeparam name="U">Type of the new RDD elements</typeparam>
        /// <param name="func">Function to apply</param>
        /// <param name="preservesPartitioning">Flag to preserve partitioning</param>
        /// <returns>New RDD by applying a given function</returns>
        public RDD<U> FlatMap<U>(Func<T, IEnumerable<U>> func, bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndexInternal<U>(
                new FlatMapUdfWrapper<T, U>(func).Execute,
                preservesPartitioning);
        }

        /// <summary>
        /// Return a new RDD by applying a function to each partition of this RDD.
        /// </summary>
        /// 
        /// <remarks>
        /// "preservesPartitioning" indicates whether the input function preserves the
        /// partitioner, which should be false unless this is a pair RDD and the input
        /// function doesn't modify the keys.
        /// </remarks>
        /// <typeparam name="U">Type of the new RDD elements</typeparam>
        /// <param name="func">Function to apply</param>
        /// <param name="preservesPartitioning">Flag to preserve partitioning</param>
        /// <returns>New RDD by applying a given function</returns>
        public RDD<U> MapPartitions<U>(
            Func<IEnumerable<T>, IEnumerable<U>> func,
            bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndexInternal<U>(
                new MapPartitionsUdfWrapper<T, U>(func).Execute,
                preservesPartitioning);
        }

        /// <summary>
        /// Return a new RDD by applying a function to each partition of this RDD,
        /// while tracking the index of the original partition.
        /// </summary>
        /// <remarks>
        /// "preservesPartitioning" indicates whether the input function preserves the
        /// partitioner, which should be false unless this is a pair RDD and the input
        /// function doesn't modify the keys.
        /// </remarks>
        /// <typeparam name="U">Type of the new RDD elements</typeparam>
        /// <param name="func">Function to apply</param>
        /// <param name="preservesPartitioning">Flag to preserve partitioning</param>
        /// <returns>New RDD by applying a given function</returns>
        public RDD<U> MapPartitionsWithIndex<U>(
            Func<int, IEnumerable<T>, IEnumerable<U>> func,
            bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndexInternal<U>(
                new MapPartitionsWithIndexUdfWrapper<T, U>(func).Execute,
                preservesPartitioning);
        }

        /// <summary>
        /// Return the number of partitions in this RDD.
        /// </summary>
        /// <returns>The number of partitions in this RDD</returns>
        public int GetNumPartitions()
        {
            return (int)GetJvmRef().Invoke("getNumPartitions");
        }

        /// <summary>
        /// Return a new RDD containing only the elements that satisfy a predicate.
        /// </summary>
        /// <param name="func">Predicate function to apply</param>
        /// <returns>A new RDD with elements that satisfy a predicate</returns>
        public RDD<T> Filter(Func<T, bool> func)
        {
            return MapPartitionsWithIndexInternal<T>(new FilterUdfWrapper(func).Execute, true);
        }

        /// <summary>
        /// Return a sampled subset of this RDD with a seed.
        /// </summary>
        /// <remarks>
        /// This is NOT guaranteed to provide exactly the fraction of the count
        /// of the given RDD.
        /// </remarks>
        /// <param name="withReplacement">True if elements be sampled multiple times</param>
        /// <param name="fraction">
        /// Expected size of the sample as a fraction of this RDD's size without replacement
        /// </param>
        /// <param name="seed">Optional user-supplied seed (random seed if not provided)</param>
        /// <returns>A sampled subset of this RDD</returns>
        public RDD<T> Sample(bool withReplacement, double fraction, long? seed = null)
        {
            return new RDD<T>(
                (seed.HasValue) ?
                (JvmObjectReference)GetJvmRef().Invoke(
                    "sample",
                    withReplacement,
                    fraction,
                    seed.GetValueOrDefault()) :
                (JvmObjectReference)GetJvmRef().Invoke(
                    "sample",
                    withReplacement,
                    fraction),
                _sparkContext,
                _serializedMode);
        }

        /// <summary>
        /// Return an enumerable collection that contains all of the elements in this RDD.
        /// </summary>
        /// <remarks>
        /// This method should only be used if the resulting array is expected to be small,
        /// as all the data is loaded into the driver's memory.
        /// </remarks>
        /// <returns>An enumerable collection of all the elements.</returns>
        public IEnumerable<T> Collect()
        {
            (int port, string secret) = CollectAndServe();
            using ISocketWrapper socket = SocketFactory.CreateSocket();
            socket.Connect(IPAddress.Loopback, port, secret);

            var collector = new RDD.Collector();
            System.IO.Stream stream = socket.InputStream;
            foreach (T element in collector.Collect(stream, _serializedMode).Cast<T>())
            {
                yield return element;
            }
        }

        /// <summary>
        /// Helper function for creating PipelinedRDD.
        /// </summary>
        /// <typeparam name="U">Type of the new RDD elements</typeparam>
        /// <param name="func">Function to apply</param>
        /// <param name="preservesPartitioning">Flag to preserve partitioning</param>
        /// <returns>New RDD by applying a given function</returns>
        internal virtual RDD<U> MapPartitionsWithIndexInternal<U>(
            RDD.WorkerFunction.ExecuteDelegate func,
            bool preservesPartitioning = false)
        {
            return new PipelinedRDD<U>(
                new RDD.WorkerFunction(func),
                preservesPartitioning,
                _jvmObject,
                _sparkContext,
                _serializedMode);
        }

        /// <summary>
        /// Returns the socket info by calling collectAndServe on the RDD object.
        /// </summary>
        /// <returns>Socket info</returns>
        private (int, string) CollectAndServe()
        {
            JvmObjectReference rddRef = GetJvmRef();
            // collectToPython() returns a pair where the first is a port number
            // and the second is the secret string to use for the authentication.
            var pair = (JvmObjectReference[])rddRef.Jvm.CallStaticJavaMethod(
                "org.apache.spark.api.python.PythonRDD",
                "collectAndServe",
                rddRef.Invoke("rdd"));
            return ((int)pair[0].Invoke("intValue"), (string)pair[1].Invoke("toString"));
        }

        /// <summary>
        /// Returns the JvmObjectReference object of this RDD.
        /// </summary>
        /// <remarks>
        /// It is possible that the JvmObjectReference object is null depending
        /// on how the RDD object is instantiated (e.g., <see cref="PipelinedRDD{U}"/>). Thus,
        /// this function should be used instead of directly accessing _jvmObject because the
        /// derived class (e.g., <see cref="PipelinedRDD{U}"/>) can override the behavior
        /// when _jvmObject is null.
        /// </remarks>
        /// <returns>JvmObjetReference object for this RDD</returns>
        private JvmObjectReference GetJvmRef()
        {
            return ((IJvmObjectReferenceProvider)this).Reference;
        }

        /// <summary>
        /// Helper to map the UDF for Map() to <see cref="RDD.WorkerFunction.ExecuteDelegate"/>.
        /// </summary>
        /// <typeparam name="TArg">Input type</typeparam>
        /// <typeparam name="TResult">Output type</typeparam>
        [UdfWrapper]
        internal sealed class MapUdfWrapper<TArg, TResult>
        {
            private readonly Func<TArg, TResult> _func;

            internal MapUdfWrapper(Func<TArg, TResult> func)
            {
                _func = func;
            }

            internal IEnumerable<object> Execute(int _, IEnumerable<object> input)
            {
                return input.Cast<TArg>().Select(_func).Cast<object>();
            }
        }

        /// <summary>
        /// Helper to map the UDF for FlatMap() to <see cref="RDD.WorkerFunction.ExecuteDelegate"/>
        /// </summary>
        /// <typeparam name="TArg">Input type</typeparam>
        /// <typeparam name="TResult">Output type</typeparam>
        [UdfWrapper]
        internal sealed class FlatMapUdfWrapper<TArg, TResult>
        {
            private readonly Func<TArg, IEnumerable<TResult>> _func;

            internal FlatMapUdfWrapper(Func<TArg, IEnumerable<TResult>> func)
            {
                _func = func;
            }

            internal IEnumerable<object> Execute(int _, IEnumerable<object> input)
            {
                return input.Cast<TArg>().SelectMany(_func).Cast<object>();
            }
        }

        /// <summary>
        /// Helper to map the UDF for MapPartitions() to
        /// <see cref="RDD.WorkerFunction.ExecuteDelegate"/>.
        /// </summary>
        /// <typeparam name="TArg">Input type</typeparam>
        /// <typeparam name="TResult">Output type</typeparam>
        [UdfWrapper]
        internal sealed class MapPartitionsUdfWrapper<TArg, TResult>
        {
            private readonly Func<IEnumerable<TArg>, IEnumerable<TResult>> _func;

            internal MapPartitionsUdfWrapper(Func<IEnumerable<TArg>, IEnumerable<TResult>> func)
            {
                _func = func;
            }

            internal IEnumerable<object> Execute(int _, IEnumerable<object> input)
            {
                return _func(input.Cast<TArg>()).Cast<object>();
            }
        }

        /// <summary>
        /// Helper to map the UDF for MapPartitionsWithIndex() to
        /// <see cref="RDD.WorkerFunction.ExecuteDelegate"/>.
        /// </summary>
        /// <typeparam name="TArg">Input type</typeparam>
        /// <typeparam name="TResult">Output type</typeparam>
        [UdfWrapper]
        internal sealed class MapPartitionsWithIndexUdfWrapper<TArg, TResult>
        {
            private readonly Func<int, IEnumerable<TArg>, IEnumerable<TResult>> _func;

            internal MapPartitionsWithIndexUdfWrapper(
                Func<int, IEnumerable<TArg>, IEnumerable<TResult>> func)
            {
                _func = func;
            }

            internal IEnumerable<object> Execute(int pid, IEnumerable<object> input)
            {
                return _func(pid, input.Cast<TArg>()).Cast<object>();
            }
        }

        /// <summary>
        /// Helper to map the UDF for Filter() to
        /// <see cref="RDD.WorkerFunction.ExecuteDelegate"/>.
        /// </summary>
        [UdfWrapper]
        internal class FilterUdfWrapper
        {
            private readonly Func<T, bool> _func;

            internal FilterUdfWrapper(Func<T, bool> func)
            {
                _func = func;
            }

            internal IEnumerable<object> Execute(int _, IEnumerable<object> input)
            {
                return input.Cast<T>().Where(_func).Cast<object>();
            }
        }
    }

    /// <summary>
    /// PipelinedRDD is used to pipeline functions applied to RDD.
    /// </summary>
    /// <typeparam name="T">Type of the elements in the RDD</typeparam>
    internal sealed class PipelinedRDD<T> : RDD<T>, IJvmObjectReferenceProvider
    {
        private readonly RDD.WorkerFunction _func;
        private readonly bool _preservesPartitioning;

        internal PipelinedRDD(
            RDD.WorkerFunction func,
            bool preservesPartitioning,
            JvmObjectReference prevRddJvmObjRef,
            SparkContext sparkContext,
            SerializedMode prevSerializedMode)
            : base(prevRddJvmObjRef, sparkContext, SerializedMode.Byte, prevSerializedMode)
        {
            _func = func ?? throw new ArgumentNullException("UDF cannot be null.");
            _preservesPartitioning = preservesPartitioning;
        }

        /// <summary>
        /// Return a new RDD by applying a function to each partition of this RDD,
        /// while tracking the index of the original partition.
        /// </summary>
        /// <typeparam name="U">The element type of new RDD</typeparam>
        /// <param name="newFunc">The function to be applied to each partition</param>
        /// <param name="preservesPartitioning">
        /// Indicates if it preserves partition parameters
        /// </param>
        /// <returns>A new RDD</returns>
        internal override RDD<U> MapPartitionsWithIndexInternal<U>(
            RDD.WorkerFunction.ExecuteDelegate newFunc,
            bool preservesPartitioning = false)
        {
            if (IsPipelinable())
            {
                RDD.WorkerFunction newWorkerFunc = RDD.WorkerFunction.Chain(
                    new RDD.WorkerFunction(_func.Func),
                    new RDD.WorkerFunction(newFunc));

                return new PipelinedRDD<U>(
                    newWorkerFunc,
                    preservesPartitioning && _preservesPartitioning,
                    _prevRddJvmObjRef,
                    _sparkContext,
                    _serializedMode);
            }

            return base.MapPartitionsWithIndexInternal<U>(newFunc, preservesPartitioning);
        }

        /// <summary>
        /// Returns the JVM reference for this RDD. It also initializes the reference
        /// if it is not yet initialized.
        /// 
        /// Note that PipelineRDD uses the JavaRDD internally.
        /// </summary>
        JvmObjectReference IJvmObjectReferenceProvider.Reference
        {
            get
            {
                if (_jvmObject == null)
                {
                    IJvmBridge jvm = _prevRddJvmObjRef.Jvm;

                    object rdd = _prevRddJvmObjRef.Invoke("rdd");
                    byte[] command = Serialize(_func.Func, _prevSerializedMode, _serializedMode);
                    JvmObjectReference pythonFunction =
                        UdfUtils.CreatePythonFunction(jvm, command);

                    var pythonRdd = (JvmObjectReference)jvm.CallStaticJavaMethod(
                        "org.apache.spark.api.dotnet.DotnetRDD",
                        "createPythonRDD",
                        rdd,
                        pythonFunction,
                        _preservesPartitioning);

                    _jvmObject = (JvmObjectReference)pythonRdd.Invoke("asJavaRDD");
                }

                return _jvmObject;
            }
        }

        /// <summary>
        /// Checks whether worker functions can be pipelined.
        /// </summary>
        /// <returns>True if worker functions can be pipelined.</returns>
        private bool IsPipelinable()
        {
            return !_isCached && !_isCheckpointed;
        }
    }
}
