// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// DataStreamWriter provides functionality to write a streaming <see cref="DataFrame"/>
    /// to external storage systems (e.g. file systems, key-value stores, etc).
    /// </summary>
    public sealed class DataStreamWriter : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;
        private readonly DataFrame _df;

        internal DataStreamWriter(JvmObjectReference jvmObject, DataFrame df)
        {
            _jvmObject = jvmObject;
            _df = df;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Specifies how data of a streaming DataFrame is written to a streaming sink.
        /// </summary>
        /// <remarks>
        /// The following mode is supported:
        /// "append": Only the new rows in the streaming DataFrame/Dataset will be written to
        ///           the sink.
        /// "complete": All the rows in the streaming DataFrame/Dataset will be written to the sink
        ///             every time there are some updates.
        /// "update": Only the rows that were updated in the streaming DataFrame will
        ///           be written to the sink every time there are some updates. If the query
        ///           doesn't contain aggregations, it will be equivalent to `append` mode.
        /// </remarks>
        /// <param name="outputMode">Output mode name</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter OutputMode(string outputMode)
        {
            _jvmObject.Invoke("outputMode", outputMode);
            return this;
        }

        /// <summary>
        /// Specifies how data of a streaming DataFrame is written to a streaming sink.
        /// </summary>
        /// <param name="outputMode">Output mode enum</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter OutputMode(OutputMode outputMode) =>
            OutputMode(outputMode.ToString());

        /// <summary>
        /// Specifies the underlying output data source.
        /// </summary>
        /// <param name="source">Name of the data source</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Format(string source)
        {
            _jvmObject.Invoke("format", source);
            return this;
        }

        /// <summary>
        /// Partitions the output by the given columns on the file system. If specified,
        /// the output is laid out on the file system similar to Hive's partitioning scheme.
        /// </summary>
        /// <param name="colNames">Column names to partition by</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter PartitionBy(params string[] colNames)
        {
            _jvmObject.Invoke("partitionBy", (object)colNames);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Option(string key, string value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Option(string key, bool value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Option(string key, long value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Option(string key, double value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds output options for the underlying data source.
        /// </summary>
        /// <param name="options">Key/value options</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Options(Dictionary<string, string> options)
        {
            _jvmObject.Invoke("options", options);
            return this;
        }

        /// <summary>
        /// Sets the trigger for the stream query.
        /// </summary>
        /// <param name="trigger">Trigger object</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Trigger(Trigger trigger)
        {
            _jvmObject.Invoke("trigger", trigger);
            return this;
        }

        /// <summary>
        /// Specifies the name of the <see cref="StreamingQuery"/> 
        /// that can be started with `start()`.
        /// This name must be unique among all the currently active queries 
        /// in the associated SQLContext.
        /// </summary>
        /// <param name="queryName">Query name</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter QueryName(string queryName)
        {
            _jvmObject.Invoke("queryName", queryName);
            return this;
        }

        /// <summary>
        /// Starts the execution of the streaming query.
        /// </summary>
        /// <param name="path">Optional output path</param>
        /// <returns>StreamingQuery object</returns>
        public StreamingQuery Start(string path = null)
        {
            if (!string.IsNullOrEmpty(path))
            {
                return new StreamingQuery((JvmObjectReference)_jvmObject.Invoke("start", path));
            }
            return new StreamingQuery((JvmObjectReference)_jvmObject.Invoke("start"));
        }

        /// <summary>
        /// Sets the output of the streaming query to be processed using the provided
        /// writer object. See <see cref="IForeachWriter"/> for more details on the
        /// lifecycle and semantics.
        /// </summary>
        /// <param name="writer"></param>
        /// <returns>This DataStreamWriter object</returns>
        [Since(Versions.V2_4_0)]
        public DataStreamWriter Foreach(IForeachWriter writer)
        {
            RDD.WorkerFunction.ExecuteDelegate wrapper =
                new ForeachWriterWrapperUdfWrapper(
                    new ForeachWriterWrapper(writer).Process).Execute;

            _jvmObject.Invoke(
                "foreach",
                _jvmObject.Jvm.CallConstructor(
                    "org.apache.spark.sql.execution.python.PythonForeachWriter",
                    UdfUtils.CreatePythonFunction(
                        _jvmObject.Jvm,
                        CommandSerDe.Serialize(
                            wrapper,
                            CommandSerDe.SerializedMode.Row,
                            CommandSerDe.SerializedMode.Row)),
                    DataType.FromJson(_jvmObject.Jvm, _df.Schema().Json)));

            return this;
        }

        /// <summary>
        /// Helper function to add given key/value pair as a new option.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamWriter object</returns>
        private DataStreamWriter OptionInternal(string key, object value)
        {
            _jvmObject.Invoke("option", key, value);
            return this;
        }
    }
}
