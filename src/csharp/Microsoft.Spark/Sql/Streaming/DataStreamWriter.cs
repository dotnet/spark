// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
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
        private readonly DataFrame _df;

        internal DataStreamWriter(JvmObjectReference jvmObject, DataFrame df)
        {
            Reference = jvmObject;
            _df = df;
        }

        public JvmObjectReference Reference { get; private set; }

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
            Reference.Invoke("outputMode", outputMode);
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
            Reference.Invoke("format", source);
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
            Reference.Invoke("partitionBy", (object)colNames);
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
            Reference.Invoke("options", options);
            return this;
        }

        /// <summary>
        /// Sets the trigger for the stream query.
        /// </summary>
        /// <param name="trigger">Trigger object</param>
        /// <returns>This DataStreamWriter object</returns>
        public DataStreamWriter Trigger(Trigger trigger)
        {
            Reference.Invoke("trigger", trigger);
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
            Reference.Invoke("queryName", queryName);
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
                return new StreamingQuery((JvmObjectReference)Reference.Invoke("start", path));
            }
            return new StreamingQuery((JvmObjectReference)Reference.Invoke("start"));
        }

        /// <summary>
        /// Starts the execution of the streaming query, which will continually output results to the
        /// given table as new data arrives. The returned <see cref="StreamingQuery"/> object can be
        /// used to interact with the stream.
        /// </summary>
        /// <remarks>
        /// For v1 table, partitioning columns provided by <see cref="PartitionBy(string[])"/> will be
        /// respected no matter the table exists or not. A new table will be created if the table not
        /// exists.
        ///
        /// For v2 table, <see cref="PartitionBy(string[])"/> will be ignored if the table already exists.
        /// <see cref="PartitionBy(string[])"/> will be respected only if the v2 table does not exist.
        /// Besides, the v2 table created by this API lacks some functionalities (e.g., customized
        /// properties, options, and serde info). If you need them, please create the v2 table manually
        /// before the execution to avoid creating a table with incomplete information.
        /// </remarks>
        /// <param name="tableName">Name of the table</param>
        /// <returns>StreamingQuery object</returns>
        [Since(Versions.V3_1_0)]
        public StreamingQuery ToTable(string tableName)
        {
            return new StreamingQuery((JvmObjectReference)Reference.Invoke("toTable", tableName));
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

            Reference.Invoke(
                "foreach",
                Reference.Jvm.CallConstructor(
                    "org.apache.spark.sql.execution.python.PythonForeachWriter",
                    UdfUtils.CreatePythonFunction(
                        Reference.Jvm,
                        CommandSerDe.Serialize(
                            wrapper,
                            CommandSerDe.SerializedMode.Row,
                            CommandSerDe.SerializedMode.Row)),
                    DataType.FromJson(Reference.Jvm, _df.Schema().Json)));

            return this;
        }

        /// <summary>
        /// Sets the output of the streaming query to be processed using the provided
        /// function. This is supported only in the micro-batch execution modes (that
        /// is, when the trigger is not continuous). In every micro-batch, the provided
        /// function will be called in every micro-batch with (i) the output rows as a
        /// <see cref="DataFrame"/> and (ii) the batch identifier. The batchId can be used
        /// to deduplicate and transactionally write the output (that is, the provided
        /// Dataset) to external systems. The output <see cref="DataFrame"/> is guaranteed
        /// to exactly same for the same batchId (assuming all operations are deterministic
        /// in the query).
        /// </summary>
        /// <param name="func">The function to apply to the DataFrame</param>
        /// <returns>This DataStreamWriter object</returns>
        [Since(Versions.V2_4_0)]
        public DataStreamWriter ForeachBatch(Action<DataFrame, long> func)
        {
            int callbackId = SparkEnvironment.CallbackServer.RegisterCallback(
                new ForeachBatchCallbackHandler(Reference.Jvm, func));
            Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.api.dotnet.DotnetForeachBatchHelper",
                "callForeachBatch",
                SparkEnvironment.CallbackServer.JvmCallbackClient,
                this,
                callbackId);
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
            Reference.Invoke("option", key, value);
            return this;
        }
    }
}
