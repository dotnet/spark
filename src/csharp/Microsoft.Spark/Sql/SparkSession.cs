// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Scala;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// The entry point to programming Spark with the Dataset and DataFrame API.
    /// </summary>
    public sealed class SparkSession : IDisposable, IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        private readonly Lazy<SparkContext> _sparkContext;
        private readonly Lazy<Catalog.Catalog> _catalog;

        private static readonly string s_sparkSessionClassName =
            "org.apache.spark.sql.SparkSession";

        /// <summary>
        /// Constructor for SparkSession.
        /// </summary>
        /// <param name="jvmObject">Reference to the JVM SparkSession object</param>
        internal SparkSession(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
            _sparkContext = new Lazy<SparkContext>(
                () => new SparkContext(
                    (JvmObjectReference)_jvmObject.Invoke("sparkContext")));
            _catalog = new Lazy<Catalog.Catalog>(
                () => new Catalog.Catalog((JvmObjectReference)_jvmObject.Invoke("catalog")));
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Returns SparkContext object associated with this SparkSession.
        /// </summary>
        public SparkContext SparkContext => _sparkContext.Value;

        /// <summary>
        /// Interface through which the user may create, drop, alter or query underlying databases,
        /// tables, functions etc.
        /// </summary>
        /// <returns>Catalog object</returns>
        public Catalog.Catalog Catalog => _catalog.Value;

        /// <summary>
        /// Creates a Builder object for SparkSession.
        /// </summary>
        /// <returns>Builder object</returns>
        public static Builder Builder() => new Builder();

        /// <summary>
        /// Changes the SparkSession that will be returned in this thread when 
        /// <see cref="Builder.GetOrCreate"/> is called. This can be used to ensure that a given
        /// thread receives a SparkSession with an isolated session, instead of the global
        /// (first created) context.
        /// </summary>
        /// <param name="session">SparkSession object</param>
        public static void SetActiveSession(SparkSession session) =>
            session._jvmObject.Jvm.CallStaticJavaMethod(
                s_sparkSessionClassName, "setActiveSession", session);

        /// <summary>
        /// Clears the active SparkSession for current thread. Subsequent calls to
        /// <see cref="Builder.GetOrCreate"/> will return the first created context
        /// instead of a thread-local override.
        /// </summary>
        public static void ClearActiveSession() =>
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_sparkSessionClassName, "clearActiveSession");

        /// <summary>
        /// Returns the active SparkSession for the current thread, returned by the builder.
        /// </summary>
        /// <returns>Return null, when calling this function on executors</returns>
        public static SparkSession GetActiveSession()
        {
            var optionalSession = new Option(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_sparkSessionClassName, "getActiveSession"));

            return optionalSession.IsDefined()
                ? new SparkSession((JvmObjectReference)optionalSession.Get())
                : null;
        }

        /// <summary>
        /// Sets the default SparkSession that is returned by the builder.
        /// </summary>
        /// <param name="session">SparkSession object</param>
        public static void SetDefaultSession(SparkSession session) =>
            session._jvmObject.Jvm.CallStaticJavaMethod(
                s_sparkSessionClassName, "setDefaultSession", session);

        /// <summary>
        /// Clears the default SparkSession that is returned by the builder.
        /// </summary>
        public static void ClearDefaultSession() =>
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_sparkSessionClassName, "clearDefaultSession");

        /// <summary>
        /// Returns the default SparkSession that is returned by the builder.
        /// </summary>
        /// <returns>SparkSession object or null if called on executors</returns>
        public static SparkSession GetDefaultSession()
        {
            var optionalSession = new Option(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_sparkSessionClassName, "getDefaultSession"));

            return optionalSession.IsDefined()
                ? new SparkSession((JvmObjectReference)optionalSession.Get())
                : null;
        }

        /// <summary>
        /// Returns the currently active SparkSession, otherwise the default one.
        /// If there is no default SparkSession, throws an exception.
        /// </summary>
        /// <returns>SparkSession object</returns>
        [Since(Versions.V2_4_0)]
        public static SparkSession Active() =>
            new SparkSession(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_sparkSessionClassName, "active"));

        /// <summary>
        /// Synonym for Stop().
        /// </summary>
        public void Dispose()
        {
            Stop();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Runtime configuration interface for Spark.
        /// <remarks>
        /// This is the interface through which the user can get and set all Spark and Hadoop
        /// configurations that are relevant to Spark SQL. When getting the value of a config,
        /// this defaults to the value set in the underlying SparkContext, if any.
        /// </remarks>
        /// </summary>
        /// <returns>The RuntimeConfig object</returns>
        public RuntimeConfig Conf() =>
            new RuntimeConfig((JvmObjectReference)_jvmObject.Invoke("conf"));

        /// <summary>
        /// Returns a <see cref="StreamingQueryManager"/> that allows managing all the
        /// <see cref="StreamingQuery"/> instances active on <c>this</c> context.
        /// </summary>
        /// <returns><see cref="StreamingQueryManager"/> object</returns>
        public StreamingQueryManager Streams() =>
            new StreamingQueryManager((JvmObjectReference)_jvmObject.Invoke("streams"));

        /// <summary>
        /// Start a new session with isolated SQL configurations, temporary tables, registered
        /// functions are isolated, but sharing the underlying SparkContext and cached data.
        /// </summary>
        /// <remarks>
        /// Other than the SparkContext, all shared state is initialized lazily.
        /// This method will force the initialization of the shared state to ensure that parent
        /// and child sessions are set up with the same shared state. If the underlying catalog
        /// implementation is Hive, this will initialize the metastore, which may take some time.
        /// </remarks>
        /// <returns>New SparkSession object</returns>
        public SparkSession NewSession() =>
            new SparkSession((JvmObjectReference)_jvmObject.Invoke("newSession"));

        /// <summary>
        /// Returns the specified table/view as a DataFrame.
        /// </summary>
        /// <param name="tableName">Name of a table or view</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Table(string tableName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("table", tableName));

        /// <summary>
        /// Creates a <see cref="DataFrame"/> from an <see cref="IEnumerable"/> containing
        /// <see cref="GenericRow"/>s using the given schema.
        /// It is important to make sure that the structure of every <see cref="GenericRow"/> of
        /// the provided <see cref="IEnumerable"/> matches
        /// the provided schema. Otherwise, there will be runtime exception.
        /// </summary>
        /// <param name="data">List of Row objects</param>
        /// <param name="schema">Schema as StructType</param>
        /// <returns>DataFrame object</returns>
        public DataFrame CreateDataFrame(IEnumerable<GenericRow> data, StructType schema) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke(
                "createDataFrame",
                data,
                DataType.FromJson(_jvmObject.Jvm, schema.Json)));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type <see cref="int"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type <see cref="int"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<int> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new IntegerType(), false));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type
        /// <see cref="Nullable{Int32}"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type
        /// <see cref="Nullable{Int32}"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<int?> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new IntegerType()));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type
        /// <see cref="string"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type <see cref="string"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<string> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new StringType()));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type
        /// <see cref="double"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type <see cref="double"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<double> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new DoubleType(), false));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type
        /// <see cref="Nullable{Double}"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type
        /// <see cref="Nullable{Double}"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<double?> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new DoubleType()));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type <see cref="bool"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type <see cref="bool"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<bool> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new BooleanType(), false));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type
        /// <see cref="Nullable{Boolean}"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type
        /// <see cref="Nullable{Boolean}"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<bool?> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new BooleanType()));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type <see cref="Date"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type <see cref="Date"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<Date> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new DateType()));

        /// <summary>
        /// Creates a Dataframe given data as <see cref="IEnumerable"/> of type
        /// <see cref="Timestamp"/>
        /// </summary>
        /// <param name="data"><see cref="IEnumerable"/> of type <see cref="Timestamp"/></param>
        /// <returns>Dataframe object</returns>
        public DataFrame CreateDataFrame(IEnumerable<Timestamp> data) =>
            CreateDataFrame(ToGenericRows(data), SchemaWithSingleColumn(new TimestampType()));

        /// <summary>
        /// Executes a SQL query using Spark, returning the result as a DataFrame.
        /// </summary>
        /// <param name="sqlText">SQL query text</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sql(string sqlText) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("sql", sqlText));

        /// <summary>
        /// Execute an arbitrary string command inside an external execution engine rather than
        /// Spark. This could be useful when user wants to execute some commands out of Spark. For
        /// example, executing custom DDL/DML command for JDBC, creating index for ElasticSearch,
        /// creating cores for Solr and so on.
        /// The command will be eagerly executed after this method is called and the returned
        /// DataFrame will contain the output of the command(if any).
        /// </summary>
        /// <param name="runner">The class name of the runner that implements
        /// `ExternalCommandRunner`</param>
        /// <param name="command">The target command to be executed</param>
        /// <param name="options">The options for the runner</param>
        /// <returns>>DataFrame object</returns>
        [Since(Versions.V3_0_0)]
        public DataFrame ExecuteCommand(
            string runner,
            string command,
            Dictionary<string, string> options) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke(
                "executeCommand",
                runner,
                command,
                options));

        /// <summary>
        /// Returns a DataFrameReader that can be used to read non-streaming data in
        /// as a DataFrame.
        /// </summary>
        /// <returns>DataFrameReader object</returns>
        public DataFrameReader Read() =>
            new DataFrameReader((JvmObjectReference)_jvmObject.Invoke("read"));

        /// <summary>
        /// Creates a DataFrame with a single column named id, containing elements in
        /// a range from 0 to end (exclusive) with step value 1.
        /// </summary>
        /// <param name="end">The end value (exclusive)</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Range(long end) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("range", end));

        /// <summary>
        /// Creates a DataFrame with a single column named id, containing elements in 
        /// a range from start to end (exclusive) with step value 1.
        /// </summary>
        /// <param name="start">The start value</param>
        /// <param name="end">The end value (exclusive)</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Range(long start, long end) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("range", start, end));

        /// <summary>
        /// Creates a DataFrame with a single column named id, containing elements in
        /// a range from start to end (exclusive) with a step value.
        /// </summary>
        /// <param name="start">The start value</param>
        /// <param name="end">The end value (exclusive)</param>
        /// <param name="step">Step value to use when creating the range</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Range(long start, long end, long step) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("range", start, end, step));

        /// <summary>
        /// Creates a DataFrame with a single column named id, containing elements in
        /// a range from start to end (exclusive) with a step value, with partition
        /// number specified.
        /// </summary>
        /// <param name="start">The start value</param>
        /// <param name="end">The end value (exclusive)</param>
        /// <param name="step">Step value to use when creating the range</param>
        /// <param name="numPartitions">The number of partitions of the DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Range(long start, long end, long step, int numPartitions) =>
            new DataFrame(
                (JvmObjectReference)_jvmObject.Invoke("range", start, end, step, numPartitions));

        /// <summary>
        /// Returns a DataStreamReader that can be used to read streaming data in as a DataFrame.
        /// </summary>
        /// <returns>DataStreamReader object</returns>
        public DataStreamReader ReadStream() =>
            new DataStreamReader((JvmObjectReference)_jvmObject.Invoke("readStream"));

        /// <summary>
        /// Returns UDFRegistraion object with which user-defined functions (UDF) can 
        /// be registered.
        /// </summary>
        /// <returns>UDFRegistration object</returns>
        public UdfRegistration Udf() =>
            new UdfRegistration((JvmObjectReference)_jvmObject.Invoke("udf"));

        /// <summary>
        /// Stops the underlying SparkContext.
        /// </summary>
        public void Stop() => _jvmObject.Invoke("stop");

        /// <summary>
        /// Get the <see cref="VersionSensor.VersionInfo"/> for the Microsoft.Spark assembly
        /// running on the Spark Driver and make a "best effort" attempt in determining the version
        /// of Microsoft.Spark.Worker assembly on the Spark Executors.
        /// </summary>
        /// <returns>
        /// A <see cref="DataFrame"/> containing the <see cref="VersionSensor.VersionInfo"/>
        /// </returns>
        public DataFrame Version(int numPartitions = 10)
        {
            StructType schema = VersionSensor.VersionInfo.s_schema;

            DataFrame sparkInfoDf =
                CreateDataFrame(
                    new GenericRow[] { VersionSensor.MicrosoftSparkVersion().ToGenericRow() },
                    schema);

            Func<Column, Column> workerInfoUdf =
                Functions.Udf<int>(
                    i => VersionSensor.MicrosoftSparkWorkerVersion().ToGenericRow(),
                    schema);
            DataFrame df = CreateDataFrame(Enumerable.Range(0, 10 * numPartitions));

            string tempColName = "WorkerVersionInfo";
            DataFrame workerInfoDf = df
                .Repartition(numPartitions)
                .WithColumn(tempColName, workerInfoUdf(df["_1"]))
                .Select(schema.Fields.Select(
                    f => Functions.Col($"{tempColName}.{f.Name}")).ToArray());

            return sparkInfoDf
                .Union(workerInfoDf)
                .DropDuplicates()
                .Sort(schema.Fields.Select(f => Functions.Col(f.Name)).ToArray());
        }

        /// <summary>
        /// Returns a single column schema of the given datatype.
        /// </summary>
        /// <param name="dataType">Datatype of the column</param>
        /// <param name="isNullable">Indicates if values of the column can be null</param>
        /// <returns>Schema as StructType</returns>
        private StructType SchemaWithSingleColumn(DataType dataType, bool isNullable = true) =>
            new StructType(new[] { new StructField("_1", dataType, isNullable) });

        /// <summary>
        /// This method is transforming each element of IEnumerable of type T input into a single 
        /// columned GenericRow.
        /// </summary>
        /// <typeparam name="T">Datatype of values in rows</typeparam>
        /// <param name="rows">List of values of type T</param>
        /// <returns><see cref="IEnumerable"/> of type <see cref="GenericRow"/></returns>
        private IEnumerable<GenericRow> ToGenericRows<T>(IEnumerable<T> rows) =>
            rows.Select(r => new GenericRow(new object[] { r }));
    }
}
