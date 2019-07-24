// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Streaming;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// The entry point to programming Spark with the Dataset and DataFrame API.
    /// </summary>
    public sealed class SparkSession : IDisposable, IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        private readonly Lazy<SparkContext> _sparkContext;

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
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Returns SparkContext object associated with this SparkSession.
        /// </summary>
        public SparkContext SparkContext => _sparkContext.Value;

        /// <summary>
        /// Creates a Builder object for SparkSession.
        /// </summary>
        /// <returns>Builder object</returns>
        public static Builder Builder() => new Builder();

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
        /// <returns></returns>
        public RuntimeConfig Conf() =>
            new RuntimeConfig((JvmObjectReference)_jvmObject.Invoke("conf"));

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
        /// Executes a SQL query using Spark, returning the result as a DataFrame.
        /// </summary>
        /// <param name="sqlText">SQL query text</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sql(string sqlText) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("sql", sqlText));

        /// <summary>
        /// Returns a DataFrameReader that can be used to read non-streaming data in
        /// as a DataFrame.
        /// </summary>
        /// <returns>DataFrameReader object</returns>
        public DataFrameReader Read() =>
            new DataFrameReader((JvmObjectReference)_jvmObject.Invoke("read"));

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
    }
}
