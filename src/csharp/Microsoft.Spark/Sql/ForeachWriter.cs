﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// <para>
    /// Interface for writing custom logic to process data generated by a query. This is
    /// often used to write the output of a streaming query to arbitrary storage systems.
    /// </para>
    ///
    /// <para>
    /// Any implementation of this interface will be used by Spark in the following way:
    /// <list type="bullet">
    /// <item>
    /// <description>
    /// A single instance of this class is responsible of all the data generated by a single task
    /// in a query.In other words, one instance is responsible for processing one partition of the
    /// data generated in a distributed manner.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// Any implementation of this class must be <see cref="SerializableAttribute"/> because each
    /// task will get a fresh serialized-deserialized copy of the provided object. Hence, it is
    /// strongly recommended that any initialization for writing data (e.g.opening a connection or
    /// starting a transaction) is done after the <see cref="Open(long, long)"/> method has been
    /// called, which signifies that the task is ready to generate data.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// The lifecycle of the methods are as follows:
    /// <example>
    /// <para/>For each partition with <c>partitionId</c>:
    /// <para/>... For each batch/epoch of streaming data(if its streaming query) with <c>epochId</c>:
    /// <para/>....... Method <c>open(partitionId, epochId)</c> is called.
    /// <para/>....... If <c>open</c> returns true:
    /// <para/>........... For each row in the partition and batch/epoch, method <c>process(row)</c> is called.
    /// <para/>....... Method <c>close(errorOrNull)</c> is called with error(if any) seen while processing rows.
    /// </example>
    /// </description>
    /// </item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// Important points to note:
    /// <list type="bullet">
    /// <item>
    /// <description>
    /// The <c>partitionId</c> and <c>epochId</c> can be used to deduplicate generated data
    /// when failures cause reprocessing of some input data. This depends on the execution
    /// mode of the query. If the streaming query is being executed in the micro-batch
    /// mode, then every partition represented by a unique tuple(partition_id, epoch_id)
    /// is guaranteed to have the same data. Hence, (partition_id, epoch_id) can be used
    /// to deduplicate and/or transactionally commit data and achieve exactly-once
    /// guarantees. However, if the streaming query is being executed in the continuous
    /// mode, then this guarantee does not hold and therefore should not be used for
    /// deduplication.
    /// </description>
    /// </item>
    /// </list>
    /// </para>
    /// </summary>
    public interface IForeachWriter
    {
        /// <summary>
        /// Called when starting to process one partition of new data in the executor.
        /// </summary>
        /// <param name="partitionId">The partition id.</param>
        /// <param name="epochId">A unique id for data deduplication.</param>
        /// <returns>True if successful, false otherwise.</returns>
        bool Open(long partitionId, long epochId);

        /// <summary>
        /// Called to process each <see cref="Row"/> in the executor side. This method
        /// will be called only if <c>Open</c> returns <c>true</c>.
        /// </summary>
        /// <param name="row">The row to process.</param>
        void Process(Row row);

        /// <summary>
        /// Called when stopping to process one partition of new data in the executor side. This is
        /// guaranteed to be called either <see cref="Open(long, long)"/> returns <c>true</c> or
        /// <c>false</c>. However, <see cref="Close(Exception)"/> won't be called in the following
        /// cases:
        /// <list type="bullet">
        /// <item>
        /// <description>
        /// CLR/JVM crashes without throwing a <see cref="Exception"/>.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <see cref="Open(long, long)"/> throws an <see cref="Exception"/>.
        /// </description>
        /// </item>
        /// </list>
        /// </summary>
        /// <param name="errorOrNull">
        /// The <see cref="Exception"/> thrown during processing or null if there was no error.
        /// </param>
        void Close(Exception errorOrNull);
    }

    /// <summary>
    /// Wraps a <see cref="IForeachWriter"/> and calls the appropriate methods as decribed in
    /// the lifecycle documentation for the interface.
    /// </summary>
    internal class ForeachWriterWrapper
    {
        private readonly IForeachWriter _foreachWriter;

        internal ForeachWriterWrapper(IForeachWriter foreachWriter) =>
            _foreachWriter = foreachWriter;

        public long EpochId { get; set; } = long.MinValue;

        internal IEnumerable<object> Execute(int partitionId, IEnumerable<object> input)
        {
            Exception error = null;
            bool opened = _foreachWriter.Open(partitionId, EpochId);
            try
            {
                if (opened)
                {
                    foreach (object o in input)
                    {
                        if (o is object[] unpickledObjects)
                        {
                            foreach (object unpickled in unpickledObjects)
                            {
                                _foreachWriter.Process((unpickled as RowConstructor).GetRow());
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                error = e;
            }
            finally
            {
                _foreachWriter.Close(error);

                if (error != null)
                {
                    throw error;
                }
            }

            return Enumerable.Empty<object>();
        }
    }
}
