// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    ///  A distributed collection of data organized into named columns.
    /// </summary>
    public sealed class DataFrame : IJvmObjectReferenceProvider
    {
        internal DataFrame(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Selects column based on the column name.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public Column this[string columnName]
        {
            get
            {
                return WrapAsColumn(Reference.Invoke("col", columnName));
            }
        }

        /// <summary>
        /// Converts this strongly typed collection of data to generic `DataFrame`.
        /// </summary>
        /// <returns></returns>
        public DataFrame ToDF() => WrapAsDataFrame(Reference.Invoke("toDF"));

        /// <summary>
        /// Converts this strongly typed collection of data to generic `DataFrame`
        /// with columns renamed.
        /// </summary>
        /// <param name="colNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame ToDF(params string[] colNames) =>
            WrapAsDataFrame(Reference.Invoke("toDF", (object)colNames));

        /// <summary>
        /// Returns the schema associated with this `DataFrame`.
        /// </summary>
        /// <returns>Schema associated with this data frame</returns>
        public StructType Schema() =>
            new StructType((JvmObjectReference)Reference.Invoke("schema"));

        /// <summary>
        /// Prints the schema to the console in a nice tree format.
        /// </summary>
        public void PrintSchema() =>
            Console.WriteLine(
                (string)((JvmObjectReference)Reference.Invoke("schema")).Invoke("treeString"));

        /// <summary>
        /// Prints the schema up to the given level to the console in a nice tree format.
        /// </summary>
        [Since(Versions.V3_0_0)]
        public void PrintSchema(int level)
        {
            var schema = (JvmObjectReference)Reference.Invoke("schema");
            Console.WriteLine((string)schema.Invoke("treeString", level));
        }

        /// <summary>
        /// Prints the plans (logical and physical) to the console for debugging purposes.
        /// </summary>
        /// <param name="extended">prints only physical if set to false</param>
        public void Explain(bool extended = false)
        {
            var execution = (JvmObjectReference)Reference.Invoke("queryExecution");
            Console.WriteLine((string)execution.Invoke(extended ? "toString" : "simpleString"));
        }

        /// <summary>
        /// Prints the plans (logical and physical) with a format specified by a given explain
        /// mode.
        /// 
        /// </summary>
        /// <param name="mode">Specifies the expected output format of plans.
        /// 1. `simple` Print only a physical plan.
        /// 2. `extended`: Print both logical and physical plans.
        /// 3. `codegen`: Print a physical plan and generated codes if they are available.
        /// 4. `cost`: Print a logical plan and statistics if they are available.
        /// 5. `formatted`: Split explain output into two sections: a physical plan outline and
        /// node details.
        /// </param>
        [Since(Versions.V3_0_0)]
        public void Explain(string mode)
        {
            var execution = (JvmObjectReference)Reference.Invoke("queryExecution");
            var explainMode = (JvmObjectReference)Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.execution.ExplainMode",
                "fromString",
                mode);
            Console.WriteLine((string)execution.Invoke("explainString", explainMode));
        }

        /// <summary>
        /// Returns all column names and their data types as an IEnumerable of Tuples.
        /// </summary>
        /// <returns>IEnumerable of Tuple of strings</returns>
        public IEnumerable<Tuple<string, string>> DTypes() =>
            Schema().Fields.Select(
                f => new Tuple<string, string>(f.Name, f.DataType.SimpleString));

        /// <summary>
        /// Returns all column names.
        /// </summary>
        /// <returns>Column names</returns>
        public IReadOnlyList<string> Columns() =>
            Schema().Fields.Select(field => field.Name).ToArray();

        /// <summary>
        /// Returns true if the Collect() and Take() methods can be run locally without any
        /// Spark executors.
        /// </summary>
        /// <returns>True if Collect() and Take() can be run locally</returns>
        public bool IsLocal() => (bool)Reference.Invoke("isLocal");

        /// <summary>
        /// Returns true if this DataFrame is empty.
        /// </summary>
        /// <returns>True if empty</returns>
        [Since(Versions.V2_4_0)]
        public bool IsEmpty() => (bool)Reference.Invoke("isEmpty");

        /// <summary>
        /// Returns true if this `DataFrame` contains one or more sources that continuously
        /// return data as it arrives.
        /// </summary>
        /// <returns>True if streaming DataFrame</returns>
        public bool IsStreaming() => (bool)Reference.Invoke("isStreaming");

        /// <summary>
        /// Returns a checkpointed version of this `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Checkpointing can be used to truncate the logical plan of this `DataFrame`, which is
        /// especially useful in iterative algorithms where the plan may grow exponentially.
        /// It will be saved to files inside the checkpoint directory set with
        /// <see cref="SparkContext.SetCheckpointDir(string)"/>.
        /// </remarks>
        /// <param name="eager">Whether to checkpoint this `DataFrame` immediately</param>
        /// <returns>Checkpointed DataFrame</returns>
        public DataFrame Checkpoint(bool eager = true) =>
            WrapAsDataFrame(Reference.Invoke("checkpoint", eager));

        /// <summary>
        /// Returns a locally checkpointed version of this `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Checkpointing can be used to truncate the logical plan of this `DataFrame`, which is
        /// especially useful in iterative algorithms where the plan may grow exponentially.
        /// Local checkpoints are written to executor storage and despite potentially faster
        /// they are unreliable and may compromise job completion.
        /// </remarks>
        /// <param name="eager">Whether to checkpoint this `DataFrame` immediately</param>
        /// <returns>DataFrame object</returns>
        public DataFrame LocalCheckpoint(bool eager = true) =>
            WrapAsDataFrame(Reference.Invoke("localCheckpoint", eager));

        /// <summary>
        /// Defines an event time watermark for this DataFrame. A watermark tracks a point in time
        /// before which we assume no more late data is going to arrive.
        /// </summary>
        /// <param name="eventTime">
        /// The name of the column that contains the event time of the row.
        /// </param>
        /// <param name="delayThreshold">
        /// The minimum delay to wait to data to arrive late, relative to the latest record that
        /// has been processed in the form of an interval (e.g. "1 minute" or "5 hours").
        /// </param>
        /// <returns>DataFrame object</returns>
        public DataFrame WithWatermark(string eventTime, string delayThreshold) =>
            WrapAsDataFrame(Reference.Invoke("withWatermark", eventTime, delayThreshold));

        /// <summary>
        /// Displays rows of the `DataFrame` in tabular form.
        /// </summary>
        /// <param name="numRows">Number of rows to show</param>
        /// <param name="truncate">If set to more than 0, truncates strings to `truncate`
        ///                        characters and all cells will be aligned right.</param>
        /// <param name="vertical">If set to true, prints output rows vertically
        ///                        (one line per column value).</param>
        public void Show(int numRows = 20, int truncate = 20, bool vertical = false) =>
            Console.WriteLine(Reference.Invoke("showString", numRows, truncate, vertical));

        /// <summary>
        /// Returns a `DataFrameNaFunctions` for working with missing data.
        /// </summary>
        /// <returns>DataFrameNaFunctions object</returns>
        public DataFrameNaFunctions Na() =>
            new DataFrameNaFunctions((JvmObjectReference)Reference.Invoke("na"));

        /// <summary>
        ///Returns a `DataFrameStatFunctions` for working statistic functions support.
        /// </summary>
        /// <returns>DataFrameNaFunctions object</returns>
        public DataFrameStatFunctions Stat() =>
            new DataFrameStatFunctions((JvmObjectReference)Reference.Invoke("stat"));

        /// <summary>
        /// Returns the content of the DataFrame as a DataFrame of JSON strings.
        /// </summary>
        /// <returns>DataFrame object with JSON strings.</returns>
        public DataFrame ToJSON() =>
            WrapAsDataFrame(Reference.Invoke("toJSON"));

        /// <summary>
        /// Join with another `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Behaves as an INNER JOIN and requires a subsequent join predicate.
        /// </remarks>
        /// <param name="right">Right side of the join operator</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Join(DataFrame right) =>
            WrapAsDataFrame(Reference.Invoke("join", right));

        /// <summary>
        /// Inner equi-join with another `DataFrame` using the given column.
        /// </summary>
        /// <param name="right">Right side of the join operator</param>
        /// <param name="usingColumn">
        /// Name of the column to join on. This column must exist on both sides.
        /// </param>
        /// <returns>DataFrame object</returns>
        public DataFrame Join(DataFrame right, string usingColumn) =>
            WrapAsDataFrame(Reference.Invoke("join", right, usingColumn));

        /// <summary>
        /// Equi-join with another `DataFrame` using the given columns. A cross join with
        /// a predicate is specified as an inner join. If you would explicitly like to
        /// perform a cross join use the `crossJoin` method.
        /// </summary>
        /// <param name="right">Right side of the join operator</param>
        /// <param name="usingColumns">Name of columns to join on</param>
        /// <param name="joinType">Type of join to perform. Default `inner`. Must be one of:
        /// `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`,
        /// `right_outer`, `left_semi`, `left_anti`</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Join(
            DataFrame right,
            IEnumerable<string> usingColumns,
            string joinType = "inner") =>
            WrapAsDataFrame(Reference.Invoke("join", right, usingColumns, joinType));

        /// <summary>
        /// Join with another `DataFrame`, using the given join expression.
        /// </summary>
        /// <param name="right">Right side of the join operator</param>
        /// <param name="joinExpr">Join expression</param>
        /// <param name="joinType">Type of join to perform. Default `inner`. Must be one of:
        /// `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`,
        /// `right_outer`, `left_semi`, `left_anti`.</param>
        /// <returns></returns>
        public DataFrame Join(DataFrame right, Column joinExpr, string joinType = "inner") =>
            WrapAsDataFrame(Reference.Invoke("join", right, joinExpr, joinType));

        /// <summary>
        /// Explicit Cartesian join with another `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Cartesian joins are very expensive without an extra filter that can be pushed down.
        /// </remarks>
        /// <param name="right">Right side of the join operator</param>
        /// <returns>DataFrame object</returns>
        public DataFrame CrossJoin(DataFrame right) =>
            WrapAsDataFrame(Reference.Invoke("crossJoin", right));

        /// <summary>
        /// Returns a new `DataFrame` with each partition sorted by the given expressions.
        /// </summary>
        /// <remarks>
        /// This is the same operation as "SORT BY" in SQL (Hive QL).
        /// </remarks>
        /// <param name="column">Column name to sort by</param>
        /// <param name="columns">Additional column names to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame SortWithinPartitions(string column, params string[] columns) =>
            WrapAsDataFrame(Reference.Invoke("sortWithinPartitions", column, columns));

        /// <summary>
        /// Returns a new `DataFrame` with each partition sorted by the given expressions.
        /// </summary>
        /// <remarks>
        /// This is the same operation as "SORT BY" in SQL (Hive QL).
        /// </remarks>
        /// <param name="columns">Column expressions to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame SortWithinPartitions(params Column[] columns) =>
            WrapAsDataFrame(Reference.Invoke("sortWithinPartitions", (object)columns));

        /// <summary>
        /// Returns a new `DataFrame` sorted by the specified column, all in ascending order.
        /// </summary>
        /// <param name="column">Column name to sort by</param>
        /// <param name="columns">Additional column names to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sort(string column, params string[] columns) =>
            WrapAsDataFrame(Reference.Invoke("sort", column, columns));

        /// <summary>
        /// Returns a new `DataFrame` sorted by the given expressions.
        /// </summary>
        /// <param name="columns">Column expressions to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sort(params Column[] columns) =>
            WrapAsDataFrame(Reference.Invoke("sort", (object)columns));

        /// <summary>
        /// Returns a new Dataset sorted by the given expressions.
        /// </summary>
        /// <remarks>
        /// This is an alias of the Sort() function.
        /// </remarks>
        /// <param name="column">Column name to sort by</param>
        /// <param name="columns">Additional column names to sort by</param>
        /// <returns></returns>
        public DataFrame OrderBy(string column, params string[] columns) =>
            WrapAsDataFrame(Reference.Invoke("orderBy", column, columns));

        /// <summary>
        /// Returns a new Dataset sorted by the given expressions.
        /// </summary>
        /// <remarks>
        /// This is an alias of the Sort() function.
        /// </remarks>
        /// <param name="columns">Column expressions to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame OrderBy(params Column[] columns) =>
            WrapAsDataFrame(Reference.Invoke("orderBy", (object)columns));

        /// <summary>
        /// Specifies some hint on the current `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Due to the limitation of the type conversion between CLR and JVM,
        /// the type of object in `parameters` should be the same.
        /// </remarks>
        /// <param name="name">Name of the hint</param>
        /// <param name="parameters">Parameters of the hint</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Hint(string name, object[] parameters = null)
        {
            // If parameters are empty, create an empty int array so
            // that the type conversion between CLR and JVM works.
            return ((parameters == null) || (parameters.Length == 0)) ?
                WrapAsDataFrame(Reference.Invoke("hint", name, new int[] { })) :
                WrapAsDataFrame(Reference.Invoke("hint", name, parameters));
        }

        /// <summary>
        /// Selects column based on the column name.
        /// </summary>
        /// <param name="colName">Column name</param>
        /// <returns>Column object</returns>
        public Column Col(string colName) => WrapAsColumn(Reference.Invoke("col", colName));

        /// <summary>
        /// Selects column based on the column name specified as a regex.
        /// </summary>
        /// <param name="colName">Column name as a regex</param>
        /// <returns>Column object</returns>
        public Column ColRegex(string colName) =>
            WrapAsColumn(Reference.Invoke("colRegex", colName));

        /// <summary>
        /// Returns a new `DataFrame` with an alias set.
        /// </summary>
        /// <param name="alias">Alias name</param>
        /// <returns>Column object</returns>
        public DataFrame As(string alias) => WrapAsDataFrame(Reference.Invoke("as", alias));

        /// <summary>
        /// Returns a new `DataFrame` with an alias set. Same as As().
        /// </summary>
        /// <param name="alias">Alias name</param>
        /// <returns>Column object</returns>
        public DataFrame Alias(string alias) => WrapAsDataFrame(Reference.Invoke("alias", alias));

        /// <summary>
        /// Selects a set of column based expressions.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Select(params Column[] columns) =>
            WrapAsDataFrame(Reference.Invoke("select", (object)columns));

        /// <summary>
        /// Selects a set of columns. This is a variant of Select() that can only select
        /// existing columns using column names (i.e. cannot construct expressions).
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Select(string column, params string[] columns) =>
            WrapAsDataFrame(Reference.Invoke("select", column, columns));

        /// <summary>
        /// Selects a set of SQL expressions. This is a variant of Select() that
        /// accepts SQL expressions.
        /// </summary>
        /// <param name="expressions"></param>
        /// <returns>DataFrame object</returns>
        public DataFrame SelectExpr(params string[] expressions) =>
            WrapAsDataFrame(Reference.Invoke("selectExpr", (object)expressions));

        /// <summary>
        /// Filters rows using the given condition.
        /// </summary>
        /// <param name="condition">Condition expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Filter(Column condition) =>
            WrapAsDataFrame(Reference.Invoke("filter", condition));

        /// <summary>
        /// Filters rows using the given SQL expression.
        /// </summary>
        /// <param name="conditionExpr">SQL expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Filter(string conditionExpr) =>
            WrapAsDataFrame(Reference.Invoke("filter", conditionExpr));

        /// <summary>
        /// Filters rows using the given condition. This is an alias for Filter().
        /// </summary>
        /// <param name="condition">Condition expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Where(Column condition) =>
            WrapAsDataFrame(Reference.Invoke("where", condition));

        /// <summary>
        /// Filters rows using the given SQL expression. This is an alias for Filter().
        /// </summary>
        /// <param name="conditionExpr">SQL expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Where(string conditionExpr) =>
            WrapAsDataFrame(Reference.Invoke("where", conditionExpr));

        /// <summary>
        /// Groups the DataFrame using the specified columns, so we can run aggregation on them.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset GroupBy(params Column[] columns) =>
            WrapAsGroupedDataset(Reference.Invoke("groupBy", (object)columns));

        /// <summary>
        /// Groups the DataFrame using the specified columns.
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset GroupBy(string column, params string[] columns) =>
            WrapAsGroupedDataset(Reference.Invoke("groupBy", column, columns));

        /// <summary>
        /// Create a multi-dimensional rollup for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Rollup(params Column[] columns) =>
            WrapAsGroupedDataset(Reference.Invoke("rollup", (object)columns));

        /// <summary>
        /// Create a multi-dimensional rollup for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Rollup(string column, params string[] columns) =>
            WrapAsGroupedDataset(Reference.Invoke("rollup", column, columns));

        /// <summary>
        /// Create a multi-dimensional cube for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Cube(params Column[] columns) =>
            WrapAsGroupedDataset(Reference.Invoke("cube", (object)columns));

        /// <summary>
        /// Create a multi-dimensional cube for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Cube(string column, params string[] columns) =>
            WrapAsGroupedDataset(Reference.Invoke("cube", column, columns));

        /// <summary>
        /// Aggregates on the entire `DataFrame` without groups.
        /// </summary>
        /// <param name="expr">Column expression to aggregate</param>
        /// <param name="exprs">Additional column expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Agg(Column expr, params Column[] exprs) =>
            WrapAsDataFrame(Reference.Invoke("agg", expr, exprs));

        /// <summary>
        /// Define (named) metrics to observe on the Dataset. This method returns an 'observed'
        /// DataFrame that returns the same result as the input, with the following guarantees:
        /// 
        /// 1. It will compute the defined aggregates(metrics) on all the data that is flowing
        /// through the Dataset at that point.
        /// 2. It will report the value of the defined aggregate columns as soon as we reach a
        /// completion point.A completion point is either the end of a query(batch mode) or the end
        /// of a streaming epoch. The value of the aggregates only reflects the data processed
        /// since the previous completion point.
        /// 
        /// Please note that continuous execution is currently not supported.
        /// </summary>
        /// <param name="name">Named metrics to observe</param>
        /// <param name="expr">Defined aggregate to observe</param>
        /// <param name="exprs">Defined aggregates to observe</param>
        /// <returns>DataFrame object</returns>
        [Since(Versions.V3_0_0)]
        public DataFrame Observe(string name, Column expr, params Column[] exprs) =>
            WrapAsDataFrame(Reference.Invoke("observe", name, expr, exprs));

        /// <summary>
        /// Create a write configuration builder for v2 sources.
        /// </summary>
        /// <param name="table">Name of table to write to</param>
        /// <returns>DataFrameWriterV2 object</returns>
        [Since(Versions.V3_0_0)]
        public DataFrameWriterV2 WriteTo(string table) =>
            new DataFrameWriterV2((JvmObjectReference)Reference.Invoke("writeTo", table));

        /// <summary>
        /// Returns a new `DataFrame` by taking the first `number` rows.
        /// </summary>
        /// <param name="n">Number of rows to take</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Limit(int n) => WrapAsDataFrame(Reference.Invoke("limit", n));

        /// <summary>
        /// Returns a new `DataFrame` containing union of rows in this `DataFrame`
        /// and another `DataFrame`.
        /// </summary>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Union(DataFrame other) =>
            WrapAsDataFrame(Reference.Invoke("union", other));

        /// <summary>
        /// Returns a new `DataFrame` containing union of rows in this `DataFrame`
        /// and another `DataFrame`, resolving columns by name.
        /// </summary>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame UnionByName(DataFrame other) =>
            WrapAsDataFrame(Reference.Invoke("unionByName", other));

        /// <summary>
        /// Returns a new <see cref="DataFrame"/> containing union of rows in this
        /// <see cref="DataFrame"/> and another <see cref="DataFrame"/>, resolving
        /// columns by name.
        /// </summary>
        /// <param name="other">Other DataFrame</param>
        /// <param name="allowMissingColumns">Allow missing columns</param>
        /// <returns>DataFrame object</returns>
        [Since(Versions.V3_1_0)]
        public DataFrame UnionByName(DataFrame other, bool allowMissingColumns) =>
            WrapAsDataFrame(Reference.Invoke("unionByName", other, allowMissingColumns));

        /// <summary>
        /// Returns a new `DataFrame` containing rows only in both this `DataFrame`
        /// and another `DataFrame`.
        /// </summary>
        /// <remarks>
        /// This is equivalent to `INTERSECT` in SQL.
        /// </remarks>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Intersect(DataFrame other) =>
            WrapAsDataFrame(Reference.Invoke("intersect", other));

        /// <summary>
        /// Returns a new `DataFrame` containing rows only in both this `DataFrame`
        /// and another `DataFrame` while preserving the duplicates.
        /// </summary>
        /// <remarks>
        /// This is equivalent to `INTERSECT ALL` in SQL.
        /// </remarks>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        [Since(Versions.V2_4_0)]
        public DataFrame IntersectAll(DataFrame other) =>
            WrapAsDataFrame(Reference.Invoke("intersectAll", other));

        /// <summary>
        /// Returns a new `DataFrame` containing rows in this `DataFrame` but
        /// not in another `DataFrame`.
        /// </summary>
        /// <remarks>
        /// This is equivalent to `EXCEPT DISTINCT` in SQL.
        /// </remarks>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Except(DataFrame other) =>
            WrapAsDataFrame(Reference.Invoke("except", other));

        /// <summary>
        /// Returns a new `DataFrame` containing rows in this `DataFrame` but
        /// not in another `DataFrame` while preserving the duplicates.
        /// </summary>
        /// <remarks>
        /// This is equivalent to `EXCEPT ALL` in SQL.
        /// </remarks>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        [Since(Versions.V2_4_0)]
        public DataFrame ExceptAll(DataFrame other) =>
            WrapAsDataFrame(Reference.Invoke("exceptAll", other));

        /// <summary>
        /// Returns a new `DataFrame` by sampling a fraction of rows (without replacement),
        /// using a user-supplied seed.
        /// </summary>
        /// <param name="fraction">Fraction of rows</param>
        /// <param name="withReplacement">Sample with replacement or not</param>
        /// <param name="seed">Optional random seed</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sample(
            double fraction,
            bool withReplacement = false,
            long? seed = null) =>
            WrapAsDataFrame(
                seed.HasValue ?
                Reference.Invoke("sample", withReplacement, fraction, seed.GetValueOrDefault()) :
                Reference.Invoke("sample", withReplacement, fraction));

        /// <summary>
        /// Randomly splits this `DataFrame` with the provided weights.
        /// </summary>
        /// <param name="weights">Weights for splits</param>
        /// <param name="seed">Optional random seed</param>
        /// <returns>DataFrame object</returns>
        public DataFrame[] RandomSplit(double[] weights, long? seed = null)
        {
            var dataFrames = (JvmObjectReference[])(seed.HasValue ?
                Reference.Invoke("randomSplit", weights, seed.GetValueOrDefault()) :
                Reference.Invoke("randomSplit", weights));

            return dataFrames.Select(jvmObject => new DataFrame(jvmObject)).ToArray();
        }

        /// <summary>
        /// Returns a new `DataFrame` by adding a column or replacing the existing column that
        /// has the same name.
        /// </summary>
        /// <param name="colName">Name of the new column</param>
        /// <param name="col">Column expression for the new column</param>
        /// <returns>DataFrame object</returns>
        public DataFrame WithColumn(string colName, Column col) =>
            WrapAsDataFrame(Reference.Invoke("withColumn", colName, col));

        /// <summary>
        /// Returns a new Dataset with a column renamed.
        /// This is a no-op if schema doesn't contain `existingName`.
        /// </summary>
        /// <param name="existingName">Existing column name</param>
        /// <param name="newName">New column name to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame WithColumnRenamed(string existingName, string newName) =>
            WrapAsDataFrame(Reference.Invoke("withColumnRenamed", existingName, newName));

        /// <summary>
        /// Returns a new `DataFrame` with columns dropped.
        /// This is a no-op if schema doesn't contain column name(s).
        /// </summary>
        /// <param name="colNames">Name of columns to drop</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(params string[] colNames) =>
            WrapAsDataFrame(Reference.Invoke("drop", (object)colNames));

        /// <summary>
        /// Returns a new `DataFrame` with a column dropped.
        /// This is a no-op if the `DataFrame` doesn't have a column with an equivalent expression.
        /// </summary>
        /// <param name="col">Column expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(Column col) => WrapAsDataFrame(Reference.Invoke("drop", col));

        /// <summary>
        /// Returns a new `DataFrame` that contains only the unique rows from this `DataFrame`.
        /// This is an alias for Distinct().
        /// </summary>
        /// <returns></returns>
        public DataFrame DropDuplicates() => WrapAsDataFrame(Reference.Invoke("dropDuplicates"));

        /// <summary>
        /// Returns a new `DataFrame` with duplicate rows removed, considering only
        /// the subset of columns.
        /// </summary>
        /// <param name="col">Column name</param>
        /// <param name="cols">Additional column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame DropDuplicates(string col, params string[] cols) =>
            WrapAsDataFrame(Reference.Invoke("dropDuplicates", col, cols));

        /// <summary>
        /// Computes basic statistics for numeric and string columns, including count, mean,
        /// stddev, min, and max. If no columns are given, this function computes statistics for
        /// all numerical or string columns.
        /// </summary>
        /// <remarks>
        /// This function is meant for exploratory data analysis, as we make no guarantee about
        /// the backward compatibility of the schema of the resulting DataFrame. If you want to
        /// programmatically compute summary statistics, use the `agg` function instead.
        /// </remarks>
        /// <param name="cols">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Describe(params string[] cols) =>
            WrapAsDataFrame(Reference.Invoke("describe", (object)cols));

        /// <summary>
        /// Computes specified statistics for numeric and string columns.
        /// </summary>
        /// <remarks>
        /// Available statistics are:
        /// - count
        /// - mean
        /// - stddev
        /// - min
        /// - max
        /// - arbitrary approximate percentiles specified as a percentage(e.g., 75%)
        ///
        /// If no statistics are given, this function computes count, mean, stddev, min,
        /// approximate quartiles(percentiles at 25%, 50%, and 75%), and max.
        /// </remarks>
        /// <param name="statistics">Statistics to compute</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Summary(params string[] statistics) =>
            WrapAsDataFrame(Reference.Invoke("summary", (object)statistics));

        /// <summary>
        /// Returns the first `n` rows.
        /// </summary>
        /// <param name="n">Number of rows</param>
        /// <returns>First `n` rows</returns>
        public IEnumerable<Row> Head(int n) => Limit(n).Collect();

        /// <summary>
        /// Returns the first row.
        /// </summary>
        /// <returns>First row</returns>
        public Row Head() => Limit(1).Collect().First();

        /// <summary>
        /// Returns the first row. Alis for Head().
        /// </summary>
        /// <returns>First row</returns>
        public Row First() => Head();

        /// <summary>
        /// Concise syntax for chaining custom transformations.
        /// </summary>
        /// <param name="func">
        /// A function that takes and returns a <see cref="DataFrame"/>
        /// </param>
        /// <returns>Transformed DataFrame object.</returns>
        public DataFrame Transform(Func<DataFrame, DataFrame> func) => func(this);

        /// <summary>
        /// Returns the first `n` rows in the `DataFrame`.
        /// </summary>
        /// <param name="n">Number of rows</param>
        /// <returns>First `n` rows</returns>
        public IEnumerable<Row> Take(int n) => Head(n);

        /// <summary>
        /// Returns the last `n` rows in the `DataFrame`.
        /// </summary>
        /// <param name="n">Number of rows</param>
        /// <returns>Last `n` rows</returns>
        [Since(Versions.V3_0_0)]
        public IEnumerable<Row> Tail(int n)
        {
            return GetRows("tailToPython", n);
        }

        /// <summary>
        /// Returns an array that contains all rows in this `DataFrame`.
        /// </summary>
        /// <remarks>
        /// This requires moving all the data into the application's driver process, and
        /// doing so on a very large dataset can crash the driver process with OutOfMemoryError.
        /// </remarks>
        /// <returns>Row objects</returns>
        public IEnumerable<Row> Collect()
        {
            return GetRows("collectToPython");
        }

        /// <summary>
        /// Returns an iterator that contains all of the rows in this `DataFrame`.
        /// The iterator will consume as much memory as the largest partition in this `DataFrame`.
        /// </summary>
        /// <returns>Row objects</returns>
        public IEnumerable<Row> ToLocalIterator()
        {
            Version version = SparkEnvironment.SparkVersion;
            return version.Major switch
            {
                2 => GetRows("toPythonIterator"),
                3 => ToLocalIterator(false),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }

        /// <summary>
        /// Returns an iterator that contains all of the rows in this `DataFrame`.
        /// The iterator will consume as much memory as the largest partition in this `DataFrame`.
        /// With prefetch it may consume up to the memory of the 2 largest partitions.
        /// </summary>
        /// <param name="prefetchPartitions">
        /// If Spark should pre-fetch the next partition before it is needed.
        /// </param>
        /// <returns>Row objects</returns>
        [Since(Versions.V3_0_0)]
        public IEnumerable<Row> ToLocalIterator(bool prefetchPartitions)
        {
            (int port, string secret, JvmObjectReference server) =
                ParseConnectionInfo(
                    Reference.Invoke("toPythonIterator", prefetchPartitions),
                    true);
            using ISocketWrapper socket = SocketFactory.CreateSocket();
            socket.Connect(IPAddress.Loopback, port, secret);
            foreach (Row row in new RowCollector().Collect(socket, server))
            {
                yield return row;
            }
        }

        /// <summary>
        /// Returns the number of rows in the `DataFrame`.
        /// </summary>
        /// <returns></returns>
        public long Count() => (long)Reference.Invoke("count");

        /// <summary>
        /// Returns a new `DataFrame` that has exactly `numPartitions` partitions.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Repartition(int numPartitions) =>
            WrapAsDataFrame(Reference.Invoke("repartition", numPartitions));

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions into
        /// `numPartitions`. The resulting `DataFrame` is hash partitioned.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Repartition(int numPartitions, params Column[] partitionExprs) =>
            WrapAsDataFrame(Reference.Invoke("repartition", numPartitions, partitionExprs));

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions, using
        /// `spark.sql.shuffle.partitions` as number of partitions.
        /// </summary>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Repartition(params Column[] partitionExprs) =>
            WrapAsDataFrame(Reference.Invoke("repartition", (object)partitionExprs));

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions into
        /// `numPartitions`. The resulting `DataFrame` is range partitioned.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame RepartitionByRange(int numPartitions, params Column[] partitionExprs)
        {
            if ((partitionExprs == null) || (partitionExprs.Length == 0))
            {
                throw new ArgumentException("partitionExprs cannot be empty.");
            }

            return WrapAsDataFrame(
                Reference.Invoke("repartitionByRange", numPartitions, partitionExprs));
        }

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions, using
        /// `spark.sql.shuffle.partitions` as number of partitions.
        /// The resulting Dataset is range partitioned.
        /// </summary>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame RepartitionByRange(params Column[] partitionExprs) =>
            WrapAsDataFrame(Reference.Invoke("repartitionByRange", (object)partitionExprs));

        /// <summary>
        /// Returns a new `DataFrame` that has exactly `numPartitions` partitions, when the
        /// fewer partitions are requested. If a larger number of partitions is requested,
        /// it will stay at the current number of partitions.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Coalesce(int numPartitions) =>
            WrapAsDataFrame(Reference.Invoke("coalesce", numPartitions));

        /// <summary>
        /// Returns a new Dataset that contains only the unique rows from this `DataFrame`.
        /// This is an alias for DropDuplicates().
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Distinct() => WrapAsDataFrame(Reference.Invoke("distinct"));

        /// <summary>
        /// Persist this <see cref="DataFrame"/> with the default storage level MEMORY_AND_DISK.
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Persist() => WrapAsDataFrame(Reference.Invoke("persist"));

        /// <summary>
        /// Persist this <see cref="DataFrame"/> with the given storage level.
        /// </summary>
        /// <param name="storageLevel">
        /// <see cref="StorageLevel"/> to persist the <see cref="DataFrame"/> to.
        /// </param>
        /// <returns>DataFrame object</returns>
        public DataFrame Persist(StorageLevel storageLevel) =>
            WrapAsDataFrame(Reference.Invoke("persist", storageLevel));

        /// <summary>
        /// Persist this <see cref="DataFrame"/> with the default storage level MEMORY_AND_DISK.
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Cache() => WrapAsDataFrame(Reference.Invoke("cache"));

        /// <summary>
        /// Get the <see cref="DataFrame"/>'s current <see cref="StorageLevel"/>.
        /// </summary>
        /// <returns><see cref="StorageLevel"/> object</returns>
        public StorageLevel StorageLevel() =>
            new StorageLevel((JvmObjectReference)Reference.Invoke("storageLevel"));

        /// <summary>
        /// Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
        /// </summary>
        /// <remarks>
        /// This will not un-persist any cached data that is built upon this `DataFrame`.
        /// </remarks>
        /// <param name="blocking"></param>
        /// <returns></returns>
        public DataFrame Unpersist(bool blocking = false) =>
            WrapAsDataFrame(Reference.Invoke("unpersist", blocking));

        /// <summary>
        /// Creates a local temporary view using the given name. The lifetime of this
        /// temporary view is tied to the SparkSession that created this `DataFrame`.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateTempView(string viewName)
        {
            Reference.Invoke("createTempView", viewName);
        }

        /// <summary>
        /// Creates or replaces a local temporary view using the given name. The lifetime of this
        /// temporary view is tied to the SparkSession that created this `DataFrame`.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateOrReplaceTempView(string viewName)
        {
            Reference.Invoke("createOrReplaceTempView", viewName);
        }

        /// <summary>
        /// Creates a global temporary view using the given name. The lifetime of this
        /// temporary view is tied to this Spark application.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateGlobalTempView(string viewName)
        {
            Reference.Invoke("createGlobalTempView", viewName);
        }

        /// <summary>
        /// Creates or replaces a global temporary view using the given name. The lifetime of this
        /// temporary view is tied to this Spark application.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateOrReplaceGlobalTempView(string viewName)
        {
            Reference.Invoke("createOrReplaceGlobalTempView", viewName);
        }

        /// <summary>
        /// Interface for saving the content of the non-streaming Dataset out
        /// into external storage.
        /// </summary>
        /// <returns>DataFrameWriter object</returns>
        public DataFrameWriter Write() =>
            new DataFrameWriter((JvmObjectReference)Reference.Invoke("write"));

        /// <summary>
        /// Interface for saving the content of the streaming Dataset out into external storage.
        /// </summary>
        /// <returns>DataStreamWriter object</returns>
        public DataStreamWriter WriteStream() =>
            new DataStreamWriter((JvmObjectReference)Reference.Invoke("writeStream"), this);

        /// <summary>
        /// Returns a best-effort snapshot of the files that compose this <see cref="DataFrame"/>.
        /// This method simply asks each constituent BaseRelation for its respective files and takes
        /// the union of all results. Depending on the source relations, this may not find all input
        /// files. Duplicates are removed.
        /// </summary>
        /// <returns>Files that compose this DataFrame</returns>
        public IEnumerable<string> InputFiles() => (string[])Reference.Invoke("inputFiles");

        /// <summary>
        /// Returns `true` when the logical query plans inside both <see cref="DataFrame"/>s are
        /// equal and therefore return same results.
        /// </summary>
        /// <remarks>
        /// The equality comparison here is simplified by tolerating the cosmetic differences
        /// such as attribute names.
        /// 
        /// This API can compare both <see cref="DataFrame"/>s very fast but can still return `false`
        /// on the <see cref="DataFrame"/> that return the same results, for instance, from different
        /// plans. Such false negative semantic can be useful when caching as an example.
        /// </remarks>
        /// <param name="other">Other DataFrame</param>
        /// <returns>
        /// `true` when the logical query plans inside both <see cref="DataFrame"/>s are
        /// equal and therefore return same results.
        /// </returns>
        [Since(Versions.V3_1_0)]
        public bool SameSemantics(DataFrame other) =>
            (bool)Reference.Invoke("sameSemantics", other);

        /// <summary>
        /// Returns a hash code of the logical query plan against this <see cref="DataFrame"/>.
        /// </summary>
        /// <remarks>
        /// Unlike the standard hash code, the hash is calculated against the query plan
        /// simplified by tolerating the cosmetic differences such as attribute names.
        /// </remarks>
        /// <returns>Hash code of the logical query plan</returns>
        [Since(Versions.V3_1_0)]
        public int SemanticHash() =>
            (int)Reference.Invoke("semanticHash");

        /// <summary>
        /// Returns row objects based on the function (either "toPythonIterator",
        /// "collectToPython", or "tailToPython").
        /// </summary>
        /// <param name="funcName">String name of function to call</param>
        /// <param name="args">Arguments to the function</param>
        /// <returns>IEnumerable of Rows from Spark</returns>
        private IEnumerable<Row> GetRows(string funcName, params object[] args)
        {
            (int port, string secret, _) = GetConnectionInfo(funcName, args);
            using ISocketWrapper socket = SocketFactory.CreateSocket();
            socket.Connect(IPAddress.Loopback, port, secret);
            foreach (Row row in new RowCollector().Collect(socket))
            {
                yield return row;
            }
        }

        /// <summary>
        /// Returns a tuple of port number and secret string which are
        /// used for connecting with Spark to receive rows for this `DataFrame`.
        /// </summary>
        /// <returns>A tuple of port number, secret string, and JVM socket auth server.</returns>
        private (int, string, JvmObjectReference) GetConnectionInfo(
            string funcName,
            params object[] args)
        {
            object result = Reference.Invoke(funcName, args);
            Version version = SparkEnvironment.SparkVersion;
            return (version.Major, version.Minor) switch
            {
                // PythonFunction.serveIterator() returns a pair where the first is a port
                // number and the second is the secret string to use for the authentication.
                (2, 4) => ParseConnectionInfo(result, false),
                (3, _) => ParseConnectionInfo(result, false),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }

        private (int, string, JvmObjectReference) ParseConnectionInfo(
            object info,
            bool parseServer)
        {
            var infos = (JvmObjectReference[])info;
            return ((int)infos[0].Invoke("intValue"),
                (string)infos[1].Invoke("toString"),
                parseServer ? infos[2] : null);
        }

        private DataFrame WrapAsDataFrame(object obj) => new DataFrame((JvmObjectReference)obj);

        private Column WrapAsColumn(object obj) => new Column((JvmObjectReference)obj);

        private RelationalGroupedDataset WrapAsGroupedDataset(object obj) =>
            new RelationalGroupedDataset((JvmObjectReference)obj, this);
    }
}
