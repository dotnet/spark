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
    /// TODO:
    /// Missing APIs:
    /// Persist() with "StorageLevel"

    /// <summary>
    ///  A distributed collection of data organized into named columns.
    /// </summary>
    public sealed class DataFrame : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal DataFrame(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Selects column based on the column name.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public Column this[string columnName]
        {
            get
            {
                return WrapAsColumn(_jvmObject.Invoke("col", columnName));
            }
        }

        /// <summary>
        /// Converts this strongly typed collection of data to generic `DataFrame`.
        /// </summary>
        /// <returns></returns>
        public DataFrame ToDF() => WrapAsDataFrame(_jvmObject.Invoke("toDF"));

        /// <summary>
        /// Converts this strongly typed collection of data to generic `DataFrame`
        /// with columns renamed.
        /// </summary>
        /// <param name="colNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame ToDF(params string[] colNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("toDF", (object)colNames));

        /// <summary>
        /// Returns the schema associated with this `DataFrame`.
        /// </summary>
        /// <returns>Schema associated with this data frame</returns>
        public StructType Schema() =>
            new StructType((JvmObjectReference)_jvmObject.Invoke("schema"));

        /// <summary>
        /// Prints the schema to the console in a nice tree format.
        /// </summary>
        public void PrintSchema() =>
            Console.WriteLine(
                (string)((JvmObjectReference)_jvmObject.Invoke("schema")).Invoke("treeString"));

        /// <summary>
        /// Prints the plans (logical and physical) to the console for debugging purposes.
        /// </summary>
        /// <param name="extended">prints only physical if set to false</param>
        public void Explain(bool extended = false)
        {
            var execution = (JvmObjectReference)_jvmObject.Invoke("queryExecution");
            Console.WriteLine((string)execution.Invoke(extended ? "toString" : "simpleString"));
        }

        /// <summary>
        /// Returns all column names and their data types as an array.
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
        public bool IsLocal() => (bool)_jvmObject.Invoke("isLocal");

        /// <summary>
        /// Returns true if this DataFrame is empty.
        /// </summary>
        /// <returns>True if empty</returns>
        [Since(Versions.V2_4_0)]
        public bool IsEmpty() => (bool)_jvmObject.Invoke("isEmpty");

        /// <summary>
        /// Returns true if this `DataFrame` contains one or more sources that continuously
        /// return data as it arrives.
        /// </summary>
        /// <returns>True if streaming DataFrame</returns>
        public bool IsStreaming() => (bool)_jvmObject.Invoke("isStreaming");

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
            WrapAsDataFrame(_jvmObject.Invoke("checkpoint", eager));

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
            WrapAsDataFrame(_jvmObject.Invoke("localCheckpoint", eager));

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
            WrapAsDataFrame(_jvmObject.Invoke("withWatermark", eventTime, delayThreshold));

        /// <summary>
        /// Displays rows of the `DataFrame` in tabular form.
        /// </summary>
        /// <param name="numRows">Number of rows to show</param>
        /// <param name="truncate">If set to more than 0, truncates strings to `truncate`
        ///                        characters and all cells will be aligned right.</param>
        /// <param name="vertical">If set to true, prints output rows vertically
        ///                        (one line per column value).</param>
        public void Show(int numRows = 20, int truncate = 20, bool vertical = false) =>
            Console.WriteLine(_jvmObject.Invoke("showString", numRows, truncate, vertical));

        /// <summary>
        /// Returns a `DataFrameNaFunctions` for working with missing data.
        /// </summary>
        /// <returns>DataFrameNaFunctions object</returns>
        public DataFrameNaFunctions Na() =>
            new DataFrameNaFunctions((JvmObjectReference)_jvmObject.Invoke("na"));

        /// <summary>
        ///Returns a `DataFrameStatFunctions` for working statistic functions support.
        /// </summary>
        /// <returns>DataFrameNaFunctions object</returns>
        public DataFrameStatFunctions Stat() =>
            new DataFrameStatFunctions((JvmObjectReference)_jvmObject.Invoke("stat"));

        /// <summary>
        /// Join with another `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Behaves as an INNER JOIN and requires a subsequent join predicate.
        /// </remarks>
        /// <param name="right">Right side of the join operator</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Join(DataFrame right) =>
            WrapAsDataFrame(_jvmObject.Invoke("join", right));

        /// <summary>
        /// Inner equi-join with another `DataFrame` using the given column.
        /// </summary>
        /// <param name="right">Right side of the join operator</param>
        /// <param name="usingColumn">
        /// Name of the column to join on. This column must exist on both sides.
        /// </param>
        /// <returns>DataFrame object</returns>
        public DataFrame Join(DataFrame right, string usingColumn) =>
            WrapAsDataFrame(_jvmObject.Invoke("join", right, usingColumn));

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
            WrapAsDataFrame(_jvmObject.Invoke("join", right, usingColumns, joinType));

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
            WrapAsDataFrame(_jvmObject.Invoke("join", right, joinExpr, joinType));

        /// <summary>
        /// Explicit Cartesian join with another `DataFrame`.
        /// </summary>
        /// <remarks>
        /// Cartesian joins are very expensive without an extra filter that can be pushed down.
        /// </remarks>
        /// <param name="right">Right side of the join operator</param>
        /// <returns>DataFrame object</returns>
        public DataFrame CrossJoin(DataFrame right) =>
            WrapAsDataFrame(_jvmObject.Invoke("crossJoin", right));

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
            WrapAsDataFrame(_jvmObject.Invoke("sortWithinPartitions", column, columns));

        /// <summary>
        /// Returns a new `DataFrame` with each partition sorted by the given expressions.
        /// </summary>
        /// <remarks>
        /// This is the same operation as "SORT BY" in SQL (Hive QL).
        /// </remarks>
        /// <param name="columns">Column expressions to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame SortWithinPartitions(params Column[] columns) =>
            WrapAsDataFrame(_jvmObject.Invoke("sortWithinPartitions", (object)columns));

        /// <summary>
        /// Returns a new `DataFrame` sorted by the specified column, all in ascending order.
        /// </summary>
        /// <param name="column">Column name to sort by</param>
        /// <param name="columns">Additional column names to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sort(string column, params string[] columns) =>
            WrapAsDataFrame(_jvmObject.Invoke("sort", column, columns));

        /// <summary>
        /// Returns a new `DataFrame` sorted by the given expressions.
        /// </summary>
        /// <param name="columns">Column expressions to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Sort(params Column[] columns) =>
            WrapAsDataFrame(_jvmObject.Invoke("sort", (object)columns));

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
            WrapAsDataFrame(_jvmObject.Invoke("orderBy", column, columns));

        /// <summary>
        /// Returns a new Dataset sorted by the given expressions.
        /// </summary>
        /// <remarks>
        /// This is an alias of the Sort() function.
        /// </remarks>
        /// <param name="columns">Column expressions to sort by</param>
        /// <returns>DataFrame object</returns>
        public DataFrame OrderBy(params Column[] columns) =>
            WrapAsDataFrame(_jvmObject.Invoke("orderBy", (object)columns));

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
                WrapAsDataFrame(_jvmObject.Invoke("hint", name, new int[] { })) :
                WrapAsDataFrame(_jvmObject.Invoke("hint", name, parameters));
        }

        /// <summary>
        /// Selects column based on the column name.
        /// </summary>
        /// <param name="colName">Column name</param>
        /// <returns>Column object</returns>
        public Column Col(string colName) => WrapAsColumn(_jvmObject.Invoke("col", colName));

        /// <summary>
        /// Selects column based on the column name specified as a regex.
        /// </summary>
        /// <param name="colName">Column name as a regex</param>
        /// <returns>Column object</returns>
        public Column ColRegex(string colName) =>
            WrapAsColumn(_jvmObject.Invoke("colRegex", colName));

        /// <summary>
        /// Returns a new `DataFrame` with an alias set.
        /// </summary>
        /// <param name="alias">Alias name</param>
        /// <returns>Column object</returns>
        public DataFrame As(string alias) => WrapAsDataFrame(_jvmObject.Invoke("as", alias));

        /// <summary>
        /// Returns a new `DataFrame` with an alias set. Same as As().
        /// </summary>
        /// <param name="alias">Alias name</param>
        /// <returns>Column object</returns>
        public DataFrame Alias(string alias) => WrapAsDataFrame(_jvmObject.Invoke("alias", alias));

        /// <summary>
        /// Selects a set of column based expressions.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Select(params Column[] columns) =>
            WrapAsDataFrame(_jvmObject.Invoke("select", (object)columns));

        /// <summary>
        /// Selects a set of columns. This is a variant of Select() that can only select
        /// existing columns using column names (i.e. cannot construct expressions).
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Select(string column, params string[] columns) =>
            WrapAsDataFrame(_jvmObject.Invoke("select", column, columns));

        /// <summary>
        /// Selects a set of SQL expressions. This is a variant of Select() that
        /// accepts SQL expressions.
        /// </summary>
        /// <param name="expressions"></param>
        /// <returns>DataFrame object</returns>
        public DataFrame SelectExpr(params string[] expressions) =>
            WrapAsDataFrame(_jvmObject.Invoke("selectExpr", (object)expressions));

        /// <summary>
        /// Filters rows using the given condition.
        /// </summary>
        /// <param name="condition">Condition expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Filter(Column condition) =>
            WrapAsDataFrame(_jvmObject.Invoke("filter", condition));

        /// <summary>
        /// Filters rows using the given SQL expression.
        /// </summary>
        /// <param name="conditionExpr">SQL expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Filter(string conditionExpr) =>
            WrapAsDataFrame(_jvmObject.Invoke("filter", conditionExpr));

        /// <summary>
        /// Filters rows using the given condition. This is an alias for Filter().
        /// </summary>
        /// <param name="condition">Condition expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Where(Column condition) =>
            WrapAsDataFrame(_jvmObject.Invoke("where", condition));

        /// <summary>
        /// Filters rows using the given SQL expression. This is an alias for Filter().
        /// </summary>
        /// <param name="conditionExpr">SQL expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Where(string conditionExpr) =>
            WrapAsDataFrame(_jvmObject.Invoke("where", conditionExpr));

        /// <summary>
        /// Groups the DataFrame using the specified columns, so we can run aggregation on them.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset GroupBy(params Column[] columns) =>
            WrapAsGroupedDataset(_jvmObject.Invoke("groupBy", (object)columns));

        /// <summary>
        /// Groups the DataFrame using the specified columns.
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset GroupBy(string column, params string[] columns) =>
            WrapAsGroupedDataset(_jvmObject.Invoke("groupBy", column, columns));

        /// <summary>
        /// Create a multi-dimensional rollup for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Rollup(params Column[] columns) =>
            WrapAsGroupedDataset(_jvmObject.Invoke("rollup", (object)columns));

        /// <summary>
        /// Create a multi-dimensional rollup for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Rollup(string column, params string[] columns) =>
            WrapAsGroupedDataset(_jvmObject.Invoke("rollup", column, columns));

        /// <summary>
        /// Create a multi-dimensional cube for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Cube(params Column[] columns) =>
            WrapAsGroupedDataset(_jvmObject.Invoke("cube", (object)columns));

        /// <summary>
        /// Create a multi-dimensional cube for the current `DataFrame` using the
        /// specified columns.
        /// </summary>
        /// <param name="column">Column name</param>
        /// <param name="columns">Additional column names</param>
        /// <returns>RelationalGroupedDataset object</returns>
        public RelationalGroupedDataset Cube(string column, params string[] columns) =>
            WrapAsGroupedDataset(_jvmObject.Invoke("cube", column, columns));

        /// <summary>
        /// Aggregates on the entire `DataFrame` without groups.
        /// </summary>
        /// <param name="expr">Column expression to aggregate</param>
        /// <param name="exprs">Additional column expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Agg(Column expr, params Column[] exprs) =>
            WrapAsDataFrame(_jvmObject.Invoke("agg", expr, exprs));

        /// <summary>
        /// Returns a new `DataFrame` by taking the first `number` rows.
        /// </summary>
        /// <param name="n">Number of rows to take</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Limit(int n) => WrapAsDataFrame(_jvmObject.Invoke("limit", n));

        /// <summary>
        /// Returns a new `DataFrame` containing union of rows in this `DataFrame`
        /// and another `DataFrame`.
        /// </summary>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Union(DataFrame other) =>
            WrapAsDataFrame(_jvmObject.Invoke("union", other));

        /// <summary>
        /// Returns a new `DataFrame` containing union of rows in this `DataFrame`
        /// and another `DataFrame`, resolving columns by name.
        /// </summary>
        /// <param name="other">Other DataFrame</param>
        /// <returns>DataFrame object</returns>
        public DataFrame UnionByName(DataFrame other) =>
            WrapAsDataFrame(_jvmObject.Invoke("unionByName", other));

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
            WrapAsDataFrame(_jvmObject.Invoke("intersect", other));

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
            WrapAsDataFrame(_jvmObject.Invoke("intersectAll", other));

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
            WrapAsDataFrame(_jvmObject.Invoke("except", other));

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
            WrapAsDataFrame(_jvmObject.Invoke("exceptAll", other));

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
                _jvmObject.Invoke("sample", withReplacement, fraction, seed.GetValueOrDefault()) :
                _jvmObject.Invoke("sample", withReplacement, fraction));

        /// <summary>
        /// Randomly splits this `DataFrame` with the provided weights.
        /// </summary>
        /// <param name="weights">Weights for splits</param>
        /// <param name="seed">Optional random seed</param>
        /// <returns>DataFrame object</returns>
        public DataFrame[] RandomSplit(double[] weights, long? seed = null)
        {
            var dataFrames = (JvmObjectReference[])(seed.HasValue ?
                _jvmObject.Invoke("randomSplit", weights, seed.GetValueOrDefault()) :
                _jvmObject.Invoke("randomSplit", weights));

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
            WrapAsDataFrame(_jvmObject.Invoke("withColumn", colName, col));

        /// <summary>
        /// Returns a new Dataset with a column renamed.
        /// This is a no-op if schema doesn't contain `existingName`.
        /// </summary>
        /// <param name="existingName">Existing column name</param>
        /// <param name="newName">New column name to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame WithColumnRenamed(string existingName, string newName) =>
            WrapAsDataFrame(_jvmObject.Invoke("withColumnRenamed", existingName, newName));

        /// <summary>
        /// Returns a new `DataFrame` with columns dropped.
        /// This is a no-op if schema doesn't contain column name(s).
        /// </summary>
        /// <param name="colNames">Name of columns to drop</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(params string[] colNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("drop", (object)colNames));

        /// <summary>
        /// Returns a new `DataFrame` with a column dropped.
        /// This is a no-op if the `DataFrame` doesn't have a column with an equivalent expression.
        /// </summary>
        /// <param name="col">Column expression</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(Column col) => WrapAsDataFrame(_jvmObject.Invoke("drop", col));

        /// <summary>
        /// Returns a new `DataFrame` that contains only the unique rows from this `DataFrame`.
        /// This is an alias for Distinct().
        /// </summary>
        /// <returns></returns>
        public DataFrame DropDuplicates() => WrapAsDataFrame(_jvmObject.Invoke("dropDuplicates"));

        /// <summary>
        /// Returns a new `DataFrame` with duplicate rows removed, considering only
        /// the subset of columns.
        /// </summary>
        /// <param name="col">Column name</param>
        /// <param name="cols">Additional column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame DropDuplicates(string col, params string[] cols) =>
            WrapAsDataFrame(_jvmObject.Invoke("dropDuplicates", col, cols));

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
            WrapAsDataFrame(_jvmObject.Invoke("describe", (object)cols));

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
            WrapAsDataFrame(_jvmObject.Invoke("summary", (object)statistics));

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
        /// Returns the first `n` rows in the `DataFrame`.
        /// </summary>
        /// <param name="n">Number of rows</param>
        /// <returns>First `n` rows</returns>
        public IEnumerable<Row> Take(int n) => Head(n);

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
            return GetRows("toPythonIterator");
        }

        /// <summary>
        /// Returns the number of rows in the `DataFrame`.
        /// </summary>
        /// <returns></returns>
        public long Count() => (long)_jvmObject.Invoke("count");

        /// <summary>
        /// Returns a new `DataFrame` that has exactly `numPartitions` partitions.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Repartition(int numPartitions) =>
            WrapAsDataFrame(_jvmObject.Invoke("repartition", numPartitions));

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions into
        /// `numPartitions`. The resulting `DataFrame` is hash partitioned.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Repartition(int numPartitions, params Column[] partitionExprs) =>
            WrapAsDataFrame(_jvmObject.Invoke("repartition", numPartitions, partitionExprs));

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions, using
        /// `spark.sql.shuffle.partitions` as number of partitions.
        /// </summary>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Repartition(params Column[] partitionExprs) =>
            WrapAsDataFrame(_jvmObject.Invoke("repartition", (object)partitionExprs));

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
                _jvmObject.Invoke("repartitionByRange", numPartitions, partitionExprs));
        }

        /// <summary>
        /// Returns a new `DataFrame` partitioned by the given partitioning expressions, using
        /// `spark.sql.shuffle.partitions` as number of partitions.
        /// The resulting Dataset is range partitioned.
        /// </summary>
        /// <param name="partitionExprs">Partitioning expressions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame RepartitionByRange(params Column[] partitionExprs) =>
            WrapAsDataFrame(_jvmObject.Invoke("repartitionByRange", (object)partitionExprs));

        /// <summary>
        /// Returns a new `DataFrame` that has exactly `numPartitions` partitions, when the
        /// fewer partitions are requested. If a larger number of partitions is requested,
        /// it will stay at the current number of partitions.
        /// </summary>
        /// <param name="numPartitions">Number of partitions</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Coalesce(int numPartitions) =>
            WrapAsDataFrame(_jvmObject.Invoke("coalesce", numPartitions));

        /// <summary>
        /// Returns a new Dataset that contains only the unique rows from this `DataFrame`.
        /// This is an alias for DropDuplicates().
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Distinct() => WrapAsDataFrame(_jvmObject.Invoke("distinct"));

        /// <summary>
        /// Persist this `DataFrame` with the default storage level (`MEMORY_AND_DISK`).
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Persist() => WrapAsDataFrame(_jvmObject.Invoke("persist"));

        /// <summary>
        /// Persist this `DataFrame` with the default storage level (`MEMORY_AND_DISK`).
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Cache() => WrapAsDataFrame(_jvmObject.Invoke("cache"));

        /// <summary>
        /// Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
        /// </summary>
        /// <remarks>
        /// This will not un-persist any cached data that is built upon this `DataFrame`.
        /// </remarks>
        /// <param name="blocking"></param>
        /// <returns></returns>
        public DataFrame Unpersist(bool blocking = false) =>
            WrapAsDataFrame(_jvmObject.Invoke("unpersist", blocking));

        /// <summary>
        /// Creates a local temporary view using the given name. The lifetime of this
        /// temporary view is tied to the SparkSession that created this `DataFrame`.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateTempView(string viewName)
        {
            _jvmObject.Invoke("createTempView", viewName);
        }

        /// <summary>
        /// Creates or replaces a local temporary view using the given name. The lifetime of this
        /// temporary view is tied to the SparkSession that created this `DataFrame`.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateOrReplaceTempView(string viewName)
        {
            _jvmObject.Invoke("createOrReplaceTempView", viewName);
        }

        /// <summary>
        /// Creates a global temporary view using the given name. The lifetime of this
        /// temporary view is tied to this Spark application.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateGlobalTempView(string viewName)
        {
            _jvmObject.Invoke("createGlobalTempView", viewName);
        }

        /// <summary>
        /// Creates or replaces a global temporary view using the given name. The lifetime of this
        /// temporary view is tied to this Spark application.
        /// </summary>
        /// <param name="viewName">Name of the view</param>
        public void CreateOrReplaceGlobalTempView(string viewName)
        {
            _jvmObject.Invoke("createOrReplaceGlobalTempView", viewName);
        }

        /// <summary>
        /// Interface for saving the content of the non-streaming Dataset out
        /// into external storage.
        /// </summary>
        /// <returns>DataFrameWriter object</returns>
        public DataFrameWriter Write() =>
            new DataFrameWriter((JvmObjectReference)_jvmObject.Invoke("write"));

        /// <summary>
        /// Interface for saving the content of the streaming Dataset out into external storage.
        /// </summary>
        /// <returns>DataStreamWriter object</returns>
        public DataStreamWriter WriteStream() =>
            new DataStreamWriter((JvmObjectReference)_jvmObject.Invoke("writeStream"), this);

        /// <summary>
        /// Returns row objects based on the function (either "toPythonIterator" or
        /// "collectToPython").
        /// </summary>
        /// <param name="funcName"></param>
        /// <returns></returns>
        private IEnumerable<Row> GetRows(string funcName)
        {
            (int port, string secret) = GetConnectionInfo(funcName);
            using (ISocketWrapper socket = SocketFactory.CreateSocket())
            {
                socket.Connect(IPAddress.Loopback, port, secret);
                foreach (Row row in new RowCollector().Collect(socket))
                {
                    yield return row;
                }
            }
        }

        /// <summary>
        /// Returns a tuple of port number and secret string which are
        /// used for connecting with Spark to receive rows for this `DataFrame`.
        /// </summary>
        /// <returns>A tuple of port number and secret string</returns>
        private (int, string) GetConnectionInfo(string funcName)
        {
            object result = _jvmObject.Invoke(funcName);
            Version version = SparkEnvironment.SparkVersion;
            return (version.Major, version.Minor, version.Build) switch
            {
                // In spark 2.3.0, PythonFunction.serveIterator() returns a port number.
                (2, 3, 0) => ((int)result, string.Empty),
                // From spark >= 2.3.1, PythonFunction.serveIterator() returns a pair
                // where the first is a port number and the second is the secret
                // string to use for the authentication.
                (2, 3, _) => ParseConnectionInfo(result),
                (2, 4, _) => ParseConnectionInfo(result),
                (3, 0, _) => ParseConnectionInfo(result),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }

        private (int, string) ParseConnectionInfo(object info)
        {
            var pair = (JvmObjectReference[])info;
            return ((int)pair[0].Invoke("intValue"), (string)pair[1].Invoke("toString"));
        }

        private DataFrame WrapAsDataFrame(object obj) => new DataFrame((JvmObjectReference)obj);

        private Column WrapAsColumn(object obj) => new Column((JvmObjectReference)obj);

        private RelationalGroupedDataset WrapAsGroupedDataset(object obj) =>
            new RelationalGroupedDataset((JvmObjectReference)obj, this);
    }
}
