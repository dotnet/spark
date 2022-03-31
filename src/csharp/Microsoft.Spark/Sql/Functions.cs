// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Expressions;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Functions available for DataFrame operations.
    /// </summary>
    public static class Functions
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_functionsClassName = "org.apache.spark.sql.functions";

        /// <summary>
        /// Returns a Column based on the given column name.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Column(string columnName)
        {
            return ApplyFunction("col", columnName);
        }

        /// <summary>
        /// Returns a Column based on the given column name. Alias for Column().
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Col(string columnName)
        {
            return Column(columnName);
        }

        /// <summary>
        /// Creates a Column of literal value.
        /// </summary>
        /// <param name="literal">Literal value</param>
        /// <returns>Column object</returns>
        public static Column Lit(object literal)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    "lit",
                    literal));
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Sort functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Returns a sort expression based on the ascending order of the column.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Asc(string columnName)
        {
            return ApplyFunction("asc", columnName);
        }

        /// <summary>
        /// Returns a sort expression based on the ascending order of the column,
        /// and null values return before non-null values.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column AscNullsFirst(string columnName)
        {
            return ApplyFunction("asc_nulls_first", columnName);
        }

        /// <summary>
        /// Returns a sort expression based on the ascending order of the column,
        /// and null values appear after non-null values.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column AscNullsLast(string columnName)
        {
            return ApplyFunction("asc_nulls_last", columnName);
        }

        /// <summary>
        /// Returns a sort expression based on the descending order of the column.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Desc(string columnName)
        {
            return ApplyFunction("desc", columnName);
        }

        /// <summary>
        /// Returns a sort expression based on the descending order of the column,
        /// and null values return before non-null values.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column DescNullsFirst(string columnName)
        {
            return ApplyFunction("desc_nulls_first", columnName);
        }

        /// <summary>
        /// Returns a sort expression based on the descending order of the column,
        /// and null values appear after non-null values.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column DescNullsLast(string columnName)
        {
            return ApplyFunction("desc_nulls_last", columnName);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Aggregate functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Returns the approximate number of distinct items in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column ApproxCountDistinct(Column column)
        {
            return ApplyFunction("approx_count_distinct", column);
        }

        /// <summary>
        /// Returns the approximate number of distinct items in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column ApproxCountDistinct(string columnName)
        {
            return ApplyFunction("approx_count_distinct", columnName);
        }

        /// <summary>
        /// Returns the approximate number of distinct items in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="rsd">Maximum estimation error allowed</param>
        /// <returns>Column object</returns>
        public static Column ApproxCountDistinct(Column column, double rsd)
        {
            return ApplyFunction("approx_count_distinct", column, rsd);
        }

        /// <summary>
        /// Returns the approximate number of distinct items in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <param name="rsd">Maximum estimation error allowed</param>
        /// <returns>Column object</returns>
        public static Column ApproxCountDistinct(string columnName, double rsd)
        {
            return ApplyFunction("approx_count_distinct", columnName, rsd);
        }

        /// <summary>
        /// Returns the average of the values in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Avg(Column column)
        {
            return ApplyFunction("avg", column);
        }

        /// <summary>
        /// Returns the average of the values in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Avg(string columnName)
        {
            return ApplyFunction("avg", columnName);
        }

        /// <summary>
        /// Returns a list of objects with duplicates.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column CollectList(Column column)
        {
            return ApplyFunction("collect_list", column);
        }

        /// <summary>
        /// Returns a list of objects with duplicates.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column CollectList(string columnName)
        {
            return ApplyFunction("collect_list", columnName);
        }

        /// <summary>
        /// Returns a set of objects with duplicate elements eliminated.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column CollectSet(Column column)
        {
            return ApplyFunction("collect_set", column);
        }

        /// <summary>
        /// Returns a set of objects with duplicate elements eliminated.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column CollectSet(string columnName)
        {
            return ApplyFunction("collect_set", columnName);
        }

        /// <summary>
        /// Returns the Pearson Correlation Coefficient for two columns.
        /// </summary>
        /// <param name="column1">Column one to apply</param>
        /// <param name="column2">Column two to apply</param>
        /// <returns>Column object</returns>
        public static Column Corr(Column column1, Column column2)
        {
            return ApplyFunction("corr", column1, column2);
        }

        /// <summary>
        /// Returns the Pearson Correlation Coefficient for two columns.
        /// </summary>
        /// <param name="columnName1">Column one name</param>
        /// <param name="columnName2">Column two name</param>
        /// <returns>Column object</returns>
        public static Column Corr(string columnName1, string columnName2)
        {
            return ApplyFunction("corr", columnName1, columnName2);
        }

        /// <summary>
        /// Returns the number of items in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Count(Column column)
        {
            return ApplyFunction("count", column);
        }

        /// <summary>
        /// Returns the number of items in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Count(string columnName)
        {
            return ApplyFunction("count", columnName);
        }

        /// <summary>
        /// Returns the number of distinct items in a group.
        /// An alias of `Count_Distinct`, and it is encouraged to use `Count_Distinct` directly.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="columns">Additional columns to apply</param>
        /// <returns>Column object</returns>
        public static Column CountDistinct(Column column, params Column[] columns)
        {
            return ApplyFunction("countDistinct", column, columns);
        }

        /// <summary>
        /// Returns the number of distinct items in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <param name="columnNames">Additional column names</param>
        /// <returns>Column object</returns>
        public static Column CountDistinct(string columnName, params string[] columnNames)
        {
            return ApplyFunction("countDistinct", columnName, columnNames);
        }

        /// <summary>
        /// Returns the number of distinct items in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="columns">Additional columns to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Count_Distinct(Column column, params Column[] columns)
        {
            return ApplyFunction("count_distinct", column, columns);
        }

        /// <summary>
        /// Returns the population covariance for two columns.
        /// </summary>
        /// <param name="column1">Column one to apply</param>
        /// <param name="column2">Column two to apply</param>
        /// <returns>Column object</returns>
        public static Column CovarPop(Column column1, Column column2)
        {
            return ApplyFunction("covar_pop", column1, column2);
        }

        /// <summary>
        /// Returns the population covariance for two columns.
        /// </summary>
        /// <param name="columnName1">Column one name</param>
        /// <param name="columnName2">Column two name</param>
        /// <returns>Column object</returns>
        public static Column CovarPop(string columnName1, string columnName2)
        {
            return ApplyFunction("covar_pop", columnName1, columnName2);
        }

        /// <summary>
        /// Returns the sample covariance for two columns.
        /// </summary>
        /// <param name="column1">Column one to apply</param>
        /// <param name="column2">Column two to apply</param>
        /// <returns>Column object</returns>
        public static Column CovarSamp(Column column1, Column column2)
        {
            return ApplyFunction("covar_samp", column1, column2);
        }

        /// <summary>
        /// Returns the sample covariance for two columns.
        /// </summary>
        /// <param name="columnName1">Column one name</param>
        /// <param name="columnName2">Column two name</param>
        /// <returns>Column object</returns>
        public static Column CovarSamp(string columnName1, string columnName2)
        {
            return ApplyFunction("covar_samp", columnName1, columnName2);
        }

        /// <summary>
        /// Returns the first value of a column in a group.
        /// </summary>
        /// <remarks>
        /// The function by default returns the first values it sees. It will return
        /// the first non-null value it sees when ignoreNulls is set to true.
        /// If all values are null, then null is returned.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="ignoreNulls">To ignore null or not</param>
        /// <returns>Column object</returns>
        public static Column First(Column column, bool ignoreNulls = false)
        {
            return ApplyFunction("first", column, ignoreNulls);
        }

        /// <summary>
        /// Returns the first value of a column in a group.
        /// </summary>
        /// <remarks>
        /// The function by default returns the first values it sees. It will return
        /// the first non-null value it sees when ignoreNulls is set to true.
        /// If all values are null, then null is returned.
        /// </remarks>
        /// <param name="columnName">Column name</param>
        /// <param name="ignoreNulls">To ignore null or not</param>
        /// <returns>Column object</returns>
        public static Column First(string columnName, bool ignoreNulls = false)
        {
            return ApplyFunction("first", columnName, ignoreNulls);
        }

        /// <summary>
        /// Indicates whether a specified column in a GROUP BY list is aggregated
        /// or not, returning 1 for aggregated or 0 for not aggregated in the result set.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Grouping(Column column)
        {
            return ApplyFunction("grouping", column);
        }

        /// <summary>
        /// Indicates whether a specified column in a GROUP BY list is aggregated
        /// or not, returning 1 for aggregated or 0 for not aggregated in the result set.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Grouping(string columnName)
        {
            return ApplyFunction("grouping", columnName);
        }

        /// <summary>
        /// Returns the number of distinct items in a group.
        /// </summary>
        /// <remarks>
        /// The list of columns should match with grouping columns exactly, or empty
        /// (meaning all the grouping columns).
        /// </remarks>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column GroupingId(params Column[] columns)
        {
            return ApplyFunction("grouping_id", (object)columns);
        }

        /// <summary>
        /// Returns the number of distinct items in a group.
        /// </summary>
        /// <remarks>
        /// The list of columns should match with grouping columns exactly.
        /// </remarks>
        /// <param name="columnName">Column name</param>
        /// <param name="columnNames">Additional column names</param>
        /// <returns>Column object</returns>
        public static Column GroupingId(string columnName, params string[] columnNames)
        {
            return ApplyFunction("grouping_id", columnName, columnNames);
        }

        /// <summary>
        /// Returns the kurtosis of the values in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Kurtosis(Column column)
        {
            return ApplyFunction("kurtosis", column);
        }

        /// <summary>
        /// Returns the kurtosis of the values in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Kurtosis(string columnName)
        {
            return ApplyFunction("kurtosis", columnName);
        }

        /// <summary>
        /// Returns the last value of a column in a group.
        /// </summary>
        /// <remarks>
        /// The function by default returns the last values it sees. It will return
        /// the last non-null value it sees when ignoreNulls is set to true.
        /// If all values are null, then null is returned.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="ignoreNulls">To ignore null or not</param>
        /// <returns>Column object</returns>
        public static Column Last(Column column, bool ignoreNulls = false)
        {
            return ApplyFunction("last", column, ignoreNulls);
        }

        /// <summary>
        /// Returns the last value of a column in a group.
        /// </summary>
        /// <remarks>
        /// The function by default returns the last values it sees. It will return
        /// the last non-null value it sees when ignoreNulls is set to true.
        /// If all values are null, then null is returned.
        /// </remarks>
        /// <param name="columnName">Column name</param>
        /// <param name="ignoreNulls">To ignore null or not</param>
        /// <returns>Column object</returns>
        public static Column Last(string columnName, bool ignoreNulls = false)
        {
            return ApplyFunction("last", columnName, ignoreNulls);
        }

        /// <summary>
        /// Returns the maximum value of the column in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Max(Column column)
        {
            return ApplyFunction("max", column);
        }

        /// <summary>
        /// Returns the maximum value of the column in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Max(string columnName)
        {
            return ApplyFunction("max", columnName);
        }

        /// <summary>
        /// Returns the average value of the column in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Mean(Column column)
        {
            return ApplyFunction("mean", column);
        }

        /// <summary>
        /// Returns the average value of the column in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Mean(string columnName)
        {
            return ApplyFunction("mean", columnName);
        }

        /// <summary>
        /// Returns the minimum value of the column in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Min(Column column)
        {
            return ApplyFunction("min", column);
        }

        /// <summary>
        /// Returns the minimum value of the column in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Min(string columnName)
        {
            return ApplyFunction("min", columnName);
        }

        /// <summary>
        /// Returns the approximate `percentile` of the numeric column `col` which
        /// is the smallest value in the ordered `col` values (sorted from least to greatest) such that
        /// no more than `percentage` of `col` values is less than the value or equal to that value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="percentage">
        /// If it is a single floating point value, it must be between 0.0 and 1.0.
        /// When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
        /// In this case, returns the approximate percentile array of column col
        /// at the given percentage array.
        /// </param>
        /// <param name="accuracy">
        /// Positive numeric literal which controls approximation accuracy at the cost of memory.
        /// Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the
        /// approximation.
        /// </param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column PercentileApprox(Column column, Column percentage, Column accuracy)
        {
            return ApplyFunction("percentile_approx", column, percentage, accuracy);
        }

        /// <summary>
        /// Returns the product of all numerical elements in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Product(Column column)
        {
            return ApplyFunction("product", column);
        }

        /// <summary>
        /// Returns the skewness of the values in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Skewness(Column column)
        {
            return ApplyFunction("skewness", column);
        }

        /// <summary>
        /// Returns the skewness of the values in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Skewness(string columnName)
        {
            return ApplyFunction("skewness", columnName);
        }

        /// <summary>
        /// Alias for StddevSamp().
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Stddev(Column column)
        {
            return ApplyFunction("stddev", column);
        }

        /// <summary>
        /// Alias for StddevSamp().
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Stddev(string columnName)
        {
            return ApplyFunction("stddev", columnName);
        }

        /// <summary>
        /// Returns the sample standard deviation of the expression in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column StddevSamp(Column column)
        {
            return ApplyFunction("stddev_samp", column);
        }

        /// <summary>
        /// Returns the sample standard deviation of the expression in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column StddevSamp(string columnName)
        {
            return ApplyFunction("stddev_samp", columnName);
        }

        /// <summary>
        /// Returns the population standard deviation of the expression in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column StddevPop(Column column)
        {
            return ApplyFunction("stddev_pop", column);
        }

        /// <summary>
        /// Returns the population standard deviation of the expression in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column StddevPop(string columnName)
        {
            return ApplyFunction("stddev_pop", columnName);
        }

        /// <summary>
        /// Returns the sum of all values in the expression.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Sum(Column column)
        {
            return ApplyFunction("sum", column);
        }

        /// <summary>
        /// Returns the sum of all values in the expression.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Sum(string columnName)
        {
            return ApplyFunction("sum", columnName);
        }

        /// <summary>
        /// Returns the sum of distinct values in the expression.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column SumDistinct(Column column)
        {
            return ApplyFunction("sumDistinct", column);
        }

        /// <summary>
        /// Returns the sum of distinct values in the expression.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column SumDistinct(string columnName)
        {
            return ApplyFunction("sumDistinct", columnName);
        }

        /// <summary>
        /// Returns the sum of distinct values in the expression.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Sum_Distinct(Column column)
        {
            return ApplyFunction("sum_distinct", column);
        }

        /// <summary>
        /// Alias for <see cref="VarSamp(Sql.Column)"/>.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Variance(Column column)
        {
            return ApplyFunction("variance", column);
        }

        /// <summary>
        /// Alias for <see cref="VarSamp(string)"/>.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Variance(string columnName)
        {
            return ApplyFunction("variance", columnName);
        }

        /// <summary>
        /// Returns the unbiased variance of the values in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column VarSamp(Column column)
        {
            return ApplyFunction("var_samp", column);
        }

        /// <summary>
        /// Returns the unbiased variance of the values in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column VarSamp(string columnName)
        {
            return ApplyFunction("var_samp", columnName);
        }

        /// <summary>
        /// Returns the population variance of the values in a group.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column VarPop(Column column)
        {
            return ApplyFunction("var_pop", column);
        }

        /// <summary>
        /// Returns the population variance of the values in a group.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column VarPop(string columnName)
        {
            return ApplyFunction("var_pop", columnName);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Window functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Window function: returns the special frame boundary that represents the first
        /// row in the window partition.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 2.4 and removed in Spark 3.0.
        /// </remarks>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V2_4_0)]
        [Removed(Versions.V3_0_0)]
        public static Column UnboundedPreceding()
        {
            return ApplyFunction("unboundedPreceding");
        }

        /// <summary>
        /// Window function: returns the special frame boundary that represents the last
        /// row in the window partition.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 2.4 and removed in Spark 3.0.
        /// </remarks>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V2_4_0)]
        [Removed(Versions.V3_0_0)]
        public static Column UnboundedFollowing()
        {
            return ApplyFunction("unboundedFollowing");
        }

        /// <summary>
        /// Window function: returns the special frame boundary that represents the current
        /// row in the window partition.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 2.4 and removed in Spark 3.0.
        /// </remarks>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V2_4_0)]
        [Removed(Versions.V3_0_0)]
        public static Column CurrentRow()
        {
            return ApplyFunction("currentRow");
        }

        /// <summary>
        /// Window function: returns the cumulative distribution of values within a window
        /// partition, i.e. the fraction of rows that are below the current row.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column CumeDist()
        {
            return ApplyFunction("cume_dist");
        }

        /// <summary>
        /// Window function: returns the rank of rows within a window partition, without any gaps.
        /// </summary>
        /// <remarks>This is equivalent to the DENSE_RANK function in SQL.</remarks>
        /// <returns>Column object</returns>
        public static Column DenseRank()
        {
            return ApplyFunction("dense_rank");
        }

        /// <summary>
        /// Window function: returns the value that is 'offset' rows before the current row,
        /// and null if there is less than 'offset' rows before the current row.
        /// For example, an 'offset' of one will return the previous row at any given point
        /// in the window partition.
        /// </summary>
        /// <remarks>This is equivalent to the LAG function in SQL.</remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="defaultValue">Default value when the offset row doesn't exist</param>
        /// <returns>Column object</returns>
        public static Column Lag(Column column, int offset, object defaultValue = null)
        {
            return (defaultValue != null) ?
                ApplyFunction("lag", column, offset, defaultValue) :
                ApplyFunction("lag", column, offset);
        }

        /// <summary>
        /// Window function: returns the value that is 'offset' rows before the current row,
        /// and null if there is less than 'offset' rows before the current row.
        /// For example, an 'offset' of one will return the previous row at any given point
        /// in the window partition.
        /// </summary>
        /// <remarks>This is equivalent to the LAG function in SQL.</remarks>
        /// <param name="columnName">Column name</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="defaultValue">Default value when the offset row doesn't exist</param>
        /// <returns>Column object</returns>
        public static Column Lag(string columnName, int offset, object defaultValue = null)
        {
            return (defaultValue != null) ?
                ApplyFunction("lag", columnName, offset, defaultValue) :
                ApplyFunction("lag", columnName, offset);
        }

        /// <summary>
        /// Window function: returns the value that is 'offset' rows before the current row,
        /// and null if there is less than 'offset' rows before the current row.
        /// 'ignoreNulls' determines whether null values of row are included in or eliminated from the 
        /// calculation.
        /// For example, an 'offset' of one will return the previous row at any given point
        /// in the window partition.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="defaultValue">Default value when the offset row doesn't exist</param>
        /// <param name="ignoreNulls">Boolean to determine whether null values are included or not</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Lag(Column column, int offset, object defaultValue, bool ignoreNulls)
        {
            return ApplyFunction("lag", column, offset, defaultValue, ignoreNulls);
        }

        /// <summary>
        /// Window function: returns the value that is 'offset' rows after the current row,
        /// and null if there is less than 'offset' rows after the current row.
        /// For example, an 'offset' of one will return the next row at any given point
        /// in the window partition.
        /// </summary>
        /// <remarks>This is equivalent to the LEAD function in SQL.</remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="defaultValue">Default value when the offset row doesn't exist</param>
        /// <returns>Column object</returns>
        public static Column Lead(Column column, int offset, object defaultValue = null)
        {
            return (defaultValue != null) ?
                ApplyFunction("lead", column, offset, defaultValue) :
                ApplyFunction("lead", column, offset);
        }

        /// <summary>
        /// Window function: returns the value that is 'offset' rows after the current row,
        /// and null if there is less than 'offset' rows after the current row.
        /// For example, an 'offset' of one will return the next row at any given point
        /// in the window partition.
        /// </summary>
        /// <remarks>This is equivalent to the LEAD function in SQL.</remarks>
        /// <param name="columnName">Column name</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="defaultValue">Default value when the offset row doesn't exist</param>
        /// <returns>Column object</returns>
        public static Column Lead(string columnName, int offset, object defaultValue = null)
        {
            return (defaultValue != null) ?
                ApplyFunction("lead", columnName, offset, defaultValue) :
                ApplyFunction("lead", columnName, offset);
        }

        /// <summary>
        /// Window function: returns the value that is 'offset' rows after the current row,
        /// and null if there is less than 'offset' rows after the current row.
        /// 'ignoreNulls' determines whether null values of row are included in or eliminated from the 
        /// calculation.
        /// For example, an 'offset' of one will return the next row at any given point
        /// in the window partition.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="defaultValue">Default value when the offset row doesn't exist</param>
        /// <param name="ignoreNulls">Boolean to determine whether null values are included or not</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Lead(Column column, int offset, object defaultValue, bool ignoreNulls)
        {
            return ApplyFunction("lead", column, offset, defaultValue, ignoreNulls);
        }

        /// <summary>
        /// Returns the value that is the `offset`th row of the window frame
        /// (counting from 1), and `null` if the size of window frame is less than `offset` rows.
        ///
        /// It will return the `offset`th non-null value it sees when ignoreNulls is set to true.
        /// If all values are null, then null is returned.
        ///
        /// This is equivalent to the nth_value function in SQL.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="offset">Offset from the current row</param>
        /// <param name="ignoreNulls">To ignore null or not</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column NthValue(Column column, int offset, bool ignoreNulls = false)
        {
            return ApplyFunction("nth_value", column, offset, ignoreNulls);
        }

        /// <summary>
        /// Window function: returns the ntile group id (from 1 to `n` inclusive) in an ordered
        /// window partition. For example, if `n` is 4, the first quarter of the rows will get
        /// value 1, the second quarter will get 2, the third quarter will get 3, and the last
        /// quarter will get 4.
        /// </summary>
        /// <remarks>This is equivalent to the NTILE function in SQL.</remarks>
        /// <param name="n">Number of buckets</param>
        /// <returns>Column object</returns>
        public static Column Ntile(int n)
        {
            return ApplyFunction("ntile", n);
        }

        /// <summary>
        /// Window function: returns the relative rank (i.e. percentile) of rows within
        /// a window partition.
        /// </summary>
        /// <remarks>This is equivalent to the PERCENT_RANK function in SQL.</remarks>
        /// <returns>Column object</returns>
        public static Column PercentRank()
        {
            return ApplyFunction("percent_rank");
        }

        /// <summary>
        /// Window function: returns the rank of rows within a window partition.
        /// </summary>
        /// <remarks>This is equivalent to the RANK function in SQL.</remarks>
        /// <returns>Column object</returns>
        public static Column Rank()
        {
            return ApplyFunction("rank");
        }

        /// <summary>
        /// Window function: returns a sequential number starting at 1 within a window partition.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column RowNumber()
        {
            return ApplyFunction("row_number");
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Non-aggregate functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Computes the absolute value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Abs(Column column)
        {
            return ApplyFunction("abs", column);
        }

        /// <summary>
        /// Creates a new array column. The input columns must all have the same data type.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Array(params Column[] columns)
        {
            return ApplyFunction("array", (object)columns);
        }

        /// <summary>
        /// Creates a new array column. The input columns must all have the same data type.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <param name="columnNames">Additional column names</param>
        /// <returns>Column object</returns>
        public static Column Array(string columnName, params string[] columnNames)
        {
            return ApplyFunction("array", columnName, columnNames);
        }

        /// <summary>
        /// Creates a new map column.
        /// </summary>
        /// <remarks>
        /// The input columns must be grouped as key-value pairs, e.g.
        /// (key1, value1, key2, value2, ...). The key columns must all have the same data type,
        /// and can't be null. The value columns must all have the same data type.
        /// </remarks>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Map(params Column[] columns)
        {
            return ApplyFunction("map", (object)columns);
        }

        /// <summary>
        /// Creates a new map column. The array in the first column is used for keys. The array
        /// in the second column is used for values. All elements in the array for key should
        /// not be null.
        /// </summary>
        /// <param name="key">Column expression for key</param>
        /// <param name="values">Column expression for values</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column MapFromArrays(Column key, Column values)
        {
            return ApplyFunction("map_from_arrays", key, values);
        }

        /// <summary>
        /// Marks a DataFrame as small enough for use in broadcast joins.
        /// </summary>
        /// <param name="df">DataFrame to apply</param>
        /// <returns>DataFrame object</returns>
        public static DataFrame Broadcast(DataFrame df)
        {
            return new DataFrame(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    "broadcast",
                    df));
        }

        /// <summary>
        /// Returns the first column that is not null, or null if all inputs are null.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Coalesce(params Column[] columns)
        {
            return ApplyFunction("coalesce", (object)columns);
        }

        /// <summary>
        /// Creates a string column for the file name of the current Spark task.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column InputFileName()
        {
            return ApplyFunction("input_file_name");
        }

        /// <summary>
        /// Return true iff the column is NaN.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column IsNaN(Column column)
        {
            return ApplyFunction("isnan", column);
        }

        /// <summary>
        /// Return true iff the column is null.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column IsNull(Column column)
        {
            return ApplyFunction("isnull", column);
        }

        /// <summary>
        /// A column expression that generates monotonically increasing 64-bit integers.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column MonotonicallyIncreasingId()
        {
            return ApplyFunction("monotonically_increasing_id");
        }

        /// <summary>
        /// Returns col1 if it is not NaN, or col2 if col1 is NaN.
        /// </summary>
        /// <remarks>
        /// Both inputs should be floating point columns (DoubleType or FloatType).
        /// </remarks>
        /// <param name="column1">Column one to apply</param>
        /// <param name="column2">Column two to apply</param>
        /// <returns>Column object</returns>
        public static Column NaNvl(Column column1, Column column2)
        {
            return ApplyFunction("nanvl", column1, column2);
        }

        /// <summary>
        /// Unary minus, i.e. negate the expression.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Negate(Column column)
        {
            return ApplyFunction("negate", column);
        }

        /// <summary>
        /// Inversion of boolean expression, i.e. NOT.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Not(Column column)
        {
            return ApplyFunction("not", column);
        }

        /// <summary>
        /// Generate a random column with independent and identically distributed (i.i.d.)
        /// samples from U[0.0, 1.0].
        /// </summary>
        /// <remarks>
        /// This is non-deterministic when data partitions are not fixed.
        /// </remarks>
        /// <param name="seed">Random seed</param>
        /// <returns>Column object</returns>
        public static Column Rand(long seed)
        {
            return ApplyFunction("rand", seed);
        }

        /// <summary>
        /// Generate a random column with independent and identically distributed (i.i.d.)
        /// samples from U[0.0, 1.0].
        /// </summary>
        /// <returns>Column object</returns>
        public static Column Rand()
        {
            return ApplyFunction("rand");
        }

        /// <summary>
        /// Generate a random column with independent and identically distributed (i.i.d.)
        /// samples from the standard normal distribution.
        /// </summary>
        /// <remarks>
        /// This is non-deterministic when data partitions are not fixed.
        /// </remarks>
        /// <param name="seed">Random seed</param>
        /// <returns>Column object</returns>
        public static Column Randn(long seed)
        {
            return ApplyFunction("randn", seed);
        }

        /// <summary>
        /// Generate a random column with independent and identically distributed (i.i.d.)
        /// samples from the standard normal distribution.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column Randn()
        {
            return ApplyFunction("randn");
        }

        /// <summary>
        /// Partition ID.
        /// </summary>
        /// <remarks>
        /// This is non-deterministic because it depends on data partitioning and task scheduling.
        /// </remarks>
        /// <returns>Column object</returns>
        public static Column SparkPartitionId()
        {
            return ApplyFunction("spark_partition_id");
        }

        /// <summary>
        /// Computes the square root of the specified float value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Sqrt(Column column)
        {
            return ApplyFunction("sqrt", column);
        }

        /// <summary>
        /// Computes the square root of the specified float value.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Sqrt(string columnName)
        {
            return ApplyFunction("sqrt", columnName);
        }

        /// <summary>
        /// Creates a new struct column that composes multiple input columns.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Struct(params Column[] columns)
        {
            return ApplyFunction("struct", (object)columns);
        }

        /// <summary>
        /// Creates a new struct column that composes multiple input columns.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <param name="columnNames">Additional column names</param>
        /// <returns>Column object</returns>
        public static Column Struct(string columnName, params string[] columnNames)
        {
            return ApplyFunction("struct", columnName, columnNames);
        }

        /// <summary>
        /// Evaluates a condition and returns one of multiple possible result expressions.
        /// If otherwise is not defined at the end, null is returned for
        /// unmatched conditions.
        /// </summary>
        /// <param name="condition">The condition to check.</param>
        /// <param name="value">The value to set if the condition is true.</param>
        /// <returns>Column object</returns>
        public static Column When(Column condition, object value)
        {
            return ApplyFunction("when", condition, value);
        }

        /// <summary>
        /// Computes bitwise NOT.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column BitwiseNOT(Column column)
        {
            return ApplyFunction("bitwiseNOT", column);
        }

        /// <summary>
        /// Computes bitwise NOT of a number.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Bitwise_Not(Column column)
        {
            return ApplyFunction("bitwise_not", column);
        }

        /// <summary>
        /// Parses the expression string into the column that it represents.
        /// </summary>
        /// <param name="expr">Expression string</param>
        /// <returns>Column object</returns>
        public static Column Expr(string expr)
        {
            return ApplyFunction("expr", expr);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Math functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Inverse cosine of `column` in radians, as if computed by `java.lang.Math.acos`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Acos(Column column)
        {
            return ApplyFunction("acos", column);
        }

        /// <summary>
        /// Inverse cosine of `columnName` in radians, as if computed by `java.lang.Math.acos`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Acos(string columnName)
        {
            return ApplyFunction("acos", columnName);
        }

        /// <summary>
        /// Inverse hyperbolic cosine of <paramref name="column"/>.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Acosh(Column column)
        {
            return ApplyFunction("acosh", column);
        }

        /// <summary>
        /// Inverse hyperbolic cosine of <paramref name="columnName"/>.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Acosh(string columnName)
        {
            return ApplyFunction("acosh", columnName);
        }

        /// <summary>
        /// Inverse sine of `column` in radians, as if computed by `java.lang.Math.asin`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Asin(Column column)
        {
            return ApplyFunction("asin", column);
        }

        /// <summary>
        /// Inverse sine of `columnName` in radians, as if computed by `java.lang.Math.asin`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Asin(string columnName)
        {
            return ApplyFunction("asin", columnName);
        }

        /// <summary>
        /// Inverse hyperbolic sine of <paramref name="column"/>.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Asinh(Column column)
        {
            return ApplyFunction("asinh", column);
        }

        /// <summary>
        /// Inverse hyperbolic sine of <paramref name="columnName"/>.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Asinh(string columnName)
        {
            return ApplyFunction("asinh", columnName);
        }

        /// <summary>
        /// Inverse tangent of `column` in radians, as if computed by `java.lang.Math.atan`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Atan(Column column)
        {
            return ApplyFunction("atan", column);
        }

        /// <summary>
        /// Inverse tangent of `columnName` in radians, as if computed by `java.lang.Math.atan`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Atan(string columnName)
        {
            return ApplyFunction("atan", columnName);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="y">Coordinate on y-axis</param>
        /// <param name="x">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(Column y, Column x)
        {
            return ApplyFunction("atan2", y, x);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="y">Coordinate on y-axis</param>
        /// <param name="xName">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(Column y, string xName)
        {
            return ApplyFunction("atan2", y, xName);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="yName">Coordinate on y-axis</param>
        /// <param name="x">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(string yName, Column x)
        {
            return ApplyFunction("atan2", yName, x);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="yName">Coordinate on y-axis</param>
        /// <param name="xName">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(string yName, string xName)
        {
            return ApplyFunction("atan2", yName, xName);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="y">Coordinate on y-axis</param>
        /// <param name="xValue">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(Column y, double xValue)
        {
            return ApplyFunction("atan2", y, xValue);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="yName">Coordinate on y-axis</param>
        /// <param name="xValue">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(string yName, double xValue)
        {
            return ApplyFunction("atan2", yName, xValue);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="yValue">Coordinate on y-axis</param>
        /// <param name="x">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(double yValue, Column x)
        {
            return ApplyFunction("atan2", yValue, x);
        }

        /// <summary>
        /// Computes atan2 for the given `x` and `y`.
        /// </summary>
        /// <param name="yValue">Coordinate on y-axis</param>
        /// <param name="xName">Coordinate on x-axis</param>
        /// <returns>Column object</returns>
        public static Column Atan2(double yValue, string xName)
        {
            return ApplyFunction("atan2", yValue, xName);
        }

        /// <summary>
        /// Inverse hyperbolic tangent of <paramref name="column"/>.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Atanh(Column column)
        {
            return ApplyFunction("atanh", column);
        }

        /// <summary>
        /// Inverse hyperbolic tangent of <paramref name="columnName"/>.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Atanh(string columnName)
        {
            return ApplyFunction("atanh", columnName);
        }

        /// <summary>
        /// An expression that returns the string representation of the binary value
        /// of the given long column. For example, bin("12") returns "1100".
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Bin(Column column)
        {
            return ApplyFunction("bin", column);
        }

        /// <summary>
        /// An expression that returns the string representation of the binary value
        /// of the given long column. For example, bin("12") returns "1100".
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Bin(string columnName)
        {
            return ApplyFunction("bin", columnName);
        }

        /// <summary>
        /// Computes the cube-root of the given column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Cbrt(Column column)
        {
            return ApplyFunction("cbrt", column);
        }

        /// <summary>
        /// Computes the cube-root of the given column.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Cbrt(string columnName)
        {
            return ApplyFunction("cbrt", columnName);
        }

        /// <summary>
        /// Computes the ceiling of the given value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Ceil(Column column)
        {
            return ApplyFunction("ceil", column);
        }

        /// <summary>
        /// Computes the ceiling of the given value.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Ceil(string columnName)
        {
            return ApplyFunction("ceil", columnName);
        }

        /// <summary>
        /// Convert a number in a string column from one base to another.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="fromBase">Source base number</param>
        /// <param name="toBase">Target base number</param>
        /// <returns>Column object</returns>
        public static Column Conv(Column column, int fromBase, int toBase)
        {
            return ApplyFunction("conv", column, fromBase, toBase);
        }

        /// <summary>
        /// Computes cosine of the angle, as if computed by `java.lang.Math.cos`
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Cos(Column column)
        {
            return ApplyFunction("cos", column);
        }

        /// <summary>
        /// Computes cosine of the angle, as if computed by `java.lang.Math.cos`
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Cos(string columnName)
        {
            return ApplyFunction("cos", columnName);
        }

        /// <summary>
        /// Computes hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Cosh(Column column)
        {
            return ApplyFunction("cosh", column);
        }

        /// <summary>
        /// Computes hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Cosh(string columnName)
        {
            return ApplyFunction("cosh", columnName);
        }

        /// <summary>
        /// Computes the exponential of the given value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Exp(Column column)
        {
            return ApplyFunction("exp", column);
        }

        /// <summary>
        /// Computes the exponential of the given value.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Exp(string columnName)
        {
            return ApplyFunction("exp", columnName);
        }

        /// <summary>
        /// Computes the exponential of the given value minus one.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Expm1(Column column)
        {
            return ApplyFunction("expm1", column);
        }

        /// <summary>
        /// Computes the exponential of the given value minus one.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Expm1(string columnName)
        {
            return ApplyFunction("expm1", columnName);
        }

        /// <summary>
        /// Computes the factorial of the given value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Factorial(Column column)
        {
            return ApplyFunction("factorial", column);
        }

        /// <summary>
        /// Computes the floor of the given value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Floor(Column column)
        {
            return ApplyFunction("floor", column);
        }

        /// <summary>
        /// Computes the floor of the given value.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Floor(string columnName)
        {
            return ApplyFunction("floor", columnName);
        }

        /// <summary>
        /// Returns the greatest value of the list of values, skipping null values.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Greatest(params Column[] columns)
        {
            return ApplyFunction("greatest", (object)columns);
        }

        /// <summary>
        /// Returns the greatest value of the list of column names, skipping null values.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <param name="columnNames">Additional column names</param>
        /// <returns>Column object</returns>
        public static Column Greatest(string columnName, params string[] columnNames)
        {
            return ApplyFunction("greatest", columnName, columnNames);
        }

        /// <summary>
        /// Computes hex value of the given column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Hex(Column column)
        {
            return ApplyFunction("hex", column);
        }

        /// <summary>
        /// Inverse of hex. Interprets each pair of characters as a hexadecimal number
        /// and converts to the byte representation of number.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Unhex(Column column)
        {
            return ApplyFunction("unhex", column);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Hypot(Column left, Column right)
        {
            return ApplyFunction("hypot", left, right);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="rightName">Right side column name</param>
        /// <returns>Column object</returns>
        public static Column Hypot(Column left, string rightName)
        {
            return ApplyFunction("hypot", left, rightName);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="leftName">Left side column name</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Hypot(string leftName, Column right)
        {
            return ApplyFunction("hypot", leftName, right);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="leftName">Left side column name</param>
        /// <param name="rightName">Right side column name</param>
        /// <returns>Column object</returns>
        public static Column Hypot(string leftName, string rightName)
        {
            return ApplyFunction("hypot", leftName, rightName);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="right">Right side value</param>
        /// <returns>Column object</returns>
        public static Column Hypot(Column left, double right)
        {
            return ApplyFunction("hypot", left, right);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="leftName">Left side column name</param>
        /// <param name="right">Right side value</param>
        /// <returns>Column object</returns>
        public static Column Hypot(string leftName, double right)
        {
            return ApplyFunction("hypot", leftName, right);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="left">Left side value</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Hypot(double left, Column right)
        {
            return ApplyFunction("hypot", left, right);
        }

        /// <summary>
        /// Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
        /// </summary>
        /// <param name="left">Left side value</param>
        /// <param name="rightName">Right side column name</param>
        /// <returns>Column object</returns>
        public static Column Hypot(double left, string rightName)
        {
            return ApplyFunction("hypot", left, rightName);
        }

        /// <summary>
        /// Returns the least value of the list of values, skipping null values.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Least(params Column[] columns)
        {
            return ApplyFunction("least", (object)columns);
        }

        /// <summary>
        /// Returns the least value of the list of values, skipping null values.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <param name="columnNames">Additional column names</param>
        /// <returns>Column object</returns>
        public static Column Least(string columnName, params string[] columnNames)
        {
            return ApplyFunction("least", columnName, columnNames);
        }

        /// <summary>
        /// Computes the natural logarithm of the given value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Log(Column column)
        {
            return ApplyFunction("log", column);
        }

        /// <summary>
        /// Computes the natural logarithm of the given value.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Log(string columnName)
        {
            return ApplyFunction("log", columnName);
        }

        /// <summary>
        /// Computes the first argument-base logarithm of the second argument.
        /// </summary>
        /// <param name="logBase">Base for logarithm</param>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Log(double logBase, Column column)
        {
            return ApplyFunction("log", logBase, column);
        }

        /// <summary>
        /// Computes the first argument-base logarithm of the second argument.
        /// </summary>
        /// <param name="logBase">Base for logarithm</param>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Log(double logBase, string columnName)
        {
            return ApplyFunction("log", logBase, columnName);
        }

        /// <summary>
        /// Computes the logarithm of the given value in base 10.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Log10(Column column)
        {
            return ApplyFunction("log10", column);
        }

        /// <summary>
        /// Computes the logarithm of the given value in base 10.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Log10(string columnName)
        {
            return ApplyFunction("log10", columnName);
        }

        /// <summary>
        /// Computes the natural logarithm of the given value plus one.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Log1p(Column column)
        {
            return ApplyFunction("log1p", column);
        }

        /// <summary>
        /// Computes the natural logarithm of the given value plus one.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Log1p(string columnName)
        {
            return ApplyFunction("log1p", columnName);
        }

        /// <summary>
        /// Computes the logarithm of the given column in base 2.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Log2(Column column)
        {
            return ApplyFunction("log2", column);
        }

        /// <summary>
        /// Computes the logarithm of the given column in base 2.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Log2(string columnName)
        {
            return ApplyFunction("log2", columnName);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Pow(Column left, Column right)
        {
            return ApplyFunction("pow", left, right);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="rightName">Right side column name</param>
        /// <returns>Column object</returns>
        public static Column Pow(Column left, string rightName)
        {
            return ApplyFunction("pow", left, rightName);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="leftName">Left side column name</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Pow(string leftName, Column right)
        {
            return ApplyFunction("pow", leftName, right);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="leftName">Left side column name</param>
        /// <param name="rightName">Right side column name</param>
        /// <returns>Column object</returns>
        public static Column Pow(string leftName, string rightName)
        {
            return ApplyFunction("pow", leftName, rightName);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="right">Right side value</param>
        /// <returns>Column object</returns>
        public static Column Pow(Column left, double right)
        {
            return ApplyFunction("pow", left, right);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="leftName">Left side column name</param>
        /// <param name="right">Right side value</param>
        /// <returns>Column object</returns>
        public static Column Pow(string leftName, double right)
        {
            return ApplyFunction("pow", leftName, right);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="left">Left side value</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Pow(double left, Column right)
        {
            return ApplyFunction("pow", left, right);
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="left">Left side value</param>
        /// <param name="rightName">Right side column name</param>
        /// <returns>Column object</returns>
        public static Column Pow(double left, string rightName)
        {
            return ApplyFunction("pow", left, rightName);
        }

        /// <summary>
        /// Returns the positive value of dividend mod divisor.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Pmod(Column left, Column right)
        {
            return ApplyFunction("pmod", left, right);
        }

        /// <summary>
        /// Returns the double value that is closest in value to the argument and
        /// is equal to a mathematical integer.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Rint(Column column)
        {
            return ApplyFunction("rint", column);
        }

        /// <summary>
        /// Returns the double value that is closest in value to the argument and
        /// is equal to a mathematical integer.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Rint(string columnName)
        {
            return ApplyFunction("rint", columnName);
        }

        /// <summary>
        /// Returns the value of the `column` rounded to 0 decimal places with
        /// HALF_UP round mode.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Round(Column column)
        {
            return ApplyFunction("round", column);
        }

        /// <summary>
        /// Returns the value of the `column` rounded to `scale` decimal places with
        /// HALF_UP round mode.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="scale">Scale factor</param>
        /// <returns>Column object</returns>
        public static Column Round(Column column, int scale)
        {
            return ApplyFunction("round", column, scale);
        }

        /// <summary>
        /// Returns the value of the `column` rounded to 0 decimal places with
        /// HALF_EVEN round mode.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Bround(Column column)
        {
            return ApplyFunction("bround", column);
        }

        /// <summary>
        /// Returns the value of the `column` rounded to `scale` decimal places with
        /// HALF_EVEN round mode.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="scale">Scale factor</param>
        /// <returns>Column object</returns>
        public static Column Bround(Column column, int scale)
        {
            return ApplyFunction("bround", column, scale);
        }


        /// <summary>
        /// Shift the given value `numBits` left.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">Number of bits to shift</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column ShiftLeft(Column column, int numBits)
        {
            return ApplyFunction("shiftLeft", column, numBits);
        }

        /// <summary>
        /// Shift the given value `numBits` left.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">Number of bits to shift</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Shiftleft(Column column, int numBits)
        {
            return ApplyFunction("shiftleft", column, numBits);
        }

        /// <summary>
        /// (Signed) shift the given value `numBits` right.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">Number of bits to shift</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column ShiftRight(Column column, int numBits)
        {
            return ApplyFunction("shiftRight", column, numBits);
        }

        /// <summary>
        /// (Signed) shift the given value `numBits` right.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">Number of bits to shift</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Shiftright(Column column, int numBits)
        {
            return ApplyFunction("shiftright", column, numBits);
        }

        /// <summary>
        /// Unsigned shift the given value `numBits` right.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">Number of bits to shift</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column ShiftRightUnsigned(Column column, int numBits)
        {
            return ApplyFunction("shiftRightUnsigned", column, numBits);
        }

        /// <summary>
        /// Unsigned shift the given value `numBits` right.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">Number of bits to shift</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Shiftrightunsigned(Column column, int numBits)
        {
            return ApplyFunction("shiftrightunsigned", column, numBits);
        }

        /// <summary>
        /// Computes the signum of the given value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Signum(Column column)
        {
            return ApplyFunction("signum", column);
        }

        /// <summary>
        /// Computes the signum of the given value.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Signum(string columnName)
        {
            return ApplyFunction("signum", columnName);
        }

        /// <summary>
        /// Computes sine of the angle, as if computed by `java.lang.Math.sin`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Sin(Column column)
        {
            return ApplyFunction("sin", column);
        }

        /// <summary>
        /// Computes sine of the angle, as if computed by `java.lang.Math.sin`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Sin(string columnName)
        {
            return ApplyFunction("sin", columnName);
        }

        /// <summary>
        /// Computes hyperbolic sine of the angle, as if computed by `java.lang.Math.sin`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Sinh(Column column)
        {
            return ApplyFunction("sinh", column);
        }

        /// <summary>
        /// Computes hyperbolic sine of the angle, as if computed by `java.lang.Math.sin`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Sinh(string columnName)
        {
            return ApplyFunction("sinh", columnName);
        }

        /// <summary>
        /// Computes tangent of the given value, as if computed by `java.lang.Math.tan`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Tan(Column column)
        {
            return ApplyFunction("tan", column);
        }

        /// <summary>
        /// Computes tangent of the given value, as if computed by `java.lang.Math.tan`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Tan(string columnName)
        {
            return ApplyFunction("tan", columnName);
        }

        /// <summary>
        /// Computes hyperbolic tangent of the given value, as if computed by
        /// `java.lang.Math.tanh`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Tanh(Column column)
        {
            return ApplyFunction("tanh", column);
        }

        /// <summary>
        /// Computes hyperbolic tangent of the given value, as if computed by
        /// `java.lang.Math.tanh`.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Tanh(string columnName)
        {
            return ApplyFunction("tanh", columnName);
        }

        /// <summary>
        /// Converts an angle measured in radians to an approximately equivalent angle
        /// measured in degrees.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Degrees(Column column)
        {
            return ApplyFunction("degrees", column);
        }

        /// <summary>
        /// Converts an angle measured in radians to an approximately equivalent angle
        /// measured in degrees.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Degrees(string columnName)
        {
            return ApplyFunction("degrees", columnName);
        }

        /// <summary>
        /// Converts an angle measured in degrees to an approximately equivalent angle
        /// measured in radians.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Radians(Column column)
        {
            return ApplyFunction("radians", column);
        }

        /// <summary>
        /// Converts an angle measured in degrees to an approximately equivalent angle
        /// measured in radians.
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Column object</returns>
        public static Column Radians(string columnName)
        {
            return ApplyFunction("radians", columnName);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Misc functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Calculates the MD5 digest of a binary column and returns the value
        /// as a 32 character hex string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Md5(Column column)
        {
            return ApplyFunction("md5", column);
        }

        /// <summary>
        /// Calculates the SHA-1 digest of a binary column and returns the value
        /// as a 40 character hex string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Sha1(Column column)
        {
            return ApplyFunction("sha1", column);
        }

        /// <summary>
        /// Calculates the SHA-2 family of hash functions of a binary column and
        /// returns the value as a hex string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="numBits">One of 224, 256, 384 or 512</param>
        /// <returns>Column object</returns>
        public static Column Sha2(Column column, int numBits)
        {
            return ApplyFunction("sha2", column, numBits);
        }

        /// <summary>
        /// Calculates the cyclic redundancy check value  (CRC32) of a binary column and
        /// returns the value as a bigint.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Crc32(Column column)
        {
            return ApplyFunction("crc32", column);
        }

        /// <summary>
        /// Calculates the hash code of given columns, and returns the result as an int column.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Hash(params Column[] columns)
        {
            return ApplyFunction("hash", (object)columns);
        }

        /// <summary>
        /// Calculates the hash code of given columns using the 64-bit variant of the xxHash
        /// algorithm, and returns the result as a long column.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column XXHash64(params Column[] columns)
        {
            return ApplyFunction("xxhash64", (object)columns);
        }

        /// <summary>
        /// Returns null if the condition is true, and throws an exception otherwise.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column AssertTrue(Column column)
        {
            return ApplyFunction("assert_true", column);
        }

        /// <summary>
        /// Returns null if the condition is true; throws an exception with the error message otherwise.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="errMsg">Error message</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column AssertTrue(Column column, Column errMsg)
        {
            return ApplyFunction("assert_true", column, errMsg);
        }

        /// <summary>
        /// Throws an exception with the provided error message.
        /// </summary>
        /// <param name="errMsg">Error message</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column RaiseError(Column errMsg)
        {
            return ApplyFunction("raise_error", errMsg);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // String functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Computes the numeric value of the first character of the string column, and returns
        /// the result as an int column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Ascii(Column column)
        {
            return ApplyFunction("ascii", column);
        }

        /// <summary>
        /// Computes the BASE64 encoding of a binary column and returns it as a string column.
        /// </summary>
        /// <remarks>
        /// This is the reverse of unbase64.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Base64(Column column)
        {
            return ApplyFunction("base64", column);
        }

        /// <summary>
        /// Concatenates multiple input string columns together into a single string column,
        /// using the given separator.
        /// </summary>
        /// <param name="sep">Separator used for string concatenation</param>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column ConcatWs(string sep, params Column[] columns)
        {
            return ApplyFunction("concat_ws", sep, columns);
        }

        /// <summary>
        /// Computes the first argument into a string from a binary using the provided
        /// character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE',
        /// 'UTF-16')
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="charset">Character set</param>
        /// <returns>Column object</returns>
        public static Column Decode(Column column, string charset)
        {
            return ApplyFunction("decode", column, charset);
        }

        /// <summary>
        /// Computes the first argument into a binary from a string using the provided
        /// character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE',
        /// 'UTF-16')
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="charset">Character set</param>
        /// <returns>Column object</returns>
        public static Column Encode(Column column, string charset)
        {
            return ApplyFunction("encode", column, charset);
        }

        /// <summary>
        /// Formats the given numeric `column` to a format like '#,###,###.##',
        /// rounded to the given `d` decimal places with HALF_EVEN round mode,
        /// and returns the result as a string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="d">Decimal places for rounding</param>
        /// <returns>Column object</returns>
        public static Column FormatNumber(Column column, int d)
        {
            return ApplyFunction("format_number", column, d);
        }

        /// <summary>
        /// Formats the arguments in printf-style and returns the result as a string column.
        /// </summary>
        /// <param name="format">Printf-style format</param>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column FormatString(string format, params Column[] columns)
        {
            return ApplyFunction("format_string", format, columns);
        }

        /// <summary>
        /// Returns a new string column by converting the first letter of each word to uppercase.
        /// Words are delimited by whitespace.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column InitCap(Column column)
        {
            return ApplyFunction("initcap", column);
        }

        /// <summary>
        /// Locate the position of the first occurrence of the given substring.
        /// </summary>
        /// <remarks>
        /// The position is not zero based, but 1 based index. Returns 0 if the given substring
        /// could not be found.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="substring">Substring to find</param>
        /// <returns>Column object</returns>
        public static Column Instr(Column column, string substring)
        {
            return ApplyFunction("instr", column, substring);
        }

        /// <summary>
        /// Computes the character length of a given string or number of bytes of a binary string.
        /// </summary>
        /// <remarks>
        /// The length of character strings includes the trailing spaces. The length of binary
        /// strings includes binary zeros.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Length(Column column)
        {
            return ApplyFunction("length", column);
        }

        /// <summary>
        /// Converts a string column to lower case.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Lower(Column column)
        {
            return ApplyFunction("lower", column);
        }

        /// <summary>
        /// Computes the Levenshtein distance of the two given string columns.
        /// </summary>
        /// <param name="left">Left side column to apply</param>
        /// <param name="right">Right side column to apply</param>
        /// <returns>Column object</returns>
        public static Column Levenshtein(Column left, Column right)
        {
            return ApplyFunction("levenshtein", left, right);
        }

        /// <summary>
        /// Locate the position of the first occurrence of the given substring.
        /// </summary>
        /// <remarks>
        /// The position is not zero based, but 1 based index. Returns 0 if the given substring
        /// could not be found.
        /// </remarks>
        /// <param name="substring">Substring to find</param>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Locate(string substring, Column column)
        {
            return ApplyFunction("locate", substring, column);
        }

        /// <summary>
        /// Locate the position of the first occurrence of the given substring
        /// starting from the given position offset.
        /// </summary>
        /// <remarks>
        /// The position is not zero based, but 1 based index. Returns 0 if the given substring
        /// could not be found.
        /// </remarks>
        /// <param name="substring">Substring to find</param>
        /// <param name="column">Column to apply</param>
        /// <param name="pos">Offset to start the search</param>
        /// <returns>Column object</returns>
        public static Column Locate(string substring, Column column, int pos)
        {
            return ApplyFunction("locate", substring, column, pos);
        }

        /// <summary>
        /// Left-pad the string column with pad to the given length `len`. If the string column is
        /// longer than `len`, the return value is shortened to `len` characters.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="len">Length of padded string</param>
        /// <param name="pad">String used for padding</param>
        /// <returns>Column object</returns>
        public static Column Lpad(Column column, int len, string pad)
        {
            return ApplyFunction("lpad", column, len, pad);
        }

        /// <summary>
        /// Trim the spaces from left end for the given string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Ltrim(Column column)
        {
            return ApplyFunction("ltrim", column);
        }

        /// <summary>
        /// Trim the specified character string from left end for the given string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="trimString">String to trim</param>
        /// <returns>Column object</returns>
        public static Column Ltrim(Column column, string trimString)
        {
            return ApplyFunction("ltrim", column, trimString);
        }

        /// <summary>
        /// Extract a specific group matched by a Java regex, from the specified string column.
        /// </summary>
        /// <remarkes>
        /// If the regex did not match, or the specified group did not match,
        /// an empty string is returned.
        /// </remarkes>
        /// <param name="column">Column to apply</param>
        /// <param name="exp">Regular expression to match</param>
        /// <param name="groupIdx">Group index to extract</param>
        /// <returns>Column object</returns>
        public static Column RegexpExtract(Column column, string exp, int groupIdx)
        {
            return ApplyFunction("regexp_extract", column, exp, groupIdx);
        }

        /// <summary>
        /// Replace all substrings of the specified string value that match the pattern with
        /// the given replacement string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="pattern">Regular expression to match</param>
        /// <param name="replacement">String to replace with</param>
        /// <returns>Column object</returns>
        public static Column RegexpReplace(Column column, string pattern, string replacement)
        {
            return ApplyFunction("regexp_replace", column, pattern, replacement);
        }

        /// <summary>
        /// Replace all substrings of the specified string value that match the pattern with
        /// the given replacement string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="pattern">Regular expression to match</param>
        /// <param name="replacement">String to replace with</param>
        /// <returns>Column object</returns>
        public static Column RegexpReplace(Column column, Column pattern, Column replacement)
        {
            return ApplyFunction("regexp_replace", column, pattern, replacement);
        }

        /// <summary>
        /// Decodes a BASE64 encoded string column and returns it as a binary column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Unbase64(Column column)
        {
            return ApplyFunction("unbase64", column);
        }

        /// <summary>
        /// Right-pad the string column with pad to the given length `len`. If the string column is
        /// longer than `len`, the return value is shortened to `len` characters.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="len">Length of padded string</param>
        /// <param name="pad">String used for padding</param>
        /// <returns>Column object</returns>
        public static Column Rpad(Column column, int len, string pad)
        {
            return ApplyFunction("rpad", column, len, pad);
        }

        /// <summary>
        /// Repeats a string column `n` times, and returns it as a new string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="n">Repeatation number</param>
        /// <returns>Column object</returns>
        public static Column Repeat(Column column, int n)
        {
            return ApplyFunction("repeat", column, n);
        }

        /// <summary>
        /// Trim the spaces from right end for the specified string value.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Rtrim(Column column)
        {
            return ApplyFunction("rtrim", column);
        }

        /// <summary>
        /// Trim the specified character string from right end for the given string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="trimString">String to trim</param>
        /// <returns>Column object</returns>
        public static Column Rtrim(Column column, string trimString)
        {
            return ApplyFunction("rtrim", column, trimString);
        }

        /// <summary>
        /// Returns the soundex code for the specified expression.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Soundex(Column column)
        {
            return ApplyFunction("soundex", column);
        }

        /// <summary>
        /// Splits string with a regular expression pattern.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="pattern">Regular expression pattern</param>
        /// <returns>Column object</returns>
        public static Column Split(Column column, string pattern)
        {
            return ApplyFunction("split", column, pattern);
        }

        /// <summary>
        /// Splits str around matches of the given pattern.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="pattern">Regular expression pattern</param>
        /// <param name="limit">An integer expression which controls the number of times the regex
        /// is applied.
        /// 1. limit greater than 0: The resulting array's length will not be more than limit, and
        /// the resulting array's last entry will contain all input beyond the last matched regex.
        /// 2. limit less than or equal to 0: `regex` will be applied as many times as possible,
        /// and the resulting array can be of any size.
        /// </param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Split(Column column, string pattern, int limit)
        {
            return ApplyFunction("split", column, pattern, limit);
        }

        /// <summary>
        /// Returns the substring (or slice of byte array) starting from the given
        /// position for the given length.
        /// </summary>
        /// <remarks>
        /// The position is not zero based, but 1 based index.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="pos">Starting position</param>
        /// <param name="len">Length of the substring</param>
        /// <returns>Column object</returns>
        public static Column Substring(Column column, int pos, int len)
        {
            return ApplyFunction("substring", column, pos, len);
        }

        /// <summary>
        /// Returns the substring from the given string before `count` occurrences of
        /// the given delimiter.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="delimiter">Delimiter to find</param>
        /// <param name="count">Number of occurrences of delimiter</param>
        /// <returns>Column object</returns>
        public static Column SubstringIndex(Column column, string delimiter, int count)
        {
            return ApplyFunction("substring_index", column, delimiter, count);
        }

        /// <summary>
        /// Overlay the specified portion of `src` with `replace`, starting from byte position
        /// `pos` of `src` and proceeding for `len` bytes.
        /// </summary>
        /// <param name="src">Source column to replace</param>
        /// <param name="replace">Replacing column</param>
        /// <param name="pos">Byte position to start overlaying from</param>
        /// <param name="len">Number of bytes to overlay</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Overlay(Column src, Column replace, Column pos, Column len)
        {
            return ApplyFunction("overlay", src, replace, pos, len);
        }

        /// <summary>
        /// Overlay the specified portion of `src` with `replace`, starting from byte position
        /// `pos` of `src`.
        /// </summary>
        /// <param name="src">Source column to replace</param>
        /// <param name="replace">Replacing column</param>
        /// <param name="pos">Byte position to start overlaying from</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Overlay(Column src, Column replace, Column pos)
        {
            return ApplyFunction("overlay", src, replace, pos);
        }

        /// <summary>
        /// Splits a string into arrays of sentences, where each sentence is an array of words.
        /// </summary>
        /// <param name="str">String to split</param>
        /// <param name="language">Language of the locale</param>
        /// <param name="country">Country of the locale</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Sentences(Column str, Column language, Column country)
        {
            return ApplyFunction("sentences", str, language, country);
        }

        /// <summary>
        /// Splits a string into arrays of sentences, where each sentence is an array of words.
        /// The default locale is used.
        /// </summary>
        /// <param name="str">String to split</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Sentences(Column str)
        {
            return ApplyFunction("sentences", str);
        }

        /// <summary>
        /// Translate any characters that match with the given `matchingString` in the column
        /// by the given `replaceString`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="matchingString">String to match</param>
        /// <param name="replaceString">String to replace with</param>
        /// <returns>Column object</returns>
        public static Column Translate(Column column, string matchingString, string replaceString)
        {
            return ApplyFunction("translate", column, matchingString, replaceString);
        }

        /// <summary>
        /// Trim the spaces from both ends for the specified string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Trim(Column column)
        {
            return ApplyFunction("trim", column);
        }

        /// <summary>
        /// Trim the specified character from both ends for the specified string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="trimString">String to trim</param>
        /// <returns>Column object</returns>
        public static Column Trim(Column column, string trimString)
        {
            return ApplyFunction("trim", column, trimString);
        }

        /// <summary>
        /// Converts a string column to upper case.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Upper(Column column)
        {
            return ApplyFunction("upper", column);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // DateTime functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Returns the date that is `numMonths` after `startDate`.
        /// </summary>
        /// <param name="startDate">Start date</param>
        /// <param name="numMonths">Number of months to add to start date</param>
        /// <returns>Column object</returns>
        public static Column AddMonths(Column startDate, int numMonths)
        {
            return ApplyFunction("add_months", startDate, numMonths);
        }

        /// <summary>
        /// Returns the date that is `numMonths` after `startDate`.
        /// </summary>
        /// <param name="startDate">Start date</param>
        /// <param name="numMonths">A column of the number of months to add to start date</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column AddMonths(Column startDate, Column numMonths)
        {
            return ApplyFunction("add_months", startDate, numMonths);
        }

        /// <summary>
        /// Returns the current date as a date column.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column CurrentDate()
        {
            return ApplyFunction("current_date");
        }

        /// <summary>
        /// Returns the current timestamp as a timestamp column.
        /// </summary>
        /// <returns>Column object</returns>
        public static Column CurrentTimestamp()
        {
            return ApplyFunction("current_timestamp");
        }

        /// <summary>
        /// Converts a date/timestamp/string to a value of string in the format specified
        /// by the date format given by the second argument.
        /// </summary>
        /// <param name="dateExpr">Date expression</param>
        /// <param name="format">Format string to apply</param>
        /// <returns>Column object</returns>
        public static Column DateFormat(Column dateExpr, string format)
        {
            return ApplyFunction("date_format", dateExpr, format);
        }

        /// <summary>
        /// Returns the date that is `days` days after `start`.
        /// </summary>
        /// <param name="start">Start date</param>
        /// <param name="days">Number of days to add to start data</param>
        /// <returns>Column object</returns>
        public static Column DateAdd(Column start, int days)
        {
            return ApplyFunction("date_add", start, days);
        }

        /// <summary>
        /// Returns the date that is `days` days after `start`.
        /// </summary>
        /// <param name="start">Start date</param>
        /// <param name="days">A column of number of days to add to start data</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column DateAdd(Column start, Column days)
        {
            return ApplyFunction("date_add", start, days);
        }

        /// <summary>
        /// Returns the date that is `days` days before `start`.
        /// </summary>
        /// <param name="start">Start date</param>
        /// <param name="days">Number of days to subtract from start data</param>
        /// <returns>Column object</returns>
        public static Column DateSub(Column start, int days)
        {
            return ApplyFunction("date_sub", start, days);
        }

        /// <summary>
        /// Returns the date that is `days` days before `start`.
        /// </summary>
        /// <param name="start">Start date</param>
        /// <param name="days">A column of number of days to subtract from start data</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column DateSub(Column start, Column days)
        {
            return ApplyFunction("date_sub", start, days);
        }

        /// <summary>
        /// Returns the number of days from `start` to `end`.
        /// </summary>
        /// <param name="start">Start date</param>
        /// <param name="end">End date</param>
        /// <returns>Column object</returns>
        public static Column DateDiff(Column end, Column start)
        {
            return ApplyFunction("datediff", end, start);
        }

        /// <summary>
        /// Extracts the year as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Year(Column column)
        {
            return ApplyFunction("year", column);
        }

        /// <summary>
        /// Extracts the quarter as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Quarter(Column column)
        {
            return ApplyFunction("quarter", column);
        }

        /// <summary>
        /// Extracts the month as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Month(Column column)
        {
            return ApplyFunction("month", column);
        }

        /// <summary>
        /// Extracts the day of the week as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column DayOfWeek(Column column)
        {
            return ApplyFunction("dayofweek", column);
        }

        /// <summary>
        /// Extracts the day of the month as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column DayOfMonth(Column column)
        {
            return ApplyFunction("dayofmonth", column);
        }

        /// <summary>
        /// Extracts the day of the year as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column DayOfYear(Column column)
        {
            return ApplyFunction("dayofyear", column);
        }

        /// <summary>
        /// Extracts the hours as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Hour(Column column)
        {
            return ApplyFunction("hour", column);
        }

        /// <summary>
        /// Returns the last day of the month which the given date belongs to.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column LastDay(Column column)
        {
            return ApplyFunction("last_day", column);
        }

        /// <summary>
        /// Extracts the minutes as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Minute(Column column)
        {
            return ApplyFunction("minute", column);
        }

        /// <summary>
        /// Returns number of months between dates `end` and `stasrt`.
        /// </summary>
        /// <param name="end">Date column</param>
        /// <param name="start">Date column</param>
        /// <returns>Column object</returns>
        public static Column MonthsBetween(Column end, Column start)
        {
            return ApplyFunction("months_between", end, start);
        }

        /// <summary>
        /// Returns number of months between dates `end` and `start`. If `roundOff` is set to true,
        /// the result is rounded off to 8 digits; it is not rounded otherwise.
        /// </summary>
        /// <param name="end">Date column</param>
        /// <param name="start">Date column</param>
        /// <param name="roundOff">To round or not</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column MonthsBetween(Column end, Column start, bool roundOff)
        {
            return ApplyFunction("months_between", end, start, roundOff);
        }

        /// <summary>
        /// Given a date column, returns the first date which is later than the value of
        /// the date column that is on the specified day of the week.
        /// </summary>
        /// <param name="date">Date column</param>
        /// <param name="dayOfWeek">
        /// One of the following (case-insensitive):
        ///   "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
        /// </param>
        /// <returns>Column object</returns>
        public static Column NextDay(Column date, string dayOfWeek)
        {
            return ApplyFunction("next_day", date, dayOfWeek);
        }

        /// <summary>
        /// Given a date column, returns the first date which is later than the value of
        /// the date column that is on the specified day of the week.
        /// </summary>
        /// <param name="date">Date column</param>
        /// <param name="dayOfWeek">
        /// A column of the day of week. One of the following (case-insensitive):
        ///   "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
        /// </param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column NextDay(Column date, Column dayOfWeek)
        {
            return ApplyFunction("next_day", date, dayOfWeek);
        }

        /// <summary>
        /// Extracts the seconds as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Second(Column column)
        {
            return ApplyFunction("second", column);
        }

        /// <summary>
        /// Extracts the week number as an integer from a given date/timestamp/string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column WeekOfYear(Column column)
        {
            return ApplyFunction("weekofyear", column);
        }

        /// <summary>
        /// Converts the number of seconds from UNIX epoch (1970-01-01 00:00:00 UTC) to a string
        /// representing the timestamp of that moment in the current system time zone with
        /// a default format "yyyy-MM-dd HH:mm:ss".
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column FromUnixTime(Column column)
        {
            return ApplyFunction("from_unixtime", column);
        }

        /// <summary>
        /// Converts the number of seconds from UNIX epoch (1970-01-01 00:00:00 UTC) to a string
        /// representing the timestamp of that moment in the current system time zone with
        /// the given format.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="format">Format of the timestamp</param>
        /// <returns>Column object</returns>
        public static Column FromUnixTime(Column column, string format)
        {
            return ApplyFunction("from_unixtime", column, format);
        }

        /// <summary>
        /// Returns the current Unix timestamp (in seconds).
        /// </summary>
        /// <remarks>
        /// All calls of `UnixTimestamp` within the same query return the same value
        /// (i.e. the current timestamp is calculated at the start of query evaluation).
        /// </remarks>
        /// <returns>Column object</returns>
        public static Column UnixTimestamp()
        {
            return ApplyFunction("unix_timestamp");
        }

        /// <summary>
        /// Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
        /// using the default timezone and the default locale.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column UnixTimestamp(Column column)
        {
            return ApplyFunction("unix_timestamp", column);
        }

        /// <summary>
        /// Converts time string with given format to Unix timestamp (in seconds).
        /// </summary>
        /// <remarks>
        /// Supported date format can be found:
        /// http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="format">Date format</param>
        /// <returns>Column object</returns>
        public static Column UnixTimestamp(Column column, string format)
        {
            return ApplyFunction("unix_timestamp", column, format);
        }

        /// <summary>
        /// Convert time string to a Unix timestamp (in seconds) by casting rules to
        /// `TimestampType`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column ToTimestamp(Column column)
        {
            return ApplyFunction("to_timestamp", column);
        }

        /// <summary>
        /// Convert time string to a Unix timestamp (in seconds) with specified format.
        /// </summary>
        /// <remarks>
        /// Supported date format can be found:
        /// http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="format">Date format</param>
        /// <returns>Column object</returns>
        public static Column ToTimestamp(Column column, string format)
        {
            return ApplyFunction("to_timestamp", column, format);
        }

        /// <summary>
        /// Converts the column into `DateType` by casting rules to `DateType`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column ToDate(Column column)
        {
            return ApplyFunction("to_date", column);
        }

        /// <summary>
        /// Converts the column into a `DateType` with a specified format.
        /// </summary>
        /// <remarks>
        /// Supported date format can be found:
        /// http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="format">Date format</param>
        /// <returns>Column object</returns>
        public static Column ToDate(Column column, string format)
        {
            return ApplyFunction("to_date", column, format);
        }

        /// <summary>
        /// Returns date truncated to the unit specified by the format.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="format">
        /// 'year', 'yyyy', 'yy' for truncate by year, or
        /// 'month', 'mon', 'mm' for truncate by month
        /// </param>
        /// <returns>Column object</returns>
        public static Column Trunc(Column column, string format)
        {
            return ApplyFunction("trunc", column, format);
        }

        /// <summary>
        /// Returns timestamp truncated to the unit specified by the format.
        /// </summary>
        /// <param name="format">
        /// 'year', 'yyyy', 'yy' for truncate by year, or
        /// 'month', 'mon', 'mm' for truncate by month, or
        /// 'day', 'dd' for truncate by day, or
        /// 'second', 'minute', 'hour', 'week', 'month', 'quarter'
        /// </param>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column DateTrunc(string format, Column column)
        {
            return ApplyFunction("date_trunc", format, column);
        }

        /// <summary>
        /// Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
        /// and renders that time as a timestamp in the given time zone. For example, 'GMT+1'
        /// would yield '2017-07-14 03:40:00.0'.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 3.0.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="tz">Timezone string</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_0_0)]
        public static Column FromUtcTimestamp(Column column, string tz)
        {
            return ApplyFunction("from_utc_timestamp", column, tz);
        }

        /// <summary>
        /// Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
        /// and renders that time as a timestamp in the given time zone. For example, 'GMT+1'
        /// would yield '2017-07-14 03:40:00.0'.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 3.0.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="tz">Timezone expression</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        [Deprecated(Versions.V3_0_0)]
        public static Column FromUtcTimestamp(Column column, Column tz)
        {
            return ApplyFunction("from_utc_timestamp", column, tz);
        }

        /// <summary>
        /// Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the
        /// given time zone, and renders that time as a timestamp in UTC. For example, 'GMT+1'
        /// would yield '2017-07-14 01:40:00.0'.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 3.0.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="tz">Timezone string</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_0_0)]
        public static Column ToUtcTimestamp(Column column, string tz)
        {
            return ApplyFunction("to_utc_timestamp", column, tz);
        }

        /// <summary>
        /// Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the
        /// given time zone, and renders that time as a timestamp in UTC. For example, 'GMT+1'
        /// would yield '2017-07-14 01:40:00.0'.
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 3.0.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="tz">Timezone expression</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        [Deprecated(Versions.V3_0_0)]
        public static Column ToUtcTimestamp(Column column, Column tz)
        {
            return ApplyFunction("to_utc_timestamp", column, tz);
        }

        /// <summary>
        /// Bucketize rows into one or more time windows given a timestamp column.
        /// </summary>
        /// <remarks>
        /// Refer to org.apache.spark.unsafe.types.CalendarInterval for the duration strings.
        /// </remarks>
        /// <param name="column">The column to use as the timestamp for windowing by time</param>
        /// <param name="windowDuration">A string specifying the width of the window</param>
        /// <param name="slideDuration">
        /// A string specifying the sliding interval of the window
        /// </param>
        /// <param name="startTime">
        /// The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals
        /// </param>
        /// <returns>Column object</returns>
        public static Column Window(
            Column column,
            string windowDuration,
            string slideDuration,
            string startTime)
        {
            return ApplyFunction("window", column, windowDuration, slideDuration, startTime);
        }

        /// <summary>
        /// Bucketize rows into one or more time windows given a timestamp column.
        /// </summary>
        /// <remarks>
        /// Refer to org.apache.spark.unsafe.types.CalendarInterval for the duration strings.
        /// </remarks>
        /// <param name="column">The column to use as the timestamp for windowing by time</param>
        /// <param name="windowDuration">A string specifying the width of the window</param>
        /// <param name="slideDuration">
        /// A string specifying the sliding interval of the window
        /// </param>
        /// <returns>Column object</returns>
        public static Column Window(
            Column column,
            string windowDuration,
            string slideDuration)
        {
            return ApplyFunction("window", column, windowDuration, slideDuration);
        }

        /// <summary>
        /// Generates tumbling time windows given a timestamp specifying column.
        /// </summary>
        /// <param name="column">The column to use as the timestamp for windowing by time</param>
        /// <param name="windowDuration">
        /// A string specifying the width of the window.
        /// Refer to org.apache.spark.unsafe.types.CalendarInterval for the duration strings.
        /// </param>
        /// <returns>Column object</returns>
        public static Column Window(Column column, string windowDuration)
        {
            return ApplyFunction("window", column, windowDuration);
        }

        /// <summary>
        /// Generates session window given a timestamp specifying column.
        /// </summary>
        /// <param name="timeColumn">The column or the expression to use as the timestamp for windowing by 
        /// time</param>
        /// <param name="gapDuration">A string specifying the timeout of the session, e.g. '10 minutes',
        /// '1 second'</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Session_Window(Column timeColumn, string gapDuration)
        {
            return ApplyFunction("session_window", timeColumn, gapDuration);
        }

        /// <summary>
        /// Generates session window given a timestamp specifying column.
        /// </summary>
        /// <param name="timeColumn">The column or the expression to use as the timestamp for windowing by 
        /// time</param>
        /// <param name="gapDuration">A column specifying the timeout of the session. It could be static 
        /// value, e.g. `10 minutes`, `1 second`, or an expression/UDF that specifies gap duration 
        /// dynamically based on the input row.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Session_Window(Column timeColumn, Column gapDuration)
        {
            return ApplyFunction("session_window", timeColumn, gapDuration);
        }

        /// <summary>
        /// Creates timestamp from the number of seconds since UTC epoch.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column TimestampSeconds(Column column)
        {
            return ApplyFunction("timestamp_seconds", column);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // Collection functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Returns null if the array is null, true if the array contains `value`,
        /// and false otherwise.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="value">Value to check for existence</param>
        /// <returns>Column object</returns>
        public static Column ArrayContains(Column column, object value)
        {
            return ApplyFunction("array_contains", column, value);
        }

        /// <summary>
        /// Returns true if `a1` and `a2` have at least one non-null element in common.
        /// If not and both arrays are non-empty and any of them contains a null,
        /// it returns null. It returns false otherwise.
        /// </summary>
        /// <param name="a1">Left side array</param>
        /// <param name="a2">Right side array</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArraysOverlap(Column a1, Column a2)
        {
            return ApplyFunction("arrays_overlap", a1, a2);
        }

        /// <summary>
        /// Returns an array containing all the elements in `column` from index `start`
        /// (or starting from the end if `start` is negative) with the specified `length`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="start">Start position in the array</param>
        /// <param name="length">Length for slicing</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column Slice(Column column, int start, int length)
        {
            return ApplyFunction("slice", column, start, length);
        }

        /// <summary>
        /// Returns an array containing all the elements in `column` from index `start`
        /// (or starting from the end if `start` is negative) with the specified `length`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="start">Start position in the array</param>
        /// <param name="length">Length for slicing</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_1_0)]
        public static Column Slice(Column column, Column start, Column length)
        {
            return ApplyFunction("slice", column, start, length);
        }

        /// <summary>
        /// Concatenates the elements of `column` using the `delimiter`.
        /// Null values are replaced with `nullReplacement`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="delimiter">Delimiter for join</param>
        /// <param name="nullReplacement">String to replace null value</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayJoin(Column column, string delimiter, string nullReplacement)
        {
            return ApplyFunction("array_join", column, delimiter, nullReplacement);
        }

        /// <summary>
        /// Concatenates the elements of `column` using the `delimiter`.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="delimiter">Delimiter for join</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayJoin(Column column, string delimiter)
        {
            return ApplyFunction("array_join", column, delimiter);
        }

        /// <summary>
        /// Concatenates multiple input columns together into a single column.
        /// </summary>
        /// <remarks>
        /// If all inputs are binary, concat returns an output as binary.
        /// Otherwise, it returns as string.
        /// </remarks>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        public static Column Concat(params Column[] columns)
        {
            return ApplyFunction("concat", (object)columns);
        }

        /// <summary>
        /// Locates the position of the first occurrence of the value in the given array as long.
        /// Returns null if either of the arguments are null.
        /// </summary>
        /// <remarks>
        /// The position is not zero based, but 1 based index.
        /// Returns 0 if value could not be found in array.
        /// </remarks>
        /// <param name="column">Column to apply</param>
        /// <param name="value">Value to locate</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayPosition(Column column, object value)
        {
            return ApplyFunction("array_position", column, value);
        }

        /// <summary>
        /// Returns element of array at given index in `value` if column is array.
        /// Returns value for the given key in `value` if column is map.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="value">Value to locate</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ElementAt(Column column, object value)
        {
            return ApplyFunction("element_at", column, value);
        }

        /// <summary>
        /// Sorts the input array in ascending order. The elements of the input array must
        /// be sortable. Null elements will be placed at the end of the returned array.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArraySort(Column column)
        {
            return ApplyFunction("array_sort", column);
        }

        /// <summary>
        /// Remove all elements that equal to element from the given array.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="element">Element to remove</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayRemove(Column column, object element)
        {
            return ApplyFunction("array_remove", column, element);
        }

        /// <summary>
        /// Removes duplicate values from the array.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayDistinct(Column column)
        {
            return ApplyFunction("array_distinct", column);
        }

        /// <summary>
        /// Returns an array of the elements in the intersection of the given two arrays,
        /// without duplicates.
        /// </summary>s
        /// <param name="col1">Left side column to apply</param>
        /// <param name="col2">Right side column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayIntersect(Column col1, Column col2)
        {
            return ApplyFunction("array_intersect", col1, col2);
        }

        /// <summary>
        /// Returns an array of the elements in the union of the given two arrays,
        /// without duplicates.
        /// </summary>
        /// <param name="col1">Left side column to apply</param>
        /// <param name="col2">Right side column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayUnion(Column col1, Column col2)
        {
            return ApplyFunction("array_union", col1, col2);
        }

        /// <summary>
        /// Returns an array of the elements in the `col1` but not in the `col2`,
        /// without duplicates. The order of elements in the result is nondeterministic.
        /// </summary>
        /// <param name="col1">Left side column to apply</param>
        /// <param name="col2">Right side column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayExcept(Column col1, Column col2)
        {
            return ApplyFunction("array_except", col1, col2);
        }

        /// <summary>
        /// Creates a new row for each element in the given array or map column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Explode(Column column)
        {
            return ApplyFunction("explode", column);
        }

        /// <summary>
        /// Creates a new row for each element in the given array or map column.
        /// Unlike Explode(), if the array/map is null or empty then null is produced.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column ExplodeOuter(Column column)
        {
            return ApplyFunction("explode_outer", column);
        }

        /// <summary>
        /// Creates a new row for each element with position in the given array or map column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column PosExplode(Column column)
        {
            return ApplyFunction("posexplode", column);
        }

        /// <summary>
        /// Creates a new row for each element with position in the given array or map column.
        /// Unlike Posexplode(), if the array/map is null or empty then the row(null, null)
        /// is produced.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column PosExplodeOuter(Column column)
        {
            return ApplyFunction("posexplode_outer", column);
        }

        /// <summary>
        /// Extracts JSON object from a JSON string based on path specified, and returns JSON
        /// string of the extracted JSON object.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="path">JSON file path</param>
        /// <returns>Column object</returns>
        public static Column GetJsonObject(Column column, string path)
        {
            return ApplyFunction("get_json_object", column, path);
        }

        /// <summary>
        /// Creates a new row for a JSON column according to the given field names.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="fields">Field names</param>
        /// <returns>Column object</returns>
        public static Column JsonTuple(Column column, params string[] fields)
        {
            return ApplyFunction("json_tuple", column, fields);
        }

        /// <summary>
        /// Parses a column containing a JSON string into a `StructType` or `ArrayType`
        /// of `StructType`s with the specified schema.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="schema">JSON format string or DDL-formatted string for a schema</param>
        /// <param name="options">Options for JSON parsing</param>
        /// <returns>Column object</returns>
        public static Column FromJson(
            Column column,
            string schema,
            Dictionary<string, string> options = null)
        {
            return ApplyFunction(
                "from_json",
                column,
                schema,
                options ?? new Dictionary<string, string>());
        }

        /// <summary>
        /// Parses a column containing a JSON string into a `StructType` or `ArrayType`
        /// of `StructType`s with the specified schema.
        /// </summary>
        /// <param name="column">String column containing JSON data</param>
        /// <param name="schema">The schema to use when parsing the JSON string</param>
        /// <param name="options">Options for JSON parsing</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column FromJson(
            Column column,
            Column schema,
            Dictionary<string, string> options = null)
        {
            return ApplyFunction(
                "from_json",
                column,
                schema,
                options ?? new Dictionary<string, string>());
        }

        /// <summary>
        /// Parses a JSON string and infers its schema in DDL format.
        /// </summary>
        /// <param name="json">JSON string</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column SchemaOfJson(string json)
        {
            return ApplyFunction("schema_of_json", json);
        }

        /// <summary>
        /// Parses a JSON string and infers its schema in DDL format.
        /// </summary>
        /// <param name="json">String literal containing a JSON string.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column SchemaOfJson(Column json)
        {
            return ApplyFunction("schema_of_json", json);
        }

        /// <summary>
        /// Parses a JSON string and infers its schema in DDL format.
        /// </summary>
        /// <param name="json">String literal containing a JSON string.</param>
        /// <param name="options">Options to control how the json is parsed.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column SchemaOfJson(Column json, Dictionary<string, string> options)
        {
            return ApplyFunction("schema_of_json", json, options);
        }

        /// <summary>
        /// Converts a column containing a `StructType`, `ArrayType` of `StructType`s,
        /// a `MapType` or `ArrayType` of `MapType`s into a JSON string.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="options">Options for JSON conversion</param>
        /// <returns>Column object</returns>
        public static Column ToJson(
            Column column,
            Dictionary<string, string> options = null)
        {
            return ApplyFunction(
                "to_json",
                column,
                options ?? new Dictionary<string, string>());
        }

        /// <summary>
        /// Returns length of array or map.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Size(Column column)
        {
            return ApplyFunction("size", column);
        }

        /// <summary>
        /// Sorts the input array for the given column in ascending (default) or
        /// descending order, the natural ordering of the array elements.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="asc">True for ascending order and false for descending order</param>
        /// <returns>Column object</returns>
        public static Column SortArray(Column column, bool asc = true)
        {
            return ApplyFunction("sort_array", column, asc);
        }

        /// <summary>
        /// Returns the minimum value in the array.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayMin(Column column)
        {
            return ApplyFunction("array_min", column);
        }

        /// <summary>
        /// Returns the maximum value in the array.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayMax(Column column)
        {
            return ApplyFunction("array_max", column);
        }

        /// <summary>
        /// Returns a random permutation of the given array.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column Shuffle(Column column)
        {
            return ApplyFunction("shuffle", column);
        }

        /// <summary>
        /// Reverses the string column and returns it as a new string column.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column Reverse(Column column)
        {
            return ApplyFunction("reverse", column);
        }

        /// <summary>
        /// Creates a single array from an array of arrays. If a structure of nested arrays
        /// is deeper than two levels, only one level of nesting is removed.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column Flatten(Column column)
        {
            return ApplyFunction("flatten", column);
        }

        /// <summary>
        /// Generate a sequence of integers from `start` to `stop`, incrementing by `step`.
        /// </summary>
        /// <param name="start">Start expression</param>
        /// <param name="stop">Stop expression</param>
        /// <param name="step">Step to increment</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column Sequence(Column start, Column stop, Column step)
        {
            return ApplyFunction("sequence", start, stop, step);
        }

        /// <summary>
        /// Generate a sequence of integers from start to stop, incrementing by 1 if start is
        /// less than or equal to stop, otherwise -1.
        /// </summary>
        /// <param name="start">Start expression</param>
        /// <param name="stop">Stop expression</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column Sequence(Column start, Column stop)
        {
            return ApplyFunction("sequence", start, stop);
        }

        /// <summary>
        /// Creates an array containing the `left` argument repeated the number of times given by
        /// the `right` argument.
        /// </summary>
        /// <param name="left">Left column expression</param>
        /// <param name="right">Right column expression</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayRepeat(Column left, Column right)
        {
            return ApplyFunction("array_repeat", left, right);
        }

        /// <summary>
        /// Creates an array containing the `left` argument repeated the `count` number of times.
        /// </summary>
        /// <param name="left">Left column expression</param>
        /// <param name="count">Number of times to repeat</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArrayRepeat(Column left, int count)
        {
            return ApplyFunction("array_repeat", left, count);
        }

        /// <summary>
        /// Returns an unordered array containing the keys of the map.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column MapKeys(Column column)
        {
            return ApplyFunction("map_keys", column);
        }

        /// <summary>
        /// Returns an unordered array containing the values of the map.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        public static Column MapValues(Column column)
        {
            return ApplyFunction("map_values", column);
        }

        /// <summary>
        /// Returns an unordered array of all entries in the given map.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column MapEntries(Column column)
        {
            return ApplyFunction("map_entries", column);
        }

        /// <summary>
        /// Returns a map created from the given array of entries.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column MapFromEntries(Column column)
        {
            return ApplyFunction("map_from_entries", column);
        }

        /// <summary>
        /// Returns a merged array of structs in which the N-th struct contains all
        /// N-th values of input arrays.
        /// </summary>
        /// <param name="columns">Columns to zip</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ArraysZip(params Column[] columns)
        {
            return ApplyFunction("arrays_zip", (object)columns);
        }

        /// <summary>
        /// Returns the union of all the given maps.
        /// </summary>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column MapConcat(params Column[] columns)
        {
            return ApplyFunction("map_concat", (object)columns);
        }

        /// <summary>
        /// Parses a column containing a CSV string into a `StructType` with the specified schema.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="schema">The schema to use when parsing the CSV string</param>
        /// <param name="options">Options to control how the CSV is parsed.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column FromCsv(
            Column column,
            StructType schema,
            Dictionary<string, string> options)
        {
            return ApplyFunction(
                "from_csv",
                column,
                DataType.FromJson(Jvm, schema.Json),
                options);
        }

        /// <summary>
        /// Parses a column containing a CSV string into a `StructType` with the specified schema.
        /// </summary>
        /// <param name="column">Column to apply</param>
        /// <param name="schema">The schema to use when parsing the CSV string</param>
        /// <param name="options">Options to control how the CSV is parsed.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column FromCsv(
            Column column,
            Column schema,
            Dictionary<string, string> options)
        {
            return ApplyFunction("from_csv", column, schema, options);
        }

        /// <summary>
        /// Parses a CSV string and infers its schema in DDL format.
        /// </summary>
        /// <param name="csv">CSV string to parse</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column SchemaOfCsv(string csv)
        {
            return ApplyFunction("schema_of_csv", csv);
        }

        /// <summary>
        /// Parses a CSV string and infers its schema in DDL format.
        /// </summary>
        /// <param name="csv">CSV string to parse</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column SchemaOfCsv(Column csv)
        {
            return ApplyFunction("schema_of_csv", csv);
        }

        /// <summary>
        /// Parses a CSV string and infers its schema in DDL format.
        /// </summary>
        /// <param name="csv">CSV string to parse</param>
        /// <param name="options">Options to control how the CSV is parsed.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column SchemaOfCsv(Column csv, Dictionary<string, string> options)
        {
            return ApplyFunction("schema_of_csv", csv, options);
        }

        /// <summary>
        /// Converts a column containing a `StructType` into a CSV string with the specified
        /// schema.
        /// </summary>
        /// <param name="column">A column containing a struct.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column ToCsv(Column column)
        {
            return ApplyFunction("to_csv", column);
        }

        /// <summary>
        /// Converts a column containing a `StructType` into a CSV string with the specified
        /// schema.
        /// </summary>
        /// <param name="column">A column containing a struct.</param>
        /// <param name="options">Options to control how the struct column is converted into a CSV
        /// string</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column ToCsv(Column column, Dictionary<string, string> options)
        {
            return ApplyFunction("to_csv", column, options);
        }

        /// <summary>
        /// A transform for timestamps and dates to partition data into years.
        /// </summary>
        /// <param name="column">A column containing a struct.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Years(Column column)
        {
            return ApplyFunction("years", column);
        }

        /// <summary>
        /// A transform for timestamps and dates to partition data into months.
        /// </summary>
        /// <param name="column">A column containing a struct.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Months(Column column)
        {
            return ApplyFunction("months", column);
        }

        /// <summary>
        /// A transform for timestamps and dates to partition data into days.
        /// </summary>
        /// <param name="column">A column containing a struct.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Days(Column column)
        {
            return ApplyFunction("days", column);
        }

        /// <summary>
        /// A transform for timestamps to partition data into hours.
        /// </summary>
        /// <param name="column">A column containing a struct.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Hours(Column column)
        {
            return ApplyFunction("hours", column);
        }

        /// <summary>
        /// A transform for any type that partitions by a hash of the input column.
        /// </summary>
        /// <param name="numBuckets">A column containing number of buckets</param>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Bucket(Column numBuckets, Column column)
        {
            return ApplyFunction("bucket", numBuckets, column);
        }

        /// <summary>
        /// A transform for any type that partitions by a hash of the input column.
        /// </summary>
        /// <param name="numBuckets">Number of buckets</param>
        /// <param name="column">Column to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column Bucket(int numBuckets, Column column)
        {
            return ApplyFunction("bucket", numBuckets, column);
        }

        /////////////////////////////////////////////////////////////////////////////////
        // UDF helper functions
        /////////////////////////////////////////////////////////////////////////////////

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column> Udf<TResult>(Func<TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply0;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column> Udf<T, TResult>(Func<T, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply1;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column> Udf<T1, T2, TResult>(Func<T1, T2, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply2;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column> Udf<T1, T2, T3, TResult>(
            Func<T1, T2, T3, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply3;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, TResult>(
            Func<T1, T2, T3, T4, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply4;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, TResult>(
            Func<T1, T2, T3, T4, T5, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply5;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, TResult>(
            Func<T1, T2, T3, T4, T5, T6, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply6;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply7;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply8;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>A delegate that when invoked will return a <see cref="Column"/> for the result of the UDF.</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply9;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="T10">Specifies the type of the tenth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> udf)
        {
            return CreateUdf<TResult>(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf)).Apply10;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column> Udf(Func<Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply0;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column> Udf<T>(Func<T, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply1;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column> Udf<T1, T2>(
            Func<T1, T2, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply2;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column> Udf<T1, T2, T3>(
            Func<T1, T2, T3, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply3;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4>(
            Func<T1, T2, T3, T4, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply4;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5>(
            Func<T1, T2, T3, T4, T5, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply5;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6>(
            Func<T1, T2, T3, T4, T5, T6, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply6;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7>(
            Func<T1, T2, T3, T4, T5, T6, T7, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply7;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, T8>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply8;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>A delegate that when invoked will return a <see cref="Column"/> for the result of the UDF.</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply9;
        }

        /// <summary>Creates a UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="T10">Specifies the type of the tenth argument to the UDF.</typeparam>
        /// <param name="udf">The UDF function implementation.</param>
        /// <param name="returnType">Schema associated with this row</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, Row> udf, StructType returnType)
        {
            return CreateUdf(udf.Method.ToString(), UdfUtils.CreateUdfWrapper(udf), returnType).Apply10;
        }

        /// <summary>
        /// Call a user-defined function registered via SparkSession.Udf().Register().
        /// </summary>
        /// <param name="udfName">Name of the registered UDF</param>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        [Deprecated(Versions.V3_2_0)]
        public static Column CallUDF(string udfName, params Column[] columns)
        {
            return ApplyFunction("callUDF", udfName, columns);
        }

        /// <summary>
        /// Call a user-defined function registered via SparkSession.Udf().Register().
        /// </summary>
        /// <param name="udfName">Name of the registered UDF</param>
        /// <param name="columns">Columns to apply</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_2_0)]
        public static Column Call_UDF(string udfName, params Column[] columns)
        {
            return ApplyFunction("call_udf", udfName, columns);
        }

        private static UserDefinedFunction CreateUdf<TResult>(string name, Delegate execute)
        {
            return CreateUdf<TResult>(name, execute, UdfUtils.PythonEvalType.SQL_BATCHED_UDF);
        }

        private static UserDefinedFunction CreateUdf(string name, Delegate execute, StructType returnType)
        {
            return CreateUdf(name, execute, UdfUtils.PythonEvalType.SQL_BATCHED_UDF, returnType);
        }

        internal static UserDefinedFunction CreateVectorUdf<TResult>(string name, Delegate execute)
        {
            return CreateUdf<TResult>(name, execute, UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF);
        }

        private static UserDefinedFunction CreateUdf<TResult>(
            string name,
            Delegate execute,
            UdfUtils.PythonEvalType evalType) =>
            CreateUdf(name, execute, evalType, UdfUtils.GetReturnType(typeof(TResult)));

        private static UserDefinedFunction CreateUdf(
            string name,
            Delegate execute,
            UdfUtils.PythonEvalType evalType,
            StructType returnType) =>
            CreateUdf(name, execute, evalType, returnType.Json);

        private static UserDefinedFunction CreateUdf(
            string name,
            Delegate execute,
            UdfUtils.PythonEvalType evalType,
            string returnType)
        {
            return UserDefinedFunction.Create(
                name,
                CommandSerDe.Serialize(
                    execute,
                    CommandSerDe.SerializedMode.Row,
                    CommandSerDe.SerializedMode.Row),
                evalType,
                returnType);
        }

        private static Column ApplyFunction(string funcName)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    funcName));
        }

        private static Column ApplyFunction(string funcName, object arg)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    funcName,
                    arg));
        }

        private static Column ApplyFunction(string funcName, object arg1, object arg2)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    funcName,
                    arg1,
                    arg2));
        }

        private static Column ApplyFunction(string funcName, params object[] args)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    funcName,
                    args));
        }
    }
}
