// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Provides functionalities for working with missing data in <see cref="DataFrame"/>.
    /// </summary>
    public sealed class DataFrameNaFunctions : IJvmObjectReferenceProvider
    {
        internal DataFrameNaFunctions(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing any null or NaN values.
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop() => WrapAsDataFrame(Reference.Invoke("drop"));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing null or NaN values.
        /// </summary>
        /// <remarks>
        /// If `how` is "any", then drop rows containing any null or NaN values.
        /// If `how` is "all", then drop rows only if every column is null or NaN for that row.
        /// </remarks>
        /// <param name="how">Determines the behavior of dropping rows</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(string how) => WrapAsDataFrame(Reference.Invoke("drop", how));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing any null or NaN values
        /// in the specified columns.
        /// </summary>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("drop", columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing any null or NaN values
        /// in the specified columns.
        /// </summary>
        /// <remarks>
        /// If `how` is "any", then drop rows containing any null or NaN values.
        /// If `how` is "all", then drop rows only if every column is null or NaN for that row.
        /// </remarks>
        /// <param name="how">Determines the behavior of dropping rows</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(string how, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("drop", how, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing less than `minNonNulls`
        /// non-null and non-NaN values.
        /// </summary>
        /// <param name="minNonNulls"></param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(int minNonNulls) =>
            WrapAsDataFrame(Reference.Invoke("drop", minNonNulls));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing less than `minNonNulls`
        /// non-null and non-NaN values in the specified columns.
        /// </summary>
        /// <param name="minNonNulls"></param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(int minNonNulls, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("drop", minNonNulls, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in numeric columns
        /// with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(long value) => WrapAsDataFrame(Reference.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in numeric columns
        /// with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(double value) => WrapAsDataFrame(Reference.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in numeric columns
        /// with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(string value) => WrapAsDataFrame(Reference.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified numeric
        /// columns. If a specified column is not a numeric column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(long value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified numeric
        /// columns. If a specified column is not a numeric column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(double value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified string
        /// columns. If a specified column is not a string column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(string value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values in boolean columns with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(bool value) => WrapAsDataFrame(Reference.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified boolean
        /// columns. If a specified column is not a boolean column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(bool value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values.
        /// </summary>
        /// <remarks>
        /// The key of the map is the column name, and the value of the map is the
        /// replacement value.
        /// </remarks>
        /// <param name="valueMap">Values to replace null values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(IDictionary<string, int> valueMap) =>
            WrapAsDataFrame(Reference.Invoke("fill", valueMap));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values.
        /// </summary>
        /// <remarks>
        /// The key of the map is the column name, and the value of the map is the
        /// replacement value.
        /// </remarks>
        /// <param name="valueMap">Values to replace null values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(IDictionary<string, long> valueMap) =>
            WrapAsDataFrame(Reference.Invoke("fill", valueMap));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values.
        /// </summary>
        /// <remarks>
        /// The key of the map is the column name, and the value of the map is the
        /// replacement value.
        /// </remarks>
        /// <param name="valueMap">Values to replace null values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(IDictionary<string, double> valueMap) =>
            WrapAsDataFrame(Reference.Invoke("fill", valueMap));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values.
        /// </summary>
        /// <remarks>
        /// The key of the map is the column name, and the value of the map is the
        /// replacement value.
        /// </remarks>
        /// <param name="valueMap">Values to replace null values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(IDictionary<string, string> valueMap) =>
            WrapAsDataFrame(Reference.Invoke("fill", valueMap));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values.
        /// </summary>
        /// <remarks>
        /// The key of the map is the column name, and the value of the map is the
        /// replacement value.
        /// </remarks>
        /// <param name="valueMap">Values to replace null values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(IDictionary<string, bool> valueMap) =>
            WrapAsDataFrame(Reference.Invoke("fill", valueMap));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnName">
        /// Name of the column to apply the value replacement. If `col` is "*", replacement
        /// is applied on all string, numeric or boolean columns.
        /// </param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(string columnName, IDictionary<double, double> replacement) =>
            WrapAsDataFrame(Reference.Invoke("replace", columnName, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnName">
        /// Name of the column to apply the value replacement. If `col` is "*", replacement
        /// is applied on all string, numeric or boolean columns.
        /// </param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(string columnName, IDictionary<bool, bool> replacement) =>
            WrapAsDataFrame(Reference.Invoke("replace", columnName, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnName">
        /// Name of the column to apply the value replacement. If `col` is "*", replacement
        /// is applied on all string, numeric or boolean columns.
        /// </param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(string columnName, IDictionary<string, string> replacement) =>
            WrapAsDataFrame(Reference.Invoke("replace", columnName, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnNames">
        /// Name of the column to apply the value replacement. If `col` is "*", replacement
        /// is applied on all string, numeric or boolean columns.
        /// </param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(
            IEnumerable<string> columnNames,
            IDictionary<double, double> replacement) =>
            WrapAsDataFrame(Reference.Invoke("replace", columnNames, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnNames">list of columns to apply the value replacement.</param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(
            IEnumerable<string> columnNames,
            IDictionary<bool, bool> replacement) =>
            WrapAsDataFrame(Reference.Invoke("replace", columnNames, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnNames">list of columns to apply the value replacement.</param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(
            IEnumerable<string> columnNames,
            IDictionary<string, string> replacement) =>
            WrapAsDataFrame(Reference.Invoke("replace", columnNames, replacement));

        private DataFrame WrapAsDataFrame(object obj) => new DataFrame((JvmObjectReference)obj);
    }
}
