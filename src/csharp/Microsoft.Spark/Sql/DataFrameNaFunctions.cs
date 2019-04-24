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
        private readonly JvmObjectReference _jvmObject;

        internal DataFrameNaFunctions(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing any null or NaN values.
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop() => WrapAsDataFrame(_jvmObject.Invoke("drop"));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing null or NaN values.
        /// </summary>
        /// <remarks>
        /// If `how` is "any", then drop rows containing any null or NaN values.
        /// If `how` is "all", then drop rows only if every column is null or NaN for that row.
        /// </remarks>
        /// <param name="how">Determines the behavior of dropping rows</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(string how) => WrapAsDataFrame(_jvmObject.Invoke("drop", how));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing any null or NaN values
        /// in the specified columns.
        /// </summary>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(IEnumerable<string> columnNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("drop", columnNames));

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
            WrapAsDataFrame(_jvmObject.Invoke("drop", how, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing less than `minNonNulls`
        /// non-null and non-NaN values.
        /// </summary>
        /// <param name="minNonNulls"></param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(int minNonNulls) =>
            WrapAsDataFrame(_jvmObject.Invoke("drop", minNonNulls));

        /// <summary>
        /// Returns a new `DataFrame` that drops rows containing less than `minNonNulls`
        /// non-null and non-NaN values in the specified columns.
        /// </summary>
        /// <param name="minNonNulls"></param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Drop(int minNonNulls, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("drop", minNonNulls, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in numeric columns
        /// with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(long value) => WrapAsDataFrame(_jvmObject.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in numeric columns
        /// with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(double value) => WrapAsDataFrame(_jvmObject.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in numeric columns
        /// with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(string value) => WrapAsDataFrame(_jvmObject.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified numeric
        /// columns. If a specified column is not a numeric column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(long value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified numeric
        /// columns. If a specified column is not a numeric column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(double value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified string
        /// columns. If a specified column is not a string column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(string value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("fill", value, columnNames));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null values in boolean columns with `value`.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(bool value) => WrapAsDataFrame(_jvmObject.Invoke("fill", value));

        /// <summary>
        /// Returns a new `DataFrame` that replaces null or NaN values in specified boolean
        /// columns. If a specified column is not a boolean column, it is ignored.
        /// </summary>
        /// <param name="value">Value to replace with</param>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Fill(bool value, IEnumerable<string> columnNames) =>
            WrapAsDataFrame(_jvmObject.Invoke("fill", value, columnNames));

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
            WrapAsDataFrame(_jvmObject.Invoke("fill", valueMap));

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
            WrapAsDataFrame(_jvmObject.Invoke("fill", valueMap));

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
            WrapAsDataFrame(_jvmObject.Invoke("fill", valueMap));

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
            WrapAsDataFrame(_jvmObject.Invoke("fill", valueMap));

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
            WrapAsDataFrame(_jvmObject.Invoke("fill", valueMap));

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
            WrapAsDataFrame(_jvmObject.Invoke("replace", columnName, replacement));

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
            WrapAsDataFrame(_jvmObject.Invoke("replace", columnName, replacement));

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
            WrapAsDataFrame(_jvmObject.Invoke("replace", columnName, replacement));

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
            WrapAsDataFrame(_jvmObject.Invoke("replace", columnNames, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnNames">list of columns to apply the value replacement.</param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(
            IEnumerable<string> columnNames,
            IDictionary<bool, bool> replacement) =>
            WrapAsDataFrame(_jvmObject.Invoke("replace", columnNames, replacement));

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// </summary>
        /// <param name="columnNames">list of columns to apply the value replacement.</param>
        /// <param name="replacement">Map that stores the replacement values</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Replace(
            IEnumerable<string> columnNames,
            IDictionary<string, string> replacement) =>
            WrapAsDataFrame(_jvmObject.Invoke("replace", columnNames, replacement));

        private DataFrame WrapAsDataFrame(object obj) => new DataFrame((JvmObjectReference)obj);
    }
}
