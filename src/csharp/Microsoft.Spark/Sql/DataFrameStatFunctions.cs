// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Provides statistic functions for <see cref="DataFrame"/>.
    /// </summary>
    public sealed class DataFrameStatFunctions : IJvmObjectReferenceProvider
    {
        internal DataFrameStatFunctions(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Calculates the approximate quantiles of a numerical column of a DataFrame.
        /// </summary>
        /// <remarks>
        /// This method implements a variation of the Greenwald-Khanna algorithm
        /// (with some speed optimizations).
        /// </remarks>
        /// <param name="columnName">Column name</param>
        /// <param name="probabilities">A list of quantile probabilities</param>
        /// <param name="relativeError">
        /// The relative target precision to achieve (greater than or equal to 0)
        /// </param>
        /// <returns>The approximate quantiles at the given probabilities</returns>
        public double[] ApproxQuantile(
            string columnName,
            IEnumerable<double> probabilities,
            double relativeError) =>
            (double[])Reference.Invoke(
                "approxQuantile", columnName, probabilities, relativeError);

        /// <summary>
        /// Calculate the sample covariance of two numerical columns of a DataFrame.
        /// </summary>
        /// <param name="colName1">First column name</param>
        /// <param name="colName2">Second column name</param>
        /// <returns>The covariance of the two columns</returns>
        public double Cov(string colName1, string colName2) =>
            (double)Reference.Invoke("cov", colName1, colName2);

        /// <summary>
        /// Calculates the correlation of two columns of a DataFrame.
        /// </summary>
        /// <remarks>
        /// Currently only the Pearson Correlation Coefficient is supported.
        /// </remarks>
        /// <param name="colName1">First column name</param>
        /// <param name="colName2">Second column name</param>
        /// <param name="method">Method name for calculating correlation</param>
        /// <returns>The Pearson Correlation Coefficient</returns>
        public double Corr(string colName1, string colName2, string method) =>
            (double)Reference.Invoke("corr", colName1, colName2, method);

        /// <summary>
        /// Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
        /// </summary>
        /// <param name="colName1">First column name</param>
        /// <param name="colName2">Second column name</param>
        /// <returns>The Pearson Correlation Coefficient</returns>
        public double Corr(string colName1, string colName2) =>
            (double)Reference.Invoke("corr", colName1, colName2);

        /// <summary>
        /// Computes a pair-wise frequency table of the given columns, also known as 
        /// a contingency table.
        /// </summary>
        /// <param name="colName1">First column name</param>
        /// <param name="colName2">Second column name</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Crosstab(string colName1, string colName2) =>
            WrapAsDataFrame(Reference.Invoke("crosstab", colName1, colName2));

        /// <summary>
        /// Finding frequent items for columns, possibly with false positives.
        /// </summary>
        /// <param name="columnNames">Column names</param>
        /// <param name="support">
        /// The minimum frequency for an item to be considered frequent.
        /// Should be greater than 1e-4.
        /// </param>
        /// <returns>DataFrame object</returns>
        public DataFrame FreqItems(IEnumerable<string> columnNames, double support) =>
            WrapAsDataFrame(Reference.Invoke("freqItems", columnNames, support));

        /// <summary>
        /// Finding frequent items for columns, possibly with false positives with
        /// a default support of 1%.
        /// </summary>
        /// <param name="columnNames">Column names</param>
        /// <returns>DataFrame object</returns>
        public DataFrame FreqItems(IEnumerable<string> columnNames) =>
            WrapAsDataFrame(Reference.Invoke("freqItems", columnNames));

        /// <summary>
        /// Returns a stratified sample without replacement based on the fraction given
        /// on each stratum.
        /// </summary>
        /// <typeparam name="T">Stratum type</typeparam>
        /// <param name="columnName">Column name that defines strata</param>
        /// <param name="fractions">
        /// Sampling fraction for each stratum. If a stratum is not specified, we treat
        /// its fraction as zero.
        /// </param>
        /// <param name="seed">Random seed</param>
        /// <returns>DataFrame object</returns>
        public DataFrame SampleBy<T>(
            string columnName,
            IDictionary<T, double> fractions,
            long seed) =>
            WrapAsDataFrame(Reference.Invoke("sampleBy", columnName, fractions, seed));

        /// <summary>
        /// Returns a stratified sample without replacement based on the fraction given
        /// on each stratum.
        /// </summary>
        /// <typeparam name="T">Stratum type</typeparam>
        /// <param name="column">Column that defines strata</param>
        /// <param name="fractions">
        /// Sampling fraction for each stratum. If a stratum is not specified, we treat
        /// its fraction as zero.
        /// </param>
        /// <param name="seed">Random seed</param>
        /// <returns>DataFrame object</returns>
        [Since(Versions.V3_0_0)]
        public DataFrame SampleBy<T>(Column column, IDictionary<T, double> fractions, long seed) =>
            WrapAsDataFrame(Reference.Invoke("sampleBy", column, fractions, seed));

        private DataFrame WrapAsDataFrame(object obj) => new DataFrame((JvmObjectReference)obj);
    }
}
