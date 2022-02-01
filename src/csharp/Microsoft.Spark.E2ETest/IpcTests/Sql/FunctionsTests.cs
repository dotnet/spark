// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class FunctionsTests
    {
        private readonly SparkSession _spark;

        public FunctionsTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            //////////////////////////////
            // Basic Functions
            //////////////////////////////

            Column col = Column("col1");
            Assert.IsType<Column>(col);

            Assert.IsType<Column>(Col("col2"));
            Assert.IsType<Column>(Lit(1));
            Assert.IsType<Column>(Lit("some column"));
            Assert.IsType<Column>(Lit(col));

            //////////////////////////////
            // Sort Functions
            //////////////////////////////
            Assert.IsType<Column>(Asc("col"));
            Assert.IsType<Column>(AscNullsFirst("col"));
            Assert.IsType<Column>(AscNullsLast("col"));
            Assert.IsType<Column>(Desc("col"));
            Assert.IsType<Column>(DescNullsFirst("col"));
            Assert.IsType<Column>(DescNullsLast("col"));

            //////////////////////////////
            // Aggregate Functions
            //////////////////////////////
            Assert.IsType<Column>(Column("col"));

            Assert.IsType<Column>(ApproxCountDistinct(col));
            Assert.IsType<Column>(ApproxCountDistinct("col"));
            Assert.IsType<Column>(ApproxCountDistinct(col, 0.05));
            Assert.IsType<Column>(ApproxCountDistinct("col", 0.05));

            Assert.IsType<Column>(Avg(col));
            Assert.IsType<Column>(Avg("col"));

            Assert.IsType<Column>(CollectList(col));
            Assert.IsType<Column>(CollectList("col"));

            Assert.IsType<Column>(CollectSet(col));
            Assert.IsType<Column>(CollectSet("col"));

            Assert.IsType<Column>(Corr(col, col));
            Assert.IsType<Column>(Corr("col1", "col2"));

            Assert.IsType<Column>(Count(col));
            Assert.IsType<Column>(Count("col"));

            Assert.IsType<Column>(CountDistinct(col));
            Assert.IsType<Column>(CountDistinct(col, col));
            Assert.IsType<Column>(CountDistinct(col, col, col));
            Assert.IsType<Column>(CountDistinct("col1"));
            Assert.IsType<Column>(CountDistinct("col1", "col2"));
            Assert.IsType<Column>(CountDistinct("col1", "col2", "col3"));

            Assert.IsType<Column>(CovarPop(col, col));
            Assert.IsType<Column>(CovarPop("col1", "col2"));

            Assert.IsType<Column>(CovarSamp(col, col));
            Assert.IsType<Column>(CovarSamp("col1", "col2"));

            Assert.IsType<Column>(First(col));
            Assert.IsType<Column>(First(col, true));
            Assert.IsType<Column>(First(col, false));
            Assert.IsType<Column>(First("col"));
            Assert.IsType<Column>(First("col", true));
            Assert.IsType<Column>(First("col", false));

            Assert.IsType<Column>(Grouping(col));
            Assert.IsType<Column>(Grouping("col"));

            Assert.IsType<Column>(GroupingId());
            Assert.IsType<Column>(GroupingId(col));
            Assert.IsType<Column>(GroupingId(col, col));
            Assert.IsType<Column>(GroupingId("col1"));
            Assert.IsType<Column>(GroupingId("col1", "col2"));
            Assert.IsType<Column>(GroupingId("col1", "col2", "col3"));

            Assert.IsType<Column>(Kurtosis(col));
            Assert.IsType<Column>(Kurtosis("col"));

            Assert.IsType<Column>(Last(col));
            Assert.IsType<Column>(Last(col, true));
            Assert.IsType<Column>(Last(col, false));
            Assert.IsType<Column>(Last("col"));
            Assert.IsType<Column>(Last("col", true));
            Assert.IsType<Column>(Last("col", false));

            Assert.IsType<Column>(Max(col));
            Assert.IsType<Column>(Max("col"));

            Assert.IsType<Column>(Mean(col));
            Assert.IsType<Column>(Mean("col"));

            Assert.IsType<Column>(Min(col));
            Assert.IsType<Column>(Min("col"));

            Assert.IsType<Column>(Skewness(col));
            Assert.IsType<Column>(Skewness("col"));

            Assert.IsType<Column>(Stddev(col));
            Assert.IsType<Column>(Stddev("col"));

            Assert.IsType<Column>(StddevSamp(col));
            Assert.IsType<Column>(StddevSamp("col"));

            Assert.IsType<Column>(StddevPop(col));
            Assert.IsType<Column>(StddevPop("col"));

            Assert.IsType<Column>(Sum(col));
            Assert.IsType<Column>(Sum("col"));

            Assert.IsType<Column>(SumDistinct(col));
            Assert.IsType<Column>(SumDistinct("col"));

            Assert.IsType<Column>(Variance(col));
            Assert.IsType<Column>(Variance("col"));

            Assert.IsType<Column>(VarSamp(col));
            Assert.IsType<Column>(VarSamp("col"));

            Assert.IsType<Column>(VarPop(col));
            Assert.IsType<Column>(VarPop("col"));

            //////////////////////////////
            // Window Functions
            //////////////////////////////
            if (SparkSettings.Version < new Version(Versions.V3_0_0))
            {
                // The following APIs are removed in Spark 3.0.
                Assert.IsType<Column>(UnboundedPreceding());

                Assert.IsType<Column>(UnboundedFollowing());

                Assert.IsType<Column>(CurrentRow());
            }

            Assert.IsType<Column>(CumeDist());

            Assert.IsType<Column>(DenseRank());

            Assert.IsType<Column>(Lag(col, 0));
            Assert.IsType<Column>(Lag(col, 2, "col2"));
            Assert.IsType<Column>(Lag("col", 0));
            Assert.IsType<Column>(Lag("col", 2, "col2"));

            Assert.IsType<Column>(Lead(col, 0));
            Assert.IsType<Column>(Lead(col, 2, "col2"));
            Assert.IsType<Column>(Lead("col", 0));
            Assert.IsType<Column>(Lead("col", 2, "col2"));

            Assert.IsType<Column>(Ntile(100));

            Assert.IsType<Column>(PercentRank());

            Assert.IsType<Column>(Rank());

            Assert.IsType<Column>(RowNumber());

            //////////////////////////////
            // Non-Aggregate Functions
            //////////////////////////////
            Assert.IsType<Column>(Column("col"));

            Assert.IsType<Column>(Abs(col));

            Assert.IsType<Column>(Array());
            Assert.IsType<Column>(Array(col));
            Assert.IsType<Column>(Array(col, col));
            Assert.IsType<Column>(Array("col1"));
            Assert.IsType<Column>(Array("col1", "col2"));
            Assert.IsType<Column>(Array("col1", "col2", "col3"));

            Assert.IsType<Column>(Map());
            Assert.IsType<Column>(Map(col));
            Assert.IsType<Column>(Map(col, col));

            Assert.IsType<Column>(MapFromArrays(col, col));

            DataFrame df = _spark
                .Read()
                .Json($"{TestEnvironment.ResourceDirectory}people.json");

            Assert.IsType<DataFrame>(Broadcast(df));

            Assert.IsType<Column>(Coalesce());
            Assert.IsType<Column>(Coalesce(col));
            Assert.IsType<Column>(Coalesce(col, col));

            Assert.IsType<Column>(InputFileName());

            Assert.IsType<Column>(IsNaN(col));

            Assert.IsType<Column>(IsNull(col));

            Assert.IsType<Column>(MonotonicallyIncreasingId());

            Assert.IsType<Column>(NaNvl(col, col));

            Assert.IsType<Column>(Negate(col));

            Assert.IsType<Column>(Not(col));

            Assert.IsType<Column>(Rand(12345));
            Assert.IsType<Column>(Rand());

            Assert.IsType<Column>(Randn(12345));
            Assert.IsType<Column>(Randn());

            Assert.IsType<Column>(SparkPartitionId());

            Assert.IsType<Column>(Sqrt(col));
            Assert.IsType<Column>(Sqrt("col"));

            Assert.IsType<Column>(Struct());
            Assert.IsType<Column>(Struct(col));
            Assert.IsType<Column>(Struct(col, col));
            Assert.IsType<Column>(Struct("col1"));
            Assert.IsType<Column>(Struct("col1", "col2"));
            Assert.IsType<Column>(Struct("col1", "col2", "col3"));

            Assert.IsType<Column>(When(col, col));
            Assert.IsType<Column>(When(col, "col"));
            Assert.IsType<Column>(When(col, 12345));

            Assert.IsType<Column>(BitwiseNOT(col));

            Assert.IsType<Column>(Expr("expr"));

            //////////////////////////////
            // Math Functions
            //////////////////////////////
            Assert.IsType<Column>(Column("col"));

            Assert.IsType<Column>(Acos(col));
            Assert.IsType<Column>(Acos("col"));

            Assert.IsType<Column>(Asin(col));
            Assert.IsType<Column>(Asin("col"));

            Assert.IsType<Column>(Atan(col));
            Assert.IsType<Column>(Atan("col"));

            Assert.IsType<Column>(Atan2(col, col));
            Assert.IsType<Column>(Atan2(col, "x"));
            Assert.IsType<Column>(Atan2("y", col));
            Assert.IsType<Column>(Atan2("y", "x"));
            Assert.IsType<Column>(Atan2(col, 0.5));
            Assert.IsType<Column>(Atan2("y", 0.5));
            Assert.IsType<Column>(Atan2(0.5, col));
            Assert.IsType<Column>(Atan2(0.5, "x"));

            Assert.IsType<Column>(Bin(col));
            Assert.IsType<Column>(Bin("col"));

            Assert.IsType<Column>(Cbrt(col));
            Assert.IsType<Column>(Cbrt("col"));

            Assert.IsType<Column>(Ceil(col));
            Assert.IsType<Column>(Ceil("col"));

            Assert.IsType<Column>(Conv(col, 2, 10));

            Assert.IsType<Column>(Cos(col));
            Assert.IsType<Column>(Cos("col"));

            Assert.IsType<Column>(Cosh(col));
            Assert.IsType<Column>(Cosh("col"));

            Assert.IsType<Column>(Exp(col));
            Assert.IsType<Column>(Exp("col"));

            Assert.IsType<Column>(Expm1(col));
            Assert.IsType<Column>(Expm1("col"));

            Assert.IsType<Column>(Factorial(col));

            Assert.IsType<Column>(Floor(col));
            Assert.IsType<Column>(Floor("col"));

            Assert.IsType<Column>(Greatest());
            Assert.IsType<Column>(Greatest(col));
            Assert.IsType<Column>(Greatest(col, col));
            Assert.IsType<Column>(Greatest("col1"));
            Assert.IsType<Column>(Greatest("col1", "col2"));
            Assert.IsType<Column>(Greatest("col1", "col2", "col3"));

            Assert.IsType<Column>(Hex(col));

            Assert.IsType<Column>(Unhex(col));

            Assert.IsType<Column>(Hypot(col, col));
            Assert.IsType<Column>(Hypot(col, "right"));
            Assert.IsType<Column>(Hypot("left", col));
            Assert.IsType<Column>(Hypot("left", "right"));
            Assert.IsType<Column>(Hypot(col, 0.5));
            Assert.IsType<Column>(Hypot("left", 0.5));
            Assert.IsType<Column>(Hypot(0.5, col));
            Assert.IsType<Column>(Hypot(0.5, "right"));

            Assert.IsType<Column>(Least());
            Assert.IsType<Column>(Least(col));
            Assert.IsType<Column>(Least(col, col));
            Assert.IsType<Column>(Least("col1"));
            Assert.IsType<Column>(Least("col1", "col2"));
            Assert.IsType<Column>(Least("col1", "col2", "col3"));

            Assert.IsType<Column>(Log(col));
            Assert.IsType<Column>(Log("col"));
            Assert.IsType<Column>(Log(2.0, col));
            Assert.IsType<Column>(Log(2.0, "col"));

            Assert.IsType<Column>(Log10(col));
            Assert.IsType<Column>(Log10("col"));

            Assert.IsType<Column>(Log1p(col));
            Assert.IsType<Column>(Log1p("col"));

            Assert.IsType<Column>(Log2(col));
            Assert.IsType<Column>(Log2("col"));

            Assert.IsType<Column>(Pow(col, col));
            Assert.IsType<Column>(Pow(col, "right"));
            Assert.IsType<Column>(Pow("left", col));
            Assert.IsType<Column>(Pow("left", "right"));
            Assert.IsType<Column>(Pow(col, 0.5));
            Assert.IsType<Column>(Pow("left", 0.5));
            Assert.IsType<Column>(Pow(0.5, col));
            Assert.IsType<Column>(Pow(0.5, "right"));

            Assert.IsType<Column>(Pmod(col, col));

            Assert.IsType<Column>(Rint(col));
            Assert.IsType<Column>(Rint("col"));

            Assert.IsType<Column>(Round(col));
            Assert.IsType<Column>(Round(col, 10));

            Assert.IsType<Column>(Bround(col));
            Assert.IsType<Column>(Bround(col, 10));

            Assert.IsType<Column>(ShiftLeft(col, 4));

            Assert.IsType<Column>(ShiftRight(col, 4));

            Assert.IsType<Column>(ShiftRightUnsigned(col, 4));

            Assert.IsType<Column>(Signum(col));
            Assert.IsType<Column>(Signum("col"));

            Assert.IsType<Column>(Sin(col));
            Assert.IsType<Column>(Sin("col"));

            Assert.IsType<Column>(Sinh(col));
            Assert.IsType<Column>(Sinh("col"));

            Assert.IsType<Column>(Tan(col));
            Assert.IsType<Column>(Tan("col"));

            Assert.IsType<Column>(Tanh(col));
            Assert.IsType<Column>(Tanh("col"));

            Assert.IsType<Column>(Degrees(col));
            Assert.IsType<Column>(Degrees("col"));

            Assert.IsType<Column>(Radians(col));
            Assert.IsType<Column>(Radians("col"));

            //////////////////////////////
            // Miscellaneous Functions
            //////////////////////////////
            Assert.IsType<Column>(Md5(col));

            Assert.IsType<Column>(Sha1(col));

            Assert.IsType<Column>(Sha2(col, 224));

            Assert.IsType<Column>(Crc32(col));

            Assert.IsType<Column>(Hash());
            Assert.IsType<Column>(Hash(col));
            Assert.IsType<Column>(Hash(col, col));

            //////////////////////////////
            // String Functions
            //////////////////////////////
            Assert.IsType<Column>(Ascii(col));

            Assert.IsType<Column>(Base64(col));

            Assert.IsType<Column>(ConcatWs(";"));
            Assert.IsType<Column>(ConcatWs(";", col));
            Assert.IsType<Column>(ConcatWs(";", col, col));

            Assert.IsType<Column>(Decode(col, "UTF-8"));

            Assert.IsType<Column>(Encode(col, "UTF-8"));

            Assert.IsType<Column>(FormatNumber(col, 2));

            Assert.IsType<Column>(FormatString("%s %d"));
            Assert.IsType<Column>(FormatString("%s %d", col));
            Assert.IsType<Column>(FormatString("%s %d", col, col));

            Assert.IsType<Column>(InitCap(col));

            Assert.IsType<Column>(Instr(col, "abc"));

            Assert.IsType<Column>(Length(col));

            Assert.IsType<Column>(Lower(col));

            Assert.IsType<Column>(Levenshtein(col, col));

            Assert.IsType<Column>(Locate("abc", col));
            Assert.IsType<Column>(Locate("abc", col, 3));

            Assert.IsType<Column>(Lpad(col, 3, "pad"));

            Assert.IsType<Column>(Ltrim(col));
            Assert.IsType<Column>(Ltrim(col, "\n"));

            Assert.IsType<Column>(RegexpExtract(col, "[a-z]", 0));

            Assert.IsType<Column>(RegexpReplace(col, "[a-z]", "hello"));
            Assert.IsType<Column>(RegexpReplace(col, col, col));

            Assert.IsType<Column>(Unbase64(col));

            Assert.IsType<Column>(Rpad(col, 3, "pad"));

            Assert.IsType<Column>(Repeat(col, 3));

            Assert.IsType<Column>(Rtrim(col));
            Assert.IsType<Column>(Rtrim(col, "\n"));

            Assert.IsType<Column>(Soundex(col));

            Assert.IsType<Column>(Split(col, "\t"));

            Assert.IsType<Column>(Substring(col, 0, 5));

            Assert.IsType<Column>(SubstringIndex(col, ";", 5));

            Assert.IsType<Column>(Translate(col, "abc", "edf"));

            Assert.IsType<Column>(Trim(col));
            Assert.IsType<Column>(Trim(col, "\n"));

            Assert.IsType<Column>(Upper(col));

            //////////////////////////////
            // DateTime Functions
            //////////////////////////////
            Assert.IsType<Column>(AddMonths(col, 3));

            Assert.IsType<Column>(CurrentDate());

            Assert.IsType<Column>(CurrentTimestamp());

            Assert.IsType<Column>(DateFormat(col, "format"));

            Assert.IsType<Column>(DateAdd(col, 5));

            Assert.IsType<Column>(DateSub(col, 5));

            Assert.IsType<Column>(DateDiff(col, col));

            Assert.IsType<Column>(Year(col));

            Assert.IsType<Column>(Quarter(col));

            Assert.IsType<Column>(Month(col));

            Assert.IsType<Column>(DayOfWeek(col));

            Assert.IsType<Column>(DayOfMonth(col));

            Assert.IsType<Column>(DayOfYear(col));

            Assert.IsType<Column>(Hour(col));

            Assert.IsType<Column>(LastDay(col));

            Assert.IsType<Column>(Minute(col));

            Assert.IsType<Column>(MonthsBetween(col, col));
            Assert.IsType<Column>(MonthsBetween(col, col, false));

            Assert.IsType<Column>(NextDay(col, "Mon"));

            Assert.IsType<Column>(Second(col));

            Assert.IsType<Column>(WeekOfYear(col));

            Assert.IsType<Column>(FromUnixTime(col));
            Assert.IsType<Column>(FromUnixTime(col, "yyyy-MM-dd HH:mm:ss"));

            Assert.IsType<Column>(UnixTimestamp());
            Assert.IsType<Column>(UnixTimestamp(col));
            Assert.IsType<Column>(UnixTimestamp(col, "yyyy-MM-dd HH:mm:ss"));

            Assert.IsType<Column>(ToTimestamp(col));
            Assert.IsType<Column>(ToTimestamp(col, "yyyy-MM-dd HH:mm:ss"));

            Assert.IsType<Column>(ToDate(col));
            Assert.IsType<Column>(ToDate(col, "yyyy-MM-dd HH:mm:ss"));

            Assert.IsType<Column>(Trunc(col, "yyyy"));

            Assert.IsType<Column>(DateTrunc("mon", col));

            if (SparkSettings.Version < new Version(Versions.V3_0_0))
            {
                // The following APIs are deprecated in Spark 3.0.
                Assert.IsType<Column>(FromUtcTimestamp(col, "GMT+1"));
                Assert.IsType<Column>(FromUtcTimestamp(col, col));

                Assert.IsType<Column>(ToUtcTimestamp(col, "GMT+1"));
                Assert.IsType<Column>(ToUtcTimestamp(col, col));
            }

            Assert.IsType<Column>(Window(col, "1 minute", "10 seconds", "5 seconds"));
            Assert.IsType<Column>(Window(col, "1 minute", "10 seconds"));
            Assert.IsType<Column>(Window(col, "1 minute"));

            //////////////////////////////
            // Collection Functions
            //////////////////////////////
            Assert.IsType<Column>(ArrayContains(col, 12345));
            Assert.IsType<Column>(ArrayContains(col, "str"));

            Assert.IsType<Column>(ArraysOverlap(col, col));

            Assert.IsType<Column>(Slice(col, 0, 4));

            Assert.IsType<Column>(ArrayJoin(col, ":", "replacement"));
            Assert.IsType<Column>(ArrayJoin(col, ":"));

            Assert.IsType<Column>(Concat());
            Assert.IsType<Column>(Concat(col));
            Assert.IsType<Column>(Concat(col, col));

            Assert.IsType<Column>(ArrayPosition(col, 1));

            Assert.IsType<Column>(ElementAt(col, 1));

            Assert.IsType<Column>(ArraySort(col));

            Assert.IsType<Column>(ArrayRemove(col, "elementToRemove"));

            Assert.IsType<Column>(ArrayDistinct(col));

            Assert.IsType<Column>(ArrayIntersect(col, col));

            Assert.IsType<Column>(ArrayUnion(col, col));

            Assert.IsType<Column>(ArrayExcept(col, col));

            Assert.IsType<Column>(Explode(col));

            Assert.IsType<Column>(ExplodeOuter(col));

            Assert.IsType<Column>(PosExplode(col));

            Assert.IsType<Column>(PosExplodeOuter(col));

            Assert.IsType<Column>(GetJsonObject(col, "abc.json"));

            Assert.IsType<Column>(JsonTuple(col, "a"));
            Assert.IsType<Column>(JsonTuple(col, "a", "b"));

            var options = new Dictionary<string, string>() { { "hello", "world" } };
            Column schema = SchemaOfJson("[{\"col\":0}]");

            Assert.IsType<Column>(FromJson(col, "a Int"));
            Assert.IsType<Column>(FromJson(col, "a Int", options));
            Assert.IsType<Column>(FromJson(col, schema));
            Assert.IsType<Column>(FromJson(col, schema, options));

            Assert.IsType<Column>(SchemaOfJson("{}"));
            Assert.IsType<Column>(SchemaOfJson(col));

            Assert.IsType<Column>(ToJson(col));
            Assert.IsType<Column>(ToJson(col, options));

            Assert.IsType<Column>(Size(col));

            Assert.IsType<Column>(SortArray(col));
            Assert.IsType<Column>(SortArray(col, true));
            Assert.IsType<Column>(SortArray(col, false));

            Assert.IsType<Column>(ArrayMin(col));

            Assert.IsType<Column>(ArrayMax(col));

            Assert.IsType<Column>(Shuffle(col));

            Assert.IsType<Column>(Reverse(col));

            Assert.IsType<Column>(Flatten(col));

            Assert.IsType<Column>(Sequence(col, col, col));
            Assert.IsType<Column>(Sequence(col, col));

            Assert.IsType<Column>(ArrayRepeat(col, col));
            Assert.IsType<Column>(ArrayRepeat(col, 5));

            Assert.IsType<Column>(MapKeys(col));

            Assert.IsType<Column>(MapValues(col));

            Assert.IsType<Column>(MapFromEntries(col));

            Assert.IsType<Column>(ArraysZip());
            Assert.IsType<Column>(ArraysZip(col));
            Assert.IsType<Column>(ArraysZip(col, col));

            Assert.IsType<Column>(MapConcat());
            Assert.IsType<Column>(MapConcat(col));
            Assert.IsType<Column>(MapConcat(col, col));

            //////////////////////////////
            // Udf Functions
            //////////////////////////////
            TestUdf();

            Assert.IsType<Column>(CallUDF("udf"));
            Assert.IsType<Column>(CallUDF("udf", col));
            Assert.IsType<Column>(CallUDF("udf", col, col));
        }

        private void TestUdf()
        {
            // Test Udf with different number of arguments.
            Column col = Udf(() => 1)();

            col = Udf<int, int>((a1) => 1)(col);

            col = Udf<int, int, int>((a1, a2) => 1)(col, col);

            col = Udf<int, int, int, int>((a1, a2, a3) => 1)(col, col, col);

            col = Udf<int, int, int, int, int>((a1, a2, a3, a4) => 1)(col, col, col, col);

            col = Udf<int, int, int, int, int, int>(
                (a1, a2, a3, a4, a5) => 1)(col, col, col, col, col);

            col = Udf<int, int, int, int, int, int, int>(
                (a1, a2, a3, a4, a5, a6) => 1)(col, col, col, col, col, col);

            col = Udf<int, int, int, int, int, int, int, int>(
                (a1, a2, a3, a4, a5, a6, a7) => 1)(col, col, col, col, col, col, col);

            col = Udf<int, int, int, int, int, int, int, int, int>(
                (a1, a2, a3, a4, a5, a6, a7, a8) => 1)(col, col, col, col, col, col, col, col);

            col = Udf<int, int, int, int, int, int, int, int, int, int>(
                (a1, a2, a3, a4, a5, a6, a7, a8, a9) => 1)(
                    col, col, col, col, col, col, col, col, col);

            col = Udf<int, int, int, int, int, int, int, int, int, int, int>(
                (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) => 1)(
                    col, col, col, col, col, col, col, col, col, col);

            // Test various retun types of Udf.

            // Test simple types.
            Udf<string, string>((arg) => arg);
            Udf<byte[], byte[]>((arg) => arg);
            Udf<bool, bool>((arg) => arg);
            Udf<decimal, decimal>((arg) => arg);
            Udf<double, double>((arg) => arg);
            Udf<float, float>((arg) => arg);
            Udf<byte, byte>((arg) => arg);
            Udf<int, int>((arg) => arg);
            Udf<long, long>((arg) => arg);
            Udf<short, short>((arg) => arg);
            Udf<Date, Date>((arg) => arg);
            Udf<Timestamp, Timestamp>((arg) => arg);

            // Test array type.
            Udf<string, string[]>((arg) => new[] { arg });
            Udf<string, IEnumerable<string>>((arg) => new[] { arg });
            Udf<string, IEnumerable<IEnumerable<string>>>((arg) => new[] { new[] { arg } });

            // Test map type.
            Udf<string, Dictionary<string, string>>(
                (arg) => new Dictionary<string, string> { { arg, arg } });
            Udf<string, IDictionary<string, string>>(
                (arg) => new Dictionary<string, string> { { arg, arg } });
            Udf<string, IDictionary<string, string[]>>(
                (arg) => new Dictionary<string, string[]> { { arg, new[] { arg } } });
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.0.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            Column col = Column("col");

            Assert.IsType<Column>(XXHash64());
            Assert.IsType<Column>(XXHash64(col));
            Assert.IsType<Column>(XXHash64(col, col));

            Assert.IsType<Column>(Split(col, "\t", 1));
            Assert.IsType<Column>(Split(col, "\t", -1));

            Assert.IsType<Column>(Overlay(col, col, col));
            Assert.IsType<Column>(Overlay(col, col, col, col));

            Assert.IsType<Column>(AddMonths(col, col));

            Assert.IsType<Column>(DateAdd(col, col));

            Assert.IsType<Column>(DateSub(col, col));

            var options = new Dictionary<string, string>() { { "hello", "world" } };
            Assert.IsType<Column>(SchemaOfJson(col, options));

            Assert.IsType<Column>(MapEntries(col));

            Column schemaCol = SchemaOfCsv("[{\"col\":0}]");
            Assert.IsType<Column>(FromCsv(col, schemaCol, options));

            Assert.IsType<Column>(SchemaOfCsv(col));
            Assert.IsType<Column>(SchemaOfCsv(col, options));

            Assert.IsType<Column>(ToCsv(col));
            Assert.IsType<Column>(ToCsv(col, options));

            Assert.IsType<Column>(Years(col));

            Assert.IsType<Column>(Months(col));

            Assert.IsType<Column>(Days(col));

            Assert.IsType<Column>(Hours(col));

            Assert.IsType<Column>(Bucket(Lit(1), col));
            Assert.IsType<Column>(Bucket(1, col));
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.1.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_1_0)]
        public void TestSignaturesV3_1_X()
        {
            Column col = Column("col");

            Assert.IsType<Column>(PercentileApprox(col, col, col));

            Assert.IsType<Column>(NthValue(col, 0));
            Assert.IsType<Column>(NthValue(col, 0, true));

            Assert.IsType<Column>(Acosh(col));
            Assert.IsType<Column>(Acosh("col"));

            Assert.IsType<Column>(Asinh(col));
            Assert.IsType<Column>(Asinh("col"));

            Assert.IsType<Column>(Atanh(col));
            Assert.IsType<Column>(Atanh("col"));

            Assert.IsType<Column>(AssertTrue(col));
            Assert.IsType<Column>(AssertTrue(col, col));

            Assert.IsType<Column>(RaiseError(col));

            Assert.IsType<Column>(TimestampSeconds(col));

            Assert.IsType<Column>(Slice(col, col, col));
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.2.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_2_0)]
        public void TestSignaturesV3_2_X()
        {
            Column col = Column("col");

            Assert.IsType<Column>(Count_Distinct(col, col, col));

            Assert.IsType<Column>(Product(col));

            Assert.IsType<Column>(Sum_Distinct(col));

            Assert.IsType<Column>(Lag(col, 2, null, true));

            Assert.IsType<Column>(Lead(col, 2, null, true));

            Assert.IsType<Column>(Bitwise_Not(col));

            Assert.IsType<Column>(Shiftleft(col, 2));

            Assert.IsType<Column>(Shiftright(col, 2));

            Assert.IsType<Column>(Shiftrightunsigned(col, 2));

            Assert.IsType<Column>(Sentences(col, col, col));
            Assert.IsType<Column>(Sentences(col));

            Assert.IsType<Column>(NextDay(col, col));

            Assert.IsType<Column>(Session_Window(col, "5 seconds"));
            Assert.IsType<Column>(Session_Window(col, col));

            Assert.IsType<Column>(Call_UDF("name", col, col));
        }
    }
}
