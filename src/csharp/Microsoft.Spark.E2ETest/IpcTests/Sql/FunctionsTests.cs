// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
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
        /// Test signatures for APIs up to Spark 2.3.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            //////////////////////////////
            // Basic Functions
            //////////////////////////////

            Column col = Column("col1");

            col = Col("col2");

            col = Lit(1);
            col = Lit("some column");
            col = Lit(col);

            //////////////////////////////
            // Sort Functions
            //////////////////////////////
            col = Asc("col");

            col = AscNullsFirst("col");

            col = AscNullsLast("col");

            col = Desc("col");

            col = DescNullsFirst("col");

            col = DescNullsLast("col");

            //////////////////////////////
            // Aggregate Functions
            //////////////////////////////
            col = Column("col");

            col = ApproxCountDistinct(col);
            col = ApproxCountDistinct("col");
            col = ApproxCountDistinct(col, 0.05);
            col = ApproxCountDistinct("col", 0.05);

            col = Avg(col);
            col = Avg("col");

            col = CollectList(col);
            col = CollectList("col");

            col = CollectSet(col);
            col = CollectSet("col");

            col = Corr(col, col);
            col = Corr("col1", "col2");

            col = Count(col);
            col = Count("col");

            col = CountDistinct(col);
            col = CountDistinct(col, col);
            col = CountDistinct(col, col, col);
            col = CountDistinct("col1");
            col = CountDistinct("col1", "col2");
            col = CountDistinct("col1", "col2", "col3");

            col = CovarPop(col, col);
            col = CovarPop("col1", "col2");

            col = CovarSamp(col, col);
            col = CovarSamp("col1", "col2");

            col = First(col);
            col = First(col, true);
            col = First(col, false);
            col = First("col");
            col = First("col", true);
            col = First("col", false);

            col = Grouping(col);
            col = Grouping("col");

            col = GroupingId();
            col = GroupingId(col);
            col = GroupingId(col, col);
            col = GroupingId("col1");
            col = GroupingId("col1", "col2");
            col = GroupingId("col1", "col2", "col3");

            col = Kurtosis(col);
            col = Kurtosis("col");

            col = Last(col);
            col = Last(col, true);
            col = Last(col, false);
            col = Last("col");
            col = Last("col", true);
            col = Last("col", false);

            col = Max(col);
            col = Max("col");

            col = Mean(col);
            col = Mean("col");

            col = Min(col);
            col = Min("col");

            col = Skewness(col);
            col = Skewness("col");

            col = Stddev(col);
            col = Stddev("col");

            col = StddevSamp(col);
            col = StddevSamp("col");

            col = StddevPop(col);
            col = StddevPop("col");

            col = Sum(col);
            col = Sum("col");

            col = SumDistinct(col);
            col = SumDistinct("col");

            col = Variance(col);
            col = Variance("col");

            col = VarSamp(col);
            col = VarSamp("col");

            col = VarPop(col);
            col = VarPop("col");

            //////////////////////////////
            // Window Functions
            //////////////////////////////
            col = UnboundedPreceding();

            col = UnboundedFollowing();

            col = CurrentRow();

            col = CumeDist();

            col = DenseRank();

            col = Lag(col, 0);
            col = Lag(col, 2, "col2");
            col = Lag("col", 0);
            col = Lag("col", 2, "col2");

            col = Lead(col, 0);
            col = Lead(col, 2, "col2");
            col = Lead("col", 0);
            col = Lead("col", 2, "col2");

            col = Ntile(100);

            col = PercentRank();

            col = Rank();

            col = RowNumber();

            //////////////////////////////
            // Non-Aggregate Functions
            //////////////////////////////
            col = Column("col");

            col = Abs(col);

            col = Array();
            col = Array(col);
            col = Array(col, col);
            col = Array("col1");
            col = Array("col1", "col2");
            col = Array("col1", "col2", "col3");

            col = Map();
            col = Map(col);
            col = Map(col, col);

            DataFrame df = _spark
               .Read()
               .Json($"{TestEnvironment.ResourceDirectory}people.json");
            df = Broadcast(df);

            col = Coalesce();
            col = Coalesce(col);
            col = Coalesce(col, col);

            col = InputFileName();

            col = IsNaN(col);

            col = IsNull(col);

            col = MonotonicallyIncreasingId();

            col = NaNvl(col, col);

            col = Negate(col);

            col = Not(col);

            col = Rand(12345);
            col = Rand();

            col = Randn(12345);
            col = Randn();

            col = SparkPartitionId();

            col = Sqrt(col);
            col = Sqrt("col");

            col = Struct();
            col = Struct(col);
            col = Struct(col, col);
            col = Struct("col1");
            col = Struct("col1", "col2");
            col = Struct("col1", "col2", "col3");

            col = When(col, col);
            col = When(col, "col");
            col = When(col, 12345);

            col = BitwiseNOT(col);

            col = Expr("expr");

            //////////////////////////////
            // Math Functions
            //////////////////////////////
            col = Column("col");

            col = Acos(col);
            col = Acos("col");

            col = Asin(col);
            col = Asin("col");

            col = Atan(col);
            col = Atan("col");

            col = Atan2(col, col);
            col = Atan2(col, "x");
            col = Atan2("y", col);
            col = Atan2("y", "x");
            col = Atan2(col, 0.5);
            col = Atan2("y", 0.5);
            col = Atan2(0.5, col);
            col = Atan2(0.5, "x");

            col = Bin(col);
            col = Bin("col");

            col = Cbrt(col);
            col = Cbrt("col");

            col = Ceil(col);
            col = Ceil("col");

            col = Conv(col, 2, 10);

            col = Cos(col);
            col = Cos("col");

            col = Cosh(col);
            col = Cosh("col");

            col = Exp(col);
            col = Exp("col");

            col = Expm1(col);
            col = Expm1("col");

            col = Factorial(col);

            col = Floor(col);
            col = Floor("col");

            col = Greatest();
            col = Greatest(col);
            col = Greatest(col, col);
            col = Greatest("col1");
            col = Greatest("col1", "col2");
            col = Greatest("col1", "col2", "col3");

            col = Hex(col);

            col = Unhex(col);

            col = Hypot(col, col);
            col = Hypot(col, "right");
            col = Hypot("left", col);
            col = Hypot("left", "right");
            col = Hypot(col, 0.5);
            col = Hypot("left", 0.5);
            col = Hypot(0.5, col);
            col = Hypot(0.5, "right");

            col = Least();
            col = Least(col);
            col = Least(col, col);
            col = Least("col1");
            col = Least("col1", "col2");
            col = Least("col1", "col2", "col3");

            col = Log(col);
            col = Log("col");
            col = Log(2.0, col);
            col = Log(2.0, "col");

            col = Log10(col);
            col = Log10("col");

            col = Log1p(col);
            col = Log1p("col");

            col = Log2(col);
            col = Log2("col");

            col = Pow(col, col);
            col = Pow(col, "right");
            col = Pow("left", col);
            col = Pow("left", "right");
            col = Pow(col, 0.5);
            col = Pow("left", 0.5);
            col = Pow(0.5, col);
            col = Pow(0.5, "right");

            col = Pmod(col, col);

            col = Rint(col);
            col = Rint("col");

            col = Round(col);
            col = Round(col, 10);

            col = Bround(col);
            col = Bround(col, 10);

            col = ShiftLeft(col, 4);

            col = ShiftRight(col, 4);

            col = ShiftRightUnsigned(col, 4);

            col = Signum(col);
            col = Signum("col");

            col = Sin(col);
            col = Sin("col");

            col = Sinh(col);
            col = Sinh("col");

            col = Tan(col);
            col = Tan("col");

            col = Tanh(col);
            col = Tanh("col");

            col = Degrees(col);
            col = Degrees("col");

            col = Radians(col);
            col = Radians("col");

            //////////////////////////////
            // Miscellaneous Functions
            //////////////////////////////
            col = Md5(col);

            col = Sha1(col);

            col = Sha2(col, 224);

            col = Crc32(col);

            col = Hash();
            col = Hash(col);
            col = Hash(col, col);

            //////////////////////////////
            // String Functions
            //////////////////////////////
            col = Ascii(col);

            col = Base64(col);

            col = ConcatWs(";");
            col = ConcatWs(";", col);
            col = ConcatWs(";", col, col);

            col = Decode(col, "UTF-8");

            col = Encode(col, "UTF-8");

            col = FormatNumber(col, 2);

            col = FormatString("%s %d");
            col = FormatString("%s %d", col);
            col = FormatString("%s %d", col, col);

            col = InitCap(col);

            col = Instr(col, "abc");

            col = Length(col);

            col = Lower(col);

            col = Levenshtein(col, col);

            col = Locate("abc", col);
            col = Locate("abc", col, 3);

            col = Lpad(col, 3, "pad");

            col = Ltrim(col);
            col = Ltrim(col, "\n");

            col = RegexpExtract(col, "[a-z]", 0);

            col = RegexpReplace(col, "[a-z]", "hello");
            col = RegexpReplace(col, col, col);

            col = Unbase64(col);

            col = Rpad(col, 3, "pad");

            col = Repeat(col, 3);

            col = Rtrim(col);
            col = Rtrim(col, "\n");

            col = Soundex(col);

            col = Split(col, "\t");

            col = Substring(col, 0, 5);

            col = SubstringIndex(col, ";", 5);

            col = Translate(col, "abc", "edf");

            col = Trim(col);
            col = Trim(col, "\n");

            col = Upper(col);

            //////////////////////////////
            // DateTime Functions
            //////////////////////////////
            col = AddMonths(col, 3);

            col = CurrentDate();

            col = CurrentTimestamp();

            col = DateFormat(col, "format");

            col = DateAdd(col, 5);

            col = DateSub(col, 5);

            col = DateDiff(col, col);

            col = Year(col);

            col = Quarter(col);

            col = Month(col);

            col = DayOfWeek(col);

            col = DayOfMonth(col);

            col = DayOfYear(col);

            col = Hour(col);

            col = LastDay(col);

            col = Minute(col);

            col = MonthsBetween(col, col);

            col = NextDay(col, "Mon");

            col = Second(col);

            col = WeekOfYear(col);

            col = FromUnixTime(col);
            col = FromUnixTime(col, "yyyy-MM-dd HH:mm:ss");

            col = UnixTimestamp();
            col = UnixTimestamp(col);
            col = UnixTimestamp(col, "yyyy-MM-dd HH:mm:ss");

            col = ToTimestamp(col);
            col = ToTimestamp(col, "yyyy-MM-dd HH:mm:ss");

            col = ToDate(col);
            col = ToDate(col, "yyyy-MM-dd HH:mm:ss");

            col = Trunc(col, "yyyy");

            col = DateTrunc("mon", col);

            col = FromUtcTimestamp(col, "GMT+1");

            col = ToUtcTimestamp(col, "GMT+1");

            col = Window(col, "1 minute", "10 seconds");
            col = Window(col, "1 minute", "10 seconds", "5 seconds");
            col = Window(col, "1 minute");

            //////////////////////////////
            // Collection Functions
            //////////////////////////////
            col = ArrayContains(col, 12345);
            col = ArrayContains(col, "str");

            col = Concat();
            col = Concat(col);
            col = Concat(col, col);

            col = Explode(col);

            col = ExplodeOuter(col);

            col = PosExplode(col);

            col = PosExplodeOuter(col);

            col = GetJsonObject(col, "abc.json");

            col = JsonTuple(col, "a");
            col = JsonTuple(col, "a", "b");

            var options = new Dictionary<string, string>() { { "hello", "world" } };

            col = FromJson(col, "a Int");
            col = FromJson(col, "a Int", options);

            col = ToJson(col);
            col = ToJson(col, options);

            col = Size(col);

            col = SortArray(col);
            col = SortArray(col, true);
            col = SortArray(col, false);

            col = Reverse(col);

            col = MapKeys(col);

            col = MapValues(col);

            //////////////////////////////
            // Udf Functions
            //////////////////////////////
            col = Udf(() => 1)();

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

            col = CallUDF("udf");
            col = CallUDF("udf", col);
            col = CallUDF("udf", col, col);
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 2.4.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestSignaturesV2_4_X()
        {
            Column col = Column("col");

            col = MapFromArrays(col, col);

            col = MonthsBetween(col, col, false);

            col = FromUtcTimestamp(col, col);

            col = ToUtcTimestamp(col, col);

            col = ArraysOverlap(col, col);

            col = Slice(col, 0, 4);

            col = ArrayJoin(col, ":", "replacement");
            col = ArrayJoin(col, ":");

            col = ArrayPosition(col, 1);

            col = ElementAt(col, 1);

            col = ArraySort(col);

            col = ArrayRemove(col, "elementToRemove");

            col = ArrayDistinct(col);

            col = ArrayIntersect(col, col);

            col = ArrayUnion(col, col);

            col = ArrayExcept(col, col);

            var options = new Dictionary<string, string>() { { "hello", "world" } };
            Column schema = SchemaOfJson("[{\"col\":0}]");

            col = FromJson(col, schema);
            col = FromJson(col, schema, options);

            col = SchemaOfJson("{}");
            col = SchemaOfJson(col);

            col = ArrayMin(col);

            col = ArrayMax(col);

            col = Shuffle(col);

            col = Reverse(col);

            col = Flatten(col);

            col = Sequence(col, col, col);
            col = Sequence(col, col);

            col = ArrayRepeat(col, col);
            col = ArrayRepeat(col, 5);

            col = MapFromEntries(col);

            col = ArraysZip();
            col = ArraysZip(col);
            col = ArraysZip(col, col);

            col = MapConcat();
            col = MapConcat(col);
            col = MapConcat(col, col);
        }
    }
}
