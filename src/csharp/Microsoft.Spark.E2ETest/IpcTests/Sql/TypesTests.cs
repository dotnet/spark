// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class TypesTests
    {
        private readonly IJvmBridge _jvm;

        public TypesTests(SparkFixture fixture) => _jvm = fixture.Jvm;

        private void Validate(DataType dataType) =>
            Assert.IsType<JvmObjectReference>(DataType.FromJson(_jvm, dataType.Json));

        [Fact]
        public void TestDataTypes()
        {
            // The following validates for all SimpleTypes.
            Validate(new NullType());
            Validate(new StringType());
            Validate(new BinaryType());
            Validate(new BooleanType());
            Validate(new DateType());
            Validate(new TimestampType());
            Validate(new DoubleType());
            Validate(new FloatType());
            Validate(new ByteType());
            Validate(new IntegerType());
            Validate(new LongType());
            Validate(new ShortType());
            Validate(new DecimalType());

            // The following validates for all ComplexTypes.
            Validate(new ArrayType(new IntegerType()));
            Validate(new MapType(new IntegerType(), new StringType()));
            Validate(new StructType(new[]
            {
                new StructField("age", new IntegerType()),
                new StructField("name", new StringType())
            }));

            // StructField is not tested because it cannot be converted from JSON by itself.
        }
    }
}
