// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Catalog;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.E2ETest.Utils.SQLUtils;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class CatalogTests
    {
        private readonly SparkSession _spark;

        public CatalogTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            WithTable(_spark, new string[] { "users", "users2", "users3", "users4", "usersp" }, () =>
            {
                Catalog catalog = _spark.Catalog;

                Assert.IsType<DataFrame>(catalog.ListDatabases());
                Assert.IsType<DataFrame>(catalog.ListFunctions());
                Assert.IsType<DataFrame>(catalog.ListFunctions("default"));

                string usersFilePath = Path.Combine(TestEnvironment.ResourceDirectory, "users.parquet");
                var usersSchema = new StructType(new[]
                {
                    new StructField("name", new StringType()),
                    new StructField("favorite_color", new StringType()),
                });
                var tableOptions = new Dictionary<string, string>() { { "path", usersFilePath } };
                Assert.IsType<DataFrame>(catalog.CreateTable("users", usersFilePath));
                Assert.IsType<DataFrame>(catalog.CreateTable("users2", usersFilePath, "parquet"));
                Assert.IsType<DataFrame>(catalog.CreateTable("users3", "parquet", tableOptions));
                Assert.IsType<DataFrame>(
                    catalog.CreateTable("users4", "parquet", usersSchema, tableOptions));

                Assert.IsType<string>(catalog.CurrentDatabase());
                Assert.IsType<bool>(catalog.DatabaseExists("default"));

                Assert.IsType<bool>(catalog.DropGlobalTempView("no-view"));
                Assert.IsType<bool>(catalog.DropTempView("no-view"));
                Assert.IsType<bool>(catalog.FunctionExists("default", "functionname"));
                Assert.IsType<bool>(catalog.FunctionExists("functionname"));
                Assert.IsType<Database>(catalog.GetDatabase("default"));
                Assert.IsType<Function>(catalog.GetFunction("abs"));
                Assert.IsType<Table>(catalog.GetTable("users"));
                Assert.IsType<Table>(catalog.GetTable("default", "users"));
                Assert.IsType<bool>(catalog.IsCached("users"));
                Assert.IsType<DataFrame>(catalog.ListColumns("users"));
                Assert.IsType<DataFrame>(catalog.ListColumns("default", "users"));
                Assert.IsType<DataFrame>(catalog.ListDatabases());
                Assert.IsType<DataFrame>(catalog.ListFunctions());
                Assert.IsType<DataFrame>(catalog.ListFunctions("default"));
                Assert.IsType<DataFrame>(catalog.ListTables());
                Assert.IsType<DataFrame>(catalog.ListTables("default"));

                catalog.RefreshByPath("/");
                catalog.RefreshTable("users");
                catalog.SetCurrentDatabase("default");
                catalog.CacheTable("users");
                catalog.UncacheTable("users");
                catalog.ClearCache();

                Assert.IsType<bool>(catalog.TableExists("users"));
                Assert.IsType<bool>(catalog.TableExists("default", "users"));

                _spark.Sql(@"CREATE TABLE IF NOT EXISTS usersp USING PARQUET PARTITIONED BY (name)  
                            AS SELECT * FROM users");
                catalog.RecoverPartitions("usersp");
            });
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.1.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_1_0)]
        public void TestSignaturesV3_1_X()
        {
            WithTable(_spark, new string[] { "users1", "users2" }, () =>
            {
                Catalog catalog = _spark.Catalog;

                string usersFilePath = Path.Combine(TestEnvironment.ResourceDirectory, "users.parquet");
                var usersSchema = new StructType(new[]
                {
                    new StructField("name", new StringType()),
                    new StructField("favorite_color", new StringType()),
                });
                var tableOptions = new Dictionary<string, string>() { { "path", usersFilePath } };
                Assert.IsType<DataFrame>(
                    catalog.CreateTable("users1", "parquet", "description", tableOptions));
                Assert.IsType<DataFrame>(
                    catalog.CreateTable("users2", "parquet", usersSchema, "description", tableOptions));
            });
        }
    }
}
