using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class StorageLevelTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public StorageLevelTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _df = _spark.CreateDataFrame(new[] { "hello", "world" });
        }

        /// <summary>
        /// Testing all public properties and methods of StorageLevel objects.
        /// </summary>
        [Fact]
        public void TestStorageLevelProperties()
        {
            var storageLevels = new List<StorageLevel> {
                StorageLevel.NONE,
                StorageLevel.DISK_ONLY,
                StorageLevel.DISK_ONLY_2,
                StorageLevel.MEMORY_ONLY,
                StorageLevel.MEMORY_ONLY_2,
                StorageLevel.MEMORY_ONLY_SER,
                StorageLevel.MEMORY_ONLY_SER_2,
                StorageLevel.MEMORY_AND_DISK,
                StorageLevel.MEMORY_AND_DISK_2,
                StorageLevel.MEMORY_AND_DISK_SER,
                StorageLevel.MEMORY_AND_DISK_SER_2,
                StorageLevel.OFF_HEAP
            };
            foreach (StorageLevel expected in storageLevels)
            {
                _df.Persist(expected);
                StorageLevel actual = _df.StorageLevel();
                Assert.Equal(expected, actual);
                // Needs to be unpersisted so other Persists can take effect.
                _df.Unpersist();
            }

            StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK;
            Assert.True(storageLevel.UseDisk);
            Assert.True(storageLevel.UseMemory);
            Assert.False(storageLevel.UseOffHeap);
            Assert.True(storageLevel.Deserialized);
            Assert.Equal(1, storageLevel.Replication);

            Assert.IsType<string>(storageLevel.Description());
            Assert.IsType<string>(storageLevel.ToString());
        }
    }
}
