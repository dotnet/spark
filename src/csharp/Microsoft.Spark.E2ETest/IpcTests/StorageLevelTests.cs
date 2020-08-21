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
        /// Test setting Storage Level as NONE, persisting a <see cref="DataFrame"/> and then
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelNone()
        {
            StorageLevel storageNone = StorageLevel.NONE;
            _df.Persist(storageNone);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageNone, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as DISK_ONLY, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelDiskOnly()
        {
            StorageLevel storageDiskOnly = StorageLevel.DISK_ONLY;
            _df.Persist(storageDiskOnly);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageDiskOnly, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as DISK_ONLY_2, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelDiskOnly2()
        {
            StorageLevel storageDiskOnly2 = StorageLevel.DISK_ONLY_2;
            _df.Persist(storageDiskOnly2);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageDiskOnly2, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_ONLY, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryOnly()
        {
            StorageLevel storageMemoryOnly = StorageLevel.MEMORY_ONLY;
            _df.Persist(storageMemoryOnly);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryOnly, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_ONLY_2, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryOnly2()
        {
            StorageLevel storageMemoryOnly2 = StorageLevel.MEMORY_ONLY_2;
            _df.Persist(storageMemoryOnly2);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryOnly2, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_ONLY_SER, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryOnlySer()
        {
            StorageLevel storageMemoryOnlySer = StorageLevel.MEMORY_ONLY_SER;
            _df.Persist(storageMemoryOnlySer);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryOnlySer, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_ONLY_SER_2, persisting a <see cref="DataFrame"/>
        /// and verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryOnlySer2()
        {
            StorageLevel storageMemoryOnlySer2 = StorageLevel.MEMORY_ONLY_SER_2;
            _df.Persist(storageMemoryOnlySer2);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryOnlySer2, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_AND_DISK, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryAndDisk()
        {
            StorageLevel storageMemoryAndDisk = StorageLevel.MEMORY_AND_DISK;
            _df.Persist(storageMemoryAndDisk);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryAndDisk, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_AND_DISK_2, persisting a <see cref="DataFrame"/>
        /// and verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryAndDisk2()
        {
            StorageLevel storageMemoryAndDisk2 = StorageLevel.MEMORY_AND_DISK_2;
            _df.Persist(storageMemoryAndDisk2);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryAndDisk2, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_AND_DISK_SER, persisting a <see cref="DataFrame"/>
        /// and verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryAndDiskSer()
        {
            StorageLevel storageMemoryAndDiskSer = StorageLevel.MEMORY_AND_DISK_SER;
            _df.Persist(storageMemoryAndDiskSer);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryAndDiskSer, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as MEMORY_AND_DISK_SER_2, persisting a
        /// <see cref="DataFrame"/> and verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelMemoryAndDiskSer2()
        {
            StorageLevel storageMemoryAndDiskSer2 = StorageLevel.MEMORY_AND_DISK_SER_2;
            _df.Persist(storageMemoryAndDiskSer2);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemoryAndDiskSer2, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

        /// <summary>
        /// Test setting Storage Level as OFF_HEAP, persisting a <see cref="DataFrame"/> and
        /// verifying the new Storage Level of the DataFrame.
        /// </summary>
        [Fact]
        public void TestStorageLevelOffHeap()
        {
            StorageLevel storageMemorOffHeap = StorageLevel.OFF_HEAP;
            _df.Persist(storageMemorOffHeap);
            StorageLevel dfNewStorageLevel = _df.StorageLevel();
            Assert.Equal(storageMemorOffHeap, dfNewStorageLevel);
            // Needs to be unpersisted so other Persists can take effect.
            _df.Unpersist();
        }

    }
}
