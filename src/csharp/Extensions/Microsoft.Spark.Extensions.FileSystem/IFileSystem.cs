using System;

namespace Microsoft.Spark.Extensions.FileSystem
{
    /// <summary>
    /// An interface for a fairly generic filesystem. It may be implemented as a distributed filesystem, or as a "local"
    /// one that reflects the locally-connected disk. The local version exists for small Hadoop instances and for testing.
    /// 
    /// All user code that may potentially use the Hadoop Distributed File System should be written to use an IFileSystem
    /// object. The Hadoop DFS is a multi-machine system that appears as a single disk.It's useful because of its fault
    /// tolerance and potentially very large capacity.
    /// </summary>
    public interface IFileSystem : IDisposable
    {
        /// <summary>
        /// Delete a file.
        /// </summary>
        /// <param name="path">The path to delete.</param>
        /// <param name="recursive">If path is a directory and set to true, the directory is deleted else throws an
        /// exception. In case of a file the recursive can be set to either true or false.</param>
        /// <returns>True if delete is successful else false.</returns>
        bool Delete(string path, bool recursive = true);
    }
}
