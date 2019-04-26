﻿using System;
using System.IO;

namespace Microsoft.Spark.E2ETest.Utils
{
    /// <summary>
    /// Creates a temporary folder at a specified root path that is automatically
    /// cleaned up when disposed.
    /// </summary>
    internal sealed class TemporaryDirectory : IDisposable
    {
        private bool disposed = false;

        /// <summary>
        /// Path to temporary folder.
        /// </summary>
        public string Path { get; }

        public TemporaryDirectory(string rootDirectory)
        {
            Path = System.IO.Path.Combine(rootDirectory, Guid.NewGuid().ToString());
            Cleanup();
            Directory.CreateDirectory(Path);
        }

        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);
            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        private void Cleanup()
        {
            if (File.Exists(Path))
            {
                File.Delete(Path);
            }
            else if (Directory.Exists(Path))
            {
                Directory.Delete(Path, true);
            }
        }

        private void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                Cleanup();
            }

            disposed = true;
        }
    }
}
