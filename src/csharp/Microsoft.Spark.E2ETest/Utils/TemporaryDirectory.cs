// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;

namespace Microsoft.Spark.E2ETest.Utils
{
    /// <summary>
    /// Creates a temporary folder that is automatically cleaned up when disposed.
    /// </summary>
    internal sealed class TemporaryDirectory : IDisposable
    {
        private bool disposed = false;

        /// <summary>
        /// Path to temporary folder.
        /// </summary>
        public string Path { get; }

        public TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), Guid.NewGuid().ToString());
            Cleanup();
            Directory.CreateDirectory(Path);
            Path = $"{Path}{System.IO.Path.DirectorySeparatorChar}";
        }

        public void Dispose()
        {
            Dispose(true);
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
            {
                return;
            }

            if (disposing)
            {
                Cleanup();
            }

            disposed = true;
        }
    }
}
