// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.E2ETest;
using Xunit;

namespace Microsoft.Spark.Extensions.Hadoop.FileSystem.E2ETest
{
    [CollectionDefinition(Constants.FileSystemTestContainerName)]
    public class FileSystemTestCollection : ICollectionFixture<SparkFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
