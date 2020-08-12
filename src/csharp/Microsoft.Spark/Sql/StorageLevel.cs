// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Net;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// TODO:
    /// Missing APIs:
    /// Persist() with "StorageLevel"

    /// <summary>
    ///  A distributed collection of data organized into named columns.
    /// </summary>
    public sealed class StorageLevel : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;
        private readonly bool _useDisk;
        private readonly bool _useMemory;
        private readonly bool _useOffHeap;
        private readonly bool _deserialized;
        private readonly int _replication;

        internal StorageLevel(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        public StorageLevel(
            bool useDisk,
            bool useMemory,
            bool useOffHeap,
            bool deserialized,
            int replication = 1)
        {
            _useDisk = useDisk;
            _useMemory = useMemory;
            _useOffHeap = useOffHeap;
            _deserialized = deserialized;
            _replication = replication;
        }
    }

}
