// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// <see cref="DataStreamWriter.ForeachBatch(Action{DataFrame, long})"/> callback handler.
    /// </summary>
    internal sealed class ForeachBatchCallbackHandler : ICallbackHandler
    {
        private readonly IJvmBridge _jvm;

        private readonly Action<DataFrame, long> _func;

        internal ForeachBatchCallbackHandler(IJvmBridge jvm, Action<DataFrame, long> func)
        {
            _jvm = jvm;
            _func = func;
        }

        public void Run(Stream inputStream)
        {
            var batchDf =
                new DataFrame(new JvmObjectReference(SerDe.ReadString(inputStream), _jvm));
            long batchId = SerDe.ReadInt64(inputStream);

            _func(batchDf, batchId);
        }
    }
}
