// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Spark.Sql
{
    public interface IForeachWriter
    {
        public bool Open(long partitionId, long epochId);

        public void Process(Row value);

        public void Close(Exception errorOrNull);
    }

    public class ForeachWriterWrapper
    {
        private readonly IForeachWriter _foreachWriter;

        public ForeachWriterWrapper(IForeachWriter foreachWriter) =>
            _foreachWriter = foreachWriter;

        public long EpochId { get; set; } = long.MinValue;

        public IEnumerable<object> Execute(int partitionId, IEnumerable<object> input)
        {
            Exception error = null;

            try
            {
                if (_foreachWriter.Open(partitionId, EpochId))
                {
                    foreach (object o in input)
                    {
                        if (o is object[] unpickledObjects)
                        {
                            foreach (object unpickled in unpickledObjects)
                            {
                                _foreachWriter.Process((unpickled as RowConstructor).GetRow());
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                error = e;
            }
            finally
            {
                _foreachWriter.Close(error);

                if (error != null)
                {
                    throw error;
                }
            }

            return Enumerable.Empty<object>();
        }
    }
}
