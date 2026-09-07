// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Worker.Command
{
    internal enum ArrowOutputPhase
    {
        NotStarted,
        Writing,
        AtBatchBoundary,
        Ended,
        Faulted
    }

    /// <summary>
    /// Tracks output framing for one TaskRunner.ProcessStream invocation.
    /// The Arrow session owns Phase; TaskRunner owns the success trailer.
    /// </summary>
    internal sealed class ArrowOutputContext
    {
        internal ArrowOutputPhase Phase { get; set; }

        internal bool SuccessTailStarted { get; private set; }

        internal bool CanWriteException =>
            !SuccessTailStarted &&
            (Phase == ArrowOutputPhase.NotStarted || Phase == ArrowOutputPhase.Ended);

        internal void BeginSuccessTail() => SuccessTailStarted = true;
    }
}
