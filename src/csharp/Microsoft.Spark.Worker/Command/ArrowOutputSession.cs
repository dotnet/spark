// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// Writes prepared batches and closes Arrow framing only at a complete batch boundary.
    /// A failed write makes the connection unusable, regardless of the exception type.
    /// </summary>
    internal sealed class ArrowOutputSession
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(ArrowOutputSession));

        private readonly Stream _output;
        private readonly IpcOptions _ipcOptions;
        private readonly ArrowOutputContext _context;
        private ArrowStreamWriter _writer;

        internal ArrowOutputSession(
            Stream output,
            IpcOptions ipcOptions,
            ArrowOutputContext context)
        {
            _output = output ?? throw new ArgumentNullException(nameof(output));
            _ipcOptions = ipcOptions ?? throw new ArgumentNullException(nameof(ipcOptions));
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        internal void Write(IEnumerable<PreparedArrowBatch> batches)
        {
            if (_context.Phase != ArrowOutputPhase.NotStarted || _context.SuccessTailStarted)
            {
                throw new InvalidOperationException("An Arrow output session cannot be restarted.");
            }

            Exception failure = null;
            IEnumerator<PreparedArrowBatch> iterator = null;
            try
            {
                iterator = batches.GetEnumerator();
                while (iterator.MoveNext())
                {
                    PreparedArrowBatch prepared = iterator.Current;
                    try
                    {
                        if (prepared == null)
                        {
                            throw new InvalidDataException("Arrow output contains a null batch.");
                        }

                        WriteBatch(prepared.Batch);
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                    }
                    finally
                    {
                        Cleanup(prepared, ref failure);
                    }

                    if (failure != null)
                    {
                        break;
                    }
                }

                if (failure == null && _context.Phase == ArrowOutputPhase.NotStarted)
                {
                    throw new InvalidDataException("Arrow output did not provide a result schema.");
                }
            }
            catch (Exception ex)
            {
                KeepFirstFailure(ex, ref failure);
            }
            finally
            {
                // Iterator cleanup is preparation work too: an intact stream can still end
                // before the original failure is reported through the outer task protocol.
                Cleanup(iterator, ref failure);
                if (_context.Phase == ArrowOutputPhase.AtBatchBoundary)
                {
                    try
                    {
                        WriteEnd();
                    }
                    catch (Exception ex)
                    {
                        KeepFirstFailure(ex, ref failure);
                    }
                }

                // Arrow 14 Dispose does not write EOS. The task retains socket ownership.
                Cleanup(_writer, ref failure);
                _writer = null;
            }

            if (failure != null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }

        private void WriteBatch(RecordBatch batch)
        {
            // Construction and the complete first batch must succeed before START is sent.
            bool starting = _context.Phase == ArrowOutputPhase.NotStarted;
            if (starting)
            {
                _writer = new ArrowStreamWriter(
                    _output, batch.Schema, leaveOpen: true, _ipcOptions);
            }

            _context.Phase = ArrowOutputPhase.Writing;
            try
            {
                if (starting)
                {
                    SerDe.Write(_output, (int)SpecialLengths.START_ARROW_STREAM);
                }

                _writer.WriteRecordBatch(batch);
                _context.Phase = ArrowOutputPhase.AtBatchBoundary;
            }
            catch
            {
                _context.Phase = ArrowOutputPhase.Faulted;
                throw;
            }
        }

        private void WriteEnd()
        {
            _context.Phase = ArrowOutputPhase.Writing;
            try
            {
                if (!_ipcOptions.WriteLegacyIpcFormat)
                {
                    SerDe.Write(_output, -1);
                }

                SerDe.Write(_output, 0);
                _context.Phase = ArrowOutputPhase.Ended;
            }
            catch
            {
                _context.Phase = ArrowOutputPhase.Faulted;
                throw;
            }
        }

        private static void Cleanup(IDisposable resource, ref Exception failure)
        {
            try
            {
                resource?.Dispose();
            }
            catch (Exception ex)
            {
                KeepFirstFailure(ex, ref failure);
            }
        }

        private static void KeepFirstFailure(Exception error, ref Exception failure)
        {
            if (failure == null)
            {
                failure = error;
            }
            else
            {
                try
                {
                    s_logger.LogError($"Arrow output cleanup failed: {error}");
                }
                catch (Exception)
                {
                    // Logging a secondary failure must not replace the original exception.
                }
            }
        }
    }
}
