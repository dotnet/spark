// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.E2ETest
{
    /// <summary>
    /// Debug-branch diagnostics only. Captures stacks before VSTest's five-minute hang guard.
    /// Does not retry, interrupt, or change the timeout of the operation being observed.
    /// </summary>
    internal sealed class E2EHangDiagnostics : IDisposable
    {
        private sealed class ActiveTest
        {
            internal readonly Stopwatch Elapsed = Stopwatch.StartNew();
        }

        private static ActiveTest s_activeTest;
        private readonly BlockingCollection<string> _lines = new BlockingCollection<string>(4096);
        private readonly ManualResetEventSlim _stop = new ManualResetEventSlim();
        private readonly IpcDebugTrace _trace;
        private readonly Thread _thread;
        private readonly Action _capture;
        private readonly TimeSpan _delay;
        private readonly TimeSpan _interval;
        private int _dropped;
        private int _jvmPid;
        private long _jvmStartTime;

        internal E2EHangDiagnostics(
            string directory,
            Action capture = null,
            TimeSpan? delay = null,
            TimeSpan? interval = null)
        {
            _trace = new IpcDebugTrace(directory, maxBytes: 4 * 1024 * 1024);
            _capture = capture ?? CaptureJvm;
            _delay = delay ?? TimeSpan.FromMinutes(2);
            _interval = interval ?? TimeSpan.FromSeconds(10);
            _thread = new Thread(Observe) { IsBackground = true, Name = "E2E hang diagnostics" };
            _thread.Start();
        }

        internal static E2EHangDiagnostics Create()
        {
            string directory = Environment.GetEnvironmentVariable("DOTNET_SPARK_DEBUG_TRACE_DIR");
            try
            {
                return string.IsNullOrWhiteSpace(directory) ? null : new E2EHangDiagnostics(directory);
            }
            catch (Exception exception)
            {
                IpcDebugTrace.Write($"diagnostic-create-failed {exception.GetType().Name}");
                return null;
            }
        }

        internal static void BeginTest() => Volatile.Write(ref s_activeTest, new ActiveTest());

        internal static void EndTest() => Volatile.Write(ref s_activeTest, null);

        internal void ObserveJvm(IJvmBridge jvm)
        {
            try
            {
                // Ask this fixture's JVM for its identity; never attach to arbitrary agent JVMs.
                // Spark's public helper avoids reflecting on JDK 17's non-exported RuntimeImpl.
                string name = (string)jvm.CallStaticJavaMethod(
                    "org.apache.spark.util.Utils", "getProcessName");
                if (int.TryParse(name.Split('@')[0], out int pid) && pid > 0)
                {
                    ObserveProcess(pid);
                }
            }
            catch (Exception exception)
            {
                IpcDebugTrace.Write($"diagnostic-jvm-identity-failed {exception.GetType().Name}");
            }
        }

        private void ObserveProcess(int pid)
        {
            using Process process = Process.GetProcessById(pid);
            _jvmStartTime = process.StartTime.ToUniversalTime().Ticks;
            Volatile.Write(ref _jvmPid, pid);
            IpcDebugTrace.Write($"fixture-jvm-pid={pid}");
        }

        internal void RecordOutput(string source, string line)
        {
            if (line == null || _stop.IsSet)
            {
                return;
            }

            string sanitized = Sanitize(line);
            // No disk I/O or waiting in the Spark stdout/stderr callbacks.
            if (sanitized != null && !_lines.TryAdd($"{DateTime.UtcNow:O} {source} {sanitized}"))
            {
                Interlocked.Increment(ref _dropped);
            }
        }

        internal static string Sanitize(string line)
        {
            if (line.Length > 4096)
            {
                return "line-omitted-too-long";
            }

            string value = line.Trim();
            // Persist structure only, not messages, thread names, command lines, URLs or data.
            string[] patterns =
            {
                @"^at [\w.$/@<>-]+\((?:[\w.$@+-]+/)?(?:[\w.$]+\.(?:java|scala):\d+|Native Method|Unknown Source)\)$",
                @"^java\.lang\.Thread\.State: [A-Z_]+(?: \([A-Za-z ]+\))?$",
                @"^- (?:locked|waiting to lock|waiting on|parking to wait for)\s+<0x[0-9a-fA-F]+>(?: \(a [\w.$]+\))?$",
                @"^- <0x[0-9a-fA-F]+> \(a [\w.$]+\)$",
                @"^Locked ownable synchronizers:$",
                @"^- None$",
                @"^Found (?:one|\d+) (?:Java-level )?deadlocks?:?$"
            };
            if (patterns.Any(pattern => Regex.IsMatch(value, pattern)))
            {
                return value;
            }

            Match exception = Regex.Match(
                value, @"^(?:Caused by: |Suppressed: )?([\w.$]+(?:Exception|Error))(?::|$)");
            if (exception.Success)
            {
                return $"exception={exception.Groups[1].Value} message-omitted";
            }

            Match log = Regex.Match(value, @"(?:^|\s)(WARN|ERROR)\s+([\w.$]+):");
            if (log.Success)
            {
                return $"{log.Groups[1].Value} {log.Groups[2].Value} message-omitted";
            }

            if (value.StartsWith("\"", StringComparison.Ordinal))
            {
                int end = value.IndexOf('"', 1);
                if (end > 0)
                {
                    string fields = string.Join(" ", Regex.Matches(value.Substring(end + 1),
                        @"\b(?:tid|nid)=0x[0-9a-fA-F]+\b|\b(?:prio|os_prio)=-?\d+\b|\b(?:cpu|elapsed)=[\d.]+m?s\b")
                        .Select(match => match.Value));
                    return fields.Length == 0 ? null : $"thread {fields}";
                }
            }

            return null;
        }

        private void Observe()
        {
            ActiveTest observed = null;
            int captures = 0;
            var sinceCapture = new Stopwatch();
            _trace.WriteEvent("diagnostic-policy allow=stack-frames,locks,states,resource-counters " +
                "omit=messages,thread-names,arguments,environment,urls,data memory-dumps=disabled");
            try
            {
                while (!_stop.Wait(100))
                {
                    Drain();
                    ActiveTest active = Volatile.Read(ref s_activeTest);
                    if (!ReferenceEquals(active, observed))
                    {
                        observed = active;
                        captures = 0;
                    }

                    if (active != null && captures < 2 && active.Elapsed.Elapsed >= _delay &&
                        (captures == 0 || sinceCapture.Elapsed >= _interval))
                    {
                        _trace.WriteEvent($"snapshot-begin number={++captures}");
                        try
                        {
                            using Process testhost = Process.GetCurrentProcess();
                            _trace.WriteEvent($"testhost cpu-ms={testhost.TotalProcessorTime.TotalMilliseconds:F0} " +
                                $"working-set={testhost.WorkingSet64} threads={testhost.Threads.Count}");
                            _capture();
                        }
                        catch (Exception exception)
                        {
                            _trace.WriteEvent($"snapshot-failed {exception.GetType().Name}");
                        }

                        _trace.WriteEvent("snapshot-end");
                        sinceCapture.Restart();
                    }
                }

                Drain(4096);
            }
            catch (Exception exception)
            {
                IpcDebugTrace.Write($"diagnostic-observer-failed {exception.GetType().Name}");
            }
        }

        private void CaptureJvm()
        {
            int pid = Volatile.Read(ref _jvmPid);
            string javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
            string tool = string.IsNullOrWhiteSpace(javaHome) ? null : Path.Combine(
                javaHome, "bin", OperatingSystem.IsWindows() ? "jcmd.exe" : "jcmd");
            if (pid == 0 || tool == null || !File.Exists(tool))
            {
                _trace.WriteEvent("jcmd-unavailable-or-jvm-not-identified");
                return;
            }

            using Process jvm = Process.GetProcessById(pid);
            if (jvm.StartTime.ToUniversalTime().Ticks != _jvmStartTime)
            {
                _trace.WriteEvent("jvm-process-generation-changed capture-skipped");
                return;
            }

            _trace.WriteEvent($"jvm pid={pid} cpu-ms={jvm.TotalProcessorTime.TotalMilliseconds:F0} " +
                $"working-set={jvm.WorkingSet64} threads={jvm.Threads.Count}");
            using var command = new Process();
            command.StartInfo = new ProcessStartInfo(tool)
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            command.StartInfo.ArgumentList.Add(pid.ToString());
            command.StartInfo.ArgumentList.Add("Thread.print");
            command.StartInfo.ArgumentList.Add("-l");
            int closedStreams = 0;
            command.OutputDataReceived += (_, args) =>
            {
                if (args.Data == null)
                {
                    Interlocked.Increment(ref closedStreams);
                }
                else
                {
                    RecordOutput("jvm-stack", args.Data);
                }
            };
            command.ErrorDataReceived += (_, args) =>
            {
                if (args.Data == null)
                {
                    Interlocked.Increment(ref closedStreams);
                }
                else
                {
                    RecordOutput("jcmd-error", args.Data);
                }
            };
            command.Start();
            command.BeginOutputReadLine();
            command.BeginErrorReadLine();
            var elapsed = Stopwatch.StartNew();
            // Process exit alone does not guarantee that both redirected streams reached EOF.
            while (!command.WaitForExit(100) || Volatile.Read(ref closedStreams) != 2)
            {
                Drain();
                if (_stop.IsSet || elapsed.Elapsed > TimeSpan.FromSeconds(15))
                {
                    // Only stop our diagnostic command, never the JVM or testhost.
                    if (!command.HasExited)
                    {
                        command.Kill();
                    }
                    _trace.WriteEvent("jcmd-stopped timeout-or-fixture-disposed");
                    return;
                }

                _stop.Wait(50);
            }

            _trace.WriteEvent($"jcmd-exit code={command.ExitCode}");
            Drain(4096);
        }

        private void Drain(int maxLines = 512)
        {
            // Bound each drain so a noisy process cannot starve the snapshot timer.
            for (int i = 0; i < maxLines && _lines.TryTake(out string line); ++i)
            {
                _trace.WriteEvent(line);
            }

            int dropped = Interlocked.Exchange(ref _dropped, 0);
            if (dropped > 0)
            {
                _trace.WriteEvent($"diagnostic-lines-dropped={dropped}");
            }
        }

        public void Dispose()
        {
            _stop.Set();
            // Diagnostic shutdown must not add an unbounded wait to fixture cleanup.
            _thread.Join(TimeSpan.FromSeconds(2));
        }
    }
}
