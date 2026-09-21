# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

[CmdletBinding()]
param(
    [string]$SparkHome = $env:SPARK_HOME,
    [string]$SparkVersion,
    [string]$JavaHome = $env:JAVA_HOME,
    [int]$Iterations = 10000,
    [int]$Concurrency = 4,
    [int]$CompileTimeoutSeconds = 60,
    [int]$RuntimeTimeoutSeconds = 150,
    [string]$OutputDirectory
)

function New-ProbeProtocolState([string]$Version, [int]$Iterations, [int]$Concurrency)
{
    return @{ Version = $Version; Iterations = $Iterations; Concurrency = $Concurrency;
        Next = 0; Invalid = $false }
}

function Add-ProbeProtocolLine([hashtable]$State, [string]$Line)
{
    if ($Line -cmatch '^probe_start spark=(4\.0\.[0-9]+) java_feature=17 iterations=([0-9]{1,5}) concurrency=([1-8])$')
    {
        if ($State.Next -ne 0 -or $Matches[1] -cne $State.Version -or
            [int]$Matches[2] -ne $State.Iterations -or [int]$Matches[3] -ne $State.Concurrency)
        { $State.Invalid = $true }
        $State.Next = 1
        return $true
    }
    if ($Line -cmatch '^probe_result mode=(immediate_read|read_after_error) completed_expected_errors=([0-9]{1,5}) concurrency=([1-8]) elapsed_ms=([0-9]{1,12})$')
    {
        $expectedMode = if ($State.Next -eq 1) { 'immediate_read' } else { 'read_after_error' }
        if ($State.Next -notin @(1, 2) -or $Matches[1] -cne $expectedMode -or
            [int]$Matches[2] -ne $State.Iterations -or [int]$Matches[3] -ne $State.Concurrency)
        { $State.Invalid = $true }
        $State.Next++
        return $true
    }
    if ($Line -cmatch '^probe_complete unix=([0-9]{1,5}) inet=([0-9]{1,5}) unknown=([0-9]{1,5}) active=([0-9]{1,5})$')
    {
        if ($State.Next -ne 3 -or ([int]$Matches[1] + [int]$Matches[2]) -ne (2 * $State.Iterations) -or
            [int]$Matches[3] -ne 0 -or [int]$Matches[4] -ne 0)
        { $State.Invalid = $true }
        $State.Next = 4
        return $true
    }
    $diagnostics = @(
        '^probe_(?:operation_error|diagnostic_error) type=[A-Za-z0-9_.$<>]{1,200}$',
        '^probe_stop reason=(?:invalid_arguments|runtime_mismatch|process_deadline|batch_deadline|unexpected_outcome|setup_or_cleanup_failure|diagnostic_failure) active=[0-9]{1,5}$',
        '^reader id=[0-9]{1,12} request=[0-9]{1,5} mode=(?:immediate_read|read_after_error) phase=(?:opening|waiting_error|reading|validating|closing)(?: open=(?:true|false) source_error=(?:true|false) family=[012])? elapsed_ms=[0-9]{1,12}$',
        '^frame=[A-Za-z0-9_.$<>]{1,400} line=-?[0-9]{1,10}$'
    )
    foreach ($pattern in $diagnostics)
    {
        if ($Line -cmatch $pattern) { $State.Invalid = $true; return $true }
    }
    $State.Invalid = $true
    return $false
}

function Test-ProbeProtocolComplete([hashtable]$State)
{
    return ($State.Next -eq 4 -and -not $State.Invalid)
}

function Write-ProbeChildLine($State, $Stream, [string]$Line, [bool]$Oversized)
{
    $known = -not $Oversized -and $State.Stage -eq 'runtime' -and $Stream.Name -eq 'stdout'
    if ($known) { $known = Add-ProbeProtocolLine -State $State.Protocol -Line $Line }
    if (-not $known)
    {
        $State[('unknown_' + $Stream.Name)]++
        return
    }
    if ($State.Written -ge 1024) { $State.Dropped++; return }
    $State.Writer.WriteLine($Line)
    $State.Writer.Flush()
    Write-Host $Line
    $State.Written++
}

function Invoke-OwnedProbeJava(
    [string]$Executable, [string[]]$Arguments, [string]$Stage,
    [int]$TimeoutSeconds, [IO.StreamWriter]$Writer, [hashtable]$Protocol)
{
    $state = @{ Stage = $Stage; Writer = $Writer; Protocol = $Protocol; Written = 0;
        unknown_stdout = 0L; unknown_stderr = 0L; Dropped = 0L; TimedOut = $false;
        ExitCode = -1; OwnedProcessId = 0; Exited = $false }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = [Diagnostics.ProcessStartInfo]::new($Executable)
    $process.StartInfo.UseShellExecute = $false
    $process.StartInfo.CreateNoWindow = $true
    $process.StartInfo.RedirectStandardOutput = $true
    $process.StartInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $process.StartInfo.ArgumentList.Add($argument) }
    # These changes belong only to this child; never modify the caller's environment.
    foreach ($name in @('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'CLASSPATH'))
    { [void]$process.StartInfo.Environment.Remove($name) }
    $started = $false
    try
    {
        $started = $process.Start()
        if (-not $started) { throw 'child-start-failed' }
        $state.OwnedProcessId = $process.Id
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $streams = foreach ($name in @('stdout', 'stderr'))
        {
            $reader = if ($name -eq 'stdout') { $process.StandardOutput } else { $process.StandardError }
            $buffer = [char[]]::new(4096)
            @{ Name = $name; Reader = $reader; Buffer = $buffer;
                Pending = $reader.ReadAsync($buffer, 0, $buffer.Length);
                Text = [Text.StringBuilder]::new(); Oversized = $false; Done = $false }
        }
        $drainDeadline = [long]::MaxValue
        while (-not $process.HasExited -or @($streams | Where-Object { -not $_.Done }).Count -gt 0)
        {
            if (-not $state.TimedOut -and $watch.Elapsed.TotalSeconds -ge $TimeoutSeconds)
            {
                $state.TimedOut = $true
                # Kill only this owned JVM. No name matching or process-tree termination.
                if (-not $process.HasExited) { $process.Kill(); [void]$process.WaitForExit(5000) }
                $drainDeadline = $watch.ElapsedMilliseconds + 2000
            }
            if ($watch.ElapsedMilliseconds -ge $drainDeadline) { break }
            foreach ($stream in $streams)
            {
                if ($stream.Done -or -not $stream.Pending.IsCompleted) { continue }
                $count = $stream.Pending.GetAwaiter().GetResult()
                if ($count -eq 0)
                {
                    if ($stream.Text.Length -gt 0 -or $stream.Oversized)
                    { Write-ProbeChildLine $state $stream $stream.Text.ToString().TrimEnd([char]13) $stream.Oversized }
                    $stream.Done = $true
                    continue
                }
                for ($index = 0; $index -lt $count; $index++)
                {
                    $character = $stream.Buffer[$index]
                    if ($character -eq [char]10)
                    {
                        Write-ProbeChildLine $state $stream $stream.Text.ToString().TrimEnd([char]13) $stream.Oversized
                        [void]$stream.Text.Clear()
                        $stream.Oversized = $false
                    }
                    elseif ($stream.Text.Length -lt 2048) { [void]$stream.Text.Append($character) }
                    else { $stream.Oversized = $true }
                }
                $stream.Pending = $stream.Reader.ReadAsync($stream.Buffer, 0, $stream.Buffer.Length)
            }
            Start-Sleep -Milliseconds 5
        }
        $state.Exited = $process.HasExited
        if ($state.Exited) { $state.ExitCode = $process.ExitCode }
    }
    finally
    {
        if ($started -and -not $process.HasExited)
        { $process.Kill(); $state.Exited = $process.WaitForExit(5000) }
        $process.Dispose()
        $summary = "runner_process stage=$Stage exit=$($state.ExitCode) timeout=$($state.TimedOut.ToString().ToLowerInvariant()) " +
            "unknown_stdout=$($state.unknown_stdout) unknown_stderr=$($state.unknown_stderr) omitted=$($state.Dropped)"
        $Writer.WriteLine($summary)
        $Writer.Flush()
        Write-Host $summary
    }
    return $state
}

# Dot-sourcing loads the pure validators for focused tests without launching a JVM.
if ($MyInvocation.InvocationName -eq '.') { return }

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$stage = 'validate_arguments'
$writer = $null
try
{
    if (-not $IsWindows -or $SparkVersion -cnotmatch '^4\.0\.[0-9]{1,3}$' -or
        $Iterations -lt 1 -or $Iterations -gt 10000 -or $Concurrency -lt 1 -or $Concurrency -gt 8 -or
        $CompileTimeoutSeconds -lt 1 -or $CompileTimeoutSeconds -gt 300 -or
        $RuntimeTimeoutSeconds -lt 1 -or $RuntimeTimeoutSeconds -gt 300 -or
        [string]::IsNullOrWhiteSpace($SparkHome) -or [string]::IsNullOrWhiteSpace($JavaHome))
    { throw 'invalid-arguments' }
    $repositoryRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
    if ([string]::IsNullOrWhiteSpace($OutputDirectory))
    { $OutputDirectory = Join-Path $repositoryRoot ('artifacts/real-file-download-probe/' + [Guid]::NewGuid().ToString('N')) }
    $runDirectory = [IO.Path]::GetFullPath($OutputDirectory)
    $sparkDirectory = (Get-Item -LiteralPath $SparkHome).FullName
    if ($runDirectory.Contains([IO.Path]::PathSeparator) -or $sparkDirectory.Contains([IO.Path]::PathSeparator))
    { throw 'classpath-separator-in-path' }
    if (Test-Path -LiteralPath $runDirectory) { throw 'fresh-output-directory-required' }
    New-Item -ItemType Directory -Path $runDirectory | Out-Null
    $classes = New-Item -ItemType Directory -Path (Join-Path $runDirectory 'classes')
    $logs = New-Item -ItemType Directory -Path (Join-Path $runDirectory 'logs')
    $writer = [IO.StreamWriter]::new((Join-Path $logs.FullName 'probe.log'), $false, [Text.UTF8Encoding]::new($false))

    $stage = 'validate_runtime'
    $javaExecutable = Join-Path $JavaHome 'bin/java.exe'
    $release = Get-Content -LiteralPath (Join-Path $sparkDirectory 'RELEASE') -First 1
    if ($release -cnotmatch '^Spark\s+(4\.0\.[0-9]+)(?:\s|$)' -or $Matches[1] -cne $SparkVersion)
    { throw 'spark-version-mismatch' }
    $javaReleaseLines = Get-Content -LiteralPath (Join-Path $JavaHome 'release')
    $javaRelease = $javaReleaseLines |
        Where-Object { $_ -cmatch '^JAVA_VERSION="(17(?:\.[0-9]+){0,3}(?:\+[0-9]+)?)"$' }
    $javaRuntime = $javaReleaseLines |
        Where-Object { $_ -cmatch '^JAVA_RUNTIME_VERSION="(17[0-9A-Za-z.+_-]{0,79})"$' }
    if (@($javaRelease).Count -ne 1 -or -not (Test-Path -LiteralPath $javaExecutable -PathType Leaf))
    { throw 'java-17-required' }
    if (@($javaRuntime).Count -ne 1) { throw 'java-runtime-identity-required' }
    $javaVersion = [regex]::Match($javaRuntime, '"([^"]+)"').Groups[1].Value
    $jars = Join-Path $sparkDirectory 'jars'
    $compiler = @(Get-ChildItem -LiteralPath $jars -Filter 'scala-compiler-*.jar' -File)
    $sparkCore = @(Get-ChildItem -LiteralPath $jars -Filter "spark-core_2.13-$SparkVersion.jar" -File)
    if ($compiler.Count -ne 1 -or $sparkCore.Count -ne 1 -or
        @(Get-ChildItem -LiteralPath $jars -Filter 'microsoft-spark*.jar' -File).Count -ne 0)
    { throw 'unexpected-distribution-jars' }
    $source = Join-Path $PSScriptRoot 'RealFileDownloadProbe.scala'
    $fingerprint = "runner_identity spark=$SparkVersion java_runtime=$javaVersion os_version=$([Environment]::OSVersion.Version) " +
        "java_sha256=$((Get-FileHash -LiteralPath $javaExecutable -Algorithm SHA256).Hash) " +
        "spark_core_sha256=$((Get-FileHash -LiteralPath $sparkCore[0].FullName -Algorithm SHA256).Hash) " +
        "scala_compiler_sha256=$((Get-FileHash -LiteralPath $compiler[0].FullName -Algorithm SHA256).Hash) " +
        "source_sha256=$((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash)"
    $writer.WriteLine($fingerprint)
    $writer.Flush()
    Write-Host $fingerprint

    $stage = 'compile'
    $distributionClasspath = Join-Path $jars '*'
    $compiled = Invoke-OwnedProbeJava -Executable $javaExecutable -Stage $stage -TimeoutSeconds $CompileTimeoutSeconds -Writer $writer -Arguments @(
        '-cp', $distributionClasspath, 'scala.tools.nsc.Main', '-usejavacp', '-deprecation', '-feature', '-unchecked', '-Xlint',
        '-d', $classes.FullName, $source)
    if ($compiled.ExitCode -ne 0 -or $compiled.TimedOut -or -not $compiled.Exited -or
        $compiled.unknown_stdout -ne 0 -or $compiled.unknown_stderr -ne 0 -or $compiled.Dropped -ne 0)
    { throw 'compile-failed' }

    $stage = 'runtime'
    $protocol = New-ProbeProtocolState -Version $SparkVersion -Iterations $Iterations -Concurrency $Concurrency
    $result = Invoke-OwnedProbeJava -Executable $javaExecutable -Stage $stage -TimeoutSeconds $RuntimeTimeoutSeconds -Writer $writer -Protocol $protocol -Arguments @(
        '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
        '-cp', ($distributionClasspath + [IO.Path]::PathSeparator + $classes.FullName),
        'org.apache.spark.RealFileDownloadProbe', $SparkVersion, [string]$Iterations, [string]$Concurrency)
    if ($result.ExitCode -ne 0 -or $result.TimedOut -or -not $result.Exited -or
        $result.unknown_stdout -ne 0 -or $result.unknown_stderr -ne 0 -or $result.Dropped -ne 0 -or
        -not (Test-ProbeProtocolComplete $protocol))
    { throw 'runtime-failed' }
    $writer.WriteLine('runner_complete success=true')
    Write-Host 'runner_complete success=true'
}
catch
{
    # Never print an exception message or a rejected child line: either can contain secrets.
    $failure = "runner_stop stage=$stage"
    if ($null -ne $writer) { $writer.WriteLine($failure); $writer.Flush() }
    Write-Host $failure
    exit 1
}
finally
{
    if ($null -ne $writer) { $writer.Dispose() }
}
