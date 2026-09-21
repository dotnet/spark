# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

param([Parameter(Mandatory)][string]$JavaHome)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'Invoke-RealFileDownloadProbe.ps1') -JavaHome $JavaHome

function Assert-Probe([bool]$Condition, [string]$Name)
{
    if (-not $Condition) { throw "Probe validation failed: $Name" }
}

$valid = @(
    'probe_start spark=4.0.3 java_feature=17 iterations=2 concurrency=1',
    'probe_result mode=immediate_read completed_expected_errors=2 concurrency=1 elapsed_ms=1',
    'probe_result mode=read_after_error completed_expected_errors=2 concurrency=1 elapsed_ms=1',
    'probe_complete unix=4 inet=0 unknown=0 active=0'
)
$cases = [Collections.Generic.List[object]]::new()
$cases.Add(@{ Name = 'valid'; Lines = $valid; Expected = $true })
$cases.Add(@{ Name = 'missing-mode'; Lines = @($valid[0], $valid[2], $valid[3]); Expected = $false })
$cases.Add(@{ Name = 'duplicate-mode'; Lines = @($valid[0], $valid[1], $valid[1], $valid[3]); Expected = $false })
$cases.Add(@{ Name = 'duplicate-complete'; Lines = $valid + $valid[3]; Expected = $false })
$cases.Add(@{ Name = 'missing-complete'; Lines = $valid[0..2]; Expected = $false })
$cases.Add(@{ Name = 'unknown-line'; Lines = $valid + 'unexpected'; Expected = $false })
$cases.Add(@{ Name = 'error-after-complete'; Lines = $valid + 'probe_diagnostic_error type=other'; Expected = $false })
foreach ($replacement in @(
    @('wrong-version', 0, 'spark=4.0.3', 'spark=4.0.4'),
    @('wrong-iterations', 0, 'iterations=2', 'iterations=3'),
    @('wrong-concurrency', 1, 'concurrency=1', 'concurrency=2'),
    @('wrong-mode-count', 1, 'completed_expected_errors=2', 'completed_expected_errors=1'),
    @('wrong-total', 3, 'unix=4', 'unix=3'),
    @('unknown-family', 3, 'unknown=0', 'unknown=1'),
    @('active-reader', 3, 'active=0', 'active=1')))
{
    $lines = [string[]]$valid.Clone()
    $index = [int]$replacement[1]
    $lines[$index] = $lines[$index].Replace($replacement[2], $replacement[3])
    $cases.Add(@{ Name = $replacement[0]; Lines = $lines; Expected = $false })
}
foreach ($case in $cases)
{
    $state = New-ProbeProtocolState -Version '4.0.3' -Iterations 2 -Concurrency 1
    foreach ($line in $case.Lines) { [void](Add-ProbeProtocolLine -State $state -Line $line) }
    Assert-Probe ((Test-ProbeProtocolComplete $state) -eq $case.Expected) $case.Name
}
Write-Host "protocol_cases=$($cases.Count) passed=true"

$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$directory = Join-Path $repository ('artifacts/real-file-download-probe-tests/' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $directory | Out-Null
$log = Join-Path $directory 'runner-tests.log'
$writer = [IO.StreamWriter]::new($log)
$survivor = [Diagnostics.Process]::new()
$survivorStarted = $false
try
{
    $compiler = Invoke-OwnedProbeJava -Executable (Join-Path $JavaHome 'bin/javac.exe') -Stage 'compile' `
        -TimeoutSeconds 30 -Writer $writer -Arguments @('-d', $directory, (Join-Path $PSScriptRoot 'tests/ProbeProcessFixture.java'))
    Assert-Probe ($compiler.ExitCode -eq 0 -and -not $compiler.TimedOut -and
        $compiler.unknown_stdout -eq 0 -and $compiler.unknown_stderr -eq 0) 'fixture-compilation'

    $java = Join-Path $JavaHome 'bin/java.exe'
    $survivor.StartInfo = [Diagnostics.ProcessStartInfo]::new($java)
    $survivor.StartInfo.UseShellExecute = $false
    $survivor.StartInfo.CreateNoWindow = $true
    foreach ($argument in @('-cp', $directory, 'ProbeProcessFixture', 'wait'))
    { $survivor.StartInfo.ArgumentList.Add($argument) }
    $survivorStarted = $survivor.Start()
    Assert-Probe $survivorStarted 'survivor-start'

    $timeout = Invoke-OwnedProbeJava -Executable $java -Stage 'timeout_test' -TimeoutSeconds 1 `
        -Writer $writer -Arguments @('-cp', $directory, 'ProbeProcessFixture', 'wait')
    Assert-Probe ($timeout.TimedOut -and $timeout.Exited -and $timeout.ExitCode -ne 0 -and
        $timeout.OwnedProcessId -ne $survivor.Id -and -not $survivor.HasExited) 'owned-child-timeout'

    $state = New-ProbeProtocolState -Version '4.0.3' -Iterations 2 -Concurrency 1
    $noise = Invoke-OwnedProbeJava -Executable $java -Stage 'runtime' -TimeoutSeconds 30 `
        -Writer $writer -Protocol $state -Arguments @('-cp', $directory, 'ProbeProcessFixture', 'noise')
    Assert-Probe ($noise.ExitCode -eq 0 -and $noise.unknown_stdout -eq 2 -and
        $noise.unknown_stderr -eq 1 -and -not (Test-ProbeProtocolComplete $state)) 'unknown-output-rejected'
}
finally
{
    # Both processes belong to this test; never terminate another Java process by name.
    if ($survivorStarted -and -not $survivor.HasExited)
    { $survivor.Kill(); [void]$survivor.WaitForExit(5000) }
    $survivor.Dispose()
    $writer.Dispose()
}
Assert-Probe (-not ((Get-Content -LiteralPath $log -Raw).Contains('synthetic-sensitive-value'))) 'redaction'
Write-Host 'owned_child_timeout=true unrelated_child_survived=true redaction=true'
