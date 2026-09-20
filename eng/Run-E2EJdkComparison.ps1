# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

<#
.SYNOPSIS
Runs paired Windows Spark 4 E2E sessions against two local JDK 17 installations.
.DESCRIPTION
Requires prebuilt net8.0 E2E/worker outputs and one Spark 4 bridge JAR. Nothing is
built or downloaded. Existing Ivy settings, repositories and extra Spark arguments
are inherited unchanged. Intentional skips are allowed; aborted, empty, failed or
incomplete runs fail the comparison. Logs stay in unique local result directories.
.EXAMPLE
./eng/Run-E2EJdkComparison.ps1 -JavaHomeA C:/jdks/17.0.17 -JavaHomeB C:/jdks/17.0.18 -SparkHome C:/spark/spark-4.0.4-bin-hadoop3 -SparkVersion 4.0.4 -HadoopHome C:/winutils -Sessions 2
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$JavaHomeA,
    [Parameter(Mandatory)][string]$JavaHomeB,
    [Parameter(Mandatory)][string]$SparkHome,
    [Parameter(Mandatory)][ValidatePattern('^4\.0\.\d+$')][string]$SparkVersion,
    [Parameter(Mandatory)][string]$HadoopHome,
    [ValidateSet('Debug', 'Release')][string]$Configuration = 'Debug',
    [ValidateRange(1, 20)][int]$Sessions = 1,
    [ValidateNotNullOrEmpty()][string]$TestFilter = 'Category!=HangStress',
    [string]$ResultsDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

# This opt-in experiment never builds, downloads, changes machine configuration,
# retries failed tests, or terminates processes. Run it from an idle Windows host.
function Assert-FixturePortAvailable
{
    $listeners = [Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
    if (@($listeners | Where-Object Port -EQ 5567).Count -ne 0)
    {
        throw 'fixture-port-occupied'
    }
}

function Get-TestProcessSnapshot
{
    $snapshot = @{}
    foreach ($process in @(Get-Process -Name java, javaw, dotnet, testhost, Microsoft.Spark.Worker -ErrorAction SilentlyContinue))
    {
        try
        {
            $snapshot["$($process.Id):$($process.StartTime.ToUniversalTime().Ticks)"] = $true
        }
        finally
        {
            $process.Dispose()
        }
    }
    return $snapshot
}

function Get-InputHashes
{
    $hashes = [ordered]@{}
    foreach ($name in $inputFiles.Keys)
    {
        $hashes[$name] = (Get-FileHash -LiteralPath $inputFiles[$name] -Algorithm SHA256).Hash
    }
    return ($hashes | ConvertTo-Json -Compress)
}

function Get-JavaIdentity([string]$JavaHome)
{
    # Never echo the other properties: these can contain paths, options or secrets.
    $output = & (Join-Path $JavaHome 'bin/java.exe') -XshowSettings:properties -version 2>&1
    if ($LASTEXITCODE -ne 0) { throw 'java-identity-failed' }
    $identity = @{}
    foreach ($line in $output)
    {
        if ([string]$line -match '^\s*java\.(vendor|runtime\.version) = ([A-Za-z0-9 ._()+-]{1,100})\s*$')
        {
            $identity[$Matches[1]] = $Matches[2].Trim()
        }
    }
    if (-not $identity.ContainsKey('vendor') -or
        -not $identity.ContainsKey('runtime.version') -or
        $identity['runtime.version'] -notmatch '^17\.')
    {
        throw 'expected-jdk17-identity'
    }
    return [ordered]@{
        vendor = $identity['vendor']
        runtime = $identity['runtime.version']
        executableSha256 = (Get-FileHash -LiteralPath (Join-Path $JavaHome 'bin/java.exe') -Algorithm SHA256).Hash
    }
}

function Get-TestResult([string]$TrxPath)
{
    [xml]$trx = Get-Content -LiteralPath $TrxPath -Raw
    $summary = $trx.SelectSingleNode("/*[local-name()='TestRun']/*[local-name()='ResultSummary']")
    $counters = $summary.SelectSingleNode("*[local-name()='Counters']")
    $cases = @($trx.SelectNodes("/*[local-name()='TestRun']/*[local-name()='Results']/*[local-name()='UnitTestResult']"))
    $passed = @($cases | Where-Object { $_.GetAttribute('outcome') -eq 'Passed' })
    $skipped = @($cases | Where-Object { $_.GetAttribute('outcome') -eq 'NotExecuted' })
    if ($summary.GetAttribute('outcome') -notin @('Completed', 'Passed') -or
        $passed.Count -eq 0 -or $cases.Count -ne [int]$counters.GetAttribute('total') -or
        $passed.Count -ne [int]$counters.GetAttribute('executed') -or
        $passed.Count -ne [int]$counters.GetAttribute('passed') -or
        [int]$counters.GetAttribute('failed') -ne 0 -or
        $cases.Count -ne ($passed.Count + $skipped.Count))
    {
        throw 'failed-empty-or-incomplete-results'
    }

    # Keep the existing xUnit order, but do not claim equivalent runs if it drifts.
    # Exclude skips from start order; skipped cases have adapter-generated timestamps.
    $orderedIds = @($passed | Sort-Object { [DateTimeOffset]$_.GetAttribute('startTime') },
        { $_.GetAttribute('testId') } | ForEach-Object { $_.GetAttribute('testId') })
    $selectedIds = @($cases | ForEach-Object { $_.GetAttribute('testId') } | Sort-Object)
    $sha = [Security.Cryptography.SHA256]::Create()
    try
    {
        $orderHash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($orderedIds -join "`n"))).Replace('-', '')
        $selectionHash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($selectedIds -join "`n"))).Replace('-', '')
    }
    finally { $sha.Dispose() }
    return [ordered]@{
        passed = $passed.Count
        skipped = $skipped.Count
        total = $cases.Count
        orderSha256 = $orderHash
        selectionSha256 = $selectionHash
    }
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$originalLocation = Get-Location
$environmentNames = @(
    'JAVA_HOME', 'PATH', 'SPARK_HOME', 'HADOOP_HOME', 'SPARK_LOCAL_IP',
    'DOTNET_WORKER_DIR', 'DOTNET_ASSEMBLY_SEARCH_PATHS',
    'DOTNET_SPARKFIXTURE_EXPECTED_SPARK_VERSION', 'DOTNET_SPARKFIXTURE_EXPECTED_IO_ENCRYPTION',
    'DOTNET_SPARK_ML_PERSISTENCE_PHASE', 'DOTNET_SPARK_ML_MODEL_PATH',
    'DOTNET_SPARK_STRESS_ENABLE', 'DOTNET_SPARK_STRESS_ITERATIONS', 'DOTNET_SPARK_STRESS_SCENARIO',
    'DOTNET_SPARK_DEBUG_TRACE_DIR', 'DOTNET_SPARK_DEBUG_TRANSPORT'
)
$originalEnvironment = @{}
foreach ($name in $environmentNames)
{
    $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$comparisonExit = 1
$stage = 'validate-inputs'
try
{
    if (-not $IsWindows) { throw 'windows-required' }
    $JavaHomeA = (Get-Item -LiteralPath $JavaHomeA).FullName
    $JavaHomeB = (Get-Item -LiteralPath $JavaHomeB).FullName
    $SparkHome = (Get-Item -LiteralPath $SparkHome).FullName
    $HadoopHome = (Get-Item -LiteralPath $HadoopHome).FullName
    if ($JavaHomeA -eq $JavaHomeB) { throw 'distinct-jdk-directories-required' }
    $testDirectory = Join-Path $repositoryRoot "artifacts/bin/Microsoft.Spark.E2ETest/$Configuration/net8.0"
    $workerDirectory = Join-Path $repositoryRoot "artifacts/bin/Microsoft.Spark.Worker/$Configuration/net8.0"
    $bridgeJars = @(Get-ChildItem -LiteralPath (Join-Path $repositoryRoot 'src/scala/microsoft-spark-4-0/target') -Filter 'microsoft-spark-4-0_2.13-*.jar' -File)
    $coreJars = @(Get-ChildItem -LiteralPath (Join-Path $SparkHome 'jars') -Filter "spark-core_2.13-$SparkVersion.jar" -File)
    if ($bridgeJars.Count -ne 1 -or $coreJars.Count -ne 1) { throw 'expected-single-built-jar' }
    $inputFiles = [ordered]@{
        e2e = Join-Path $testDirectory 'Microsoft.Spark.E2ETest.dll'
        library = Join-Path $testDirectory 'Microsoft.Spark.dll'
        worker = Join-Path $workerDirectory 'Microsoft.Spark.Worker.dll'
        bridge = $bridgeJars[0].FullName
        sparkCore = $coreJars[0].FullName
    }
    foreach ($path in @($inputFiles.Values) + @(
        (Join-Path $JavaHomeA 'bin/java.exe'), (Join-Path $JavaHomeB 'bin/java.exe'),
        (Join-Path $SparkHome 'bin/spark-submit.cmd'), (Join-Path $HadoopHome 'bin/winutils.exe'),
        (Join-Path $workerDirectory 'Microsoft.Spark.Worker.exe')))
    {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw 'required-built-input-missing' }
    }
    $null = Get-Command dotnet -CommandType Application
    Assert-FixturePortAvailable
    $baselineHashes = Get-InputHashes
    $runtimes = @(
        @{ label = 'A'; home = $JavaHomeA; identity = (Get-JavaIdentity $JavaHomeA) },
        @{ label = 'B'; home = $JavaHomeB; identity = (Get-JavaIdentity $JavaHomeB) }
    )
    if ([string]::IsNullOrWhiteSpace($ResultsDirectory))
    {
        $ResultsDirectory = Join-Path $repositoryRoot 'artifacts/TestResults/jdk-comparison'
    }
    $runDirectory = Join-Path ($ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($ResultsDirectory)) ([Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $runDirectory | Out-Null
    $baselineHashes | Set-Content -LiteralPath (Join-Path $runDirectory 'input-sha256.json')
    Set-Location -LiteralPath $repositoryRoot
    $comparisonExit = 0
    $referenceResult = $null
    for ($session = 1; $session -le $Sessions; $session++)
    {
        foreach ($runtime in $runtimes)
        {
            $stage = 'preflight'
            Assert-FixturePortAvailable
            if ((Get-InputHashes) -cne $baselineHashes) { throw 'built-inputs-changed' }
            if ((Get-FileHash -LiteralPath (Join-Path $runtime.home 'bin/java.exe') -Algorithm SHA256).Hash -cne
                $runtime.identity.executableSha256) { throw 'jdk-input-changed' }
            $beforeProcesses = Get-TestProcessSnapshot
            $legDirectory = Join-Path $runDirectory ("session-$session-" + $runtime.label)
            New-Item -ItemType Directory -Path $legDirectory | Out-Null
            $leg = [ordered]@{ session = $session; runtime = $runtime.label; identity = $runtime.identity; status = 'FAIL'; reason = 'runner'; exitCode = -1 }
            try
            {
                $env:JAVA_HOME = $runtime.home
                $env:PATH = "$($runtime.home)\bin;$HadoopHome\bin;$($originalEnvironment['PATH'])"
                $env:SPARK_HOME = $SparkHome
                $env:HADOOP_HOME = $HadoopHome
                $env:SPARK_LOCAL_IP = '127.0.0.1'
                $env:DOTNET_WORKER_DIR = $workerDirectory
                $env:DOTNET_ASSEMBLY_SEARCH_PATHS = $testDirectory
                $env:DOTNET_SPARKFIXTURE_EXPECTED_SPARK_VERSION = $SparkVersion
                $env:DOTNET_SPARKFIXTURE_EXPECTED_IO_ENCRYPTION = $null
                $env:DOTNET_SPARK_ML_PERSISTENCE_PHASE = $null
                $env:DOTNET_SPARK_ML_MODEL_PATH = $null
                $env:DOTNET_SPARK_STRESS_ENABLE = $null
                $env:DOTNET_SPARK_STRESS_ITERATIONS = $null
                $env:DOTNET_SPARK_STRESS_SCENARIO = $null
                $env:DOTNET_SPARK_DEBUG_TRACE_DIR = Join-Path $legDirectory 'traces'
                $env:DOTNET_SPARK_DEBUG_TRANSPORT = '1'
                $testArguments = @(
                    'test', 'src/csharp/Microsoft.Spark.E2ETest/Microsoft.Spark.E2ETest.csproj',
                    '--configuration', $Configuration, '--no-build', '--no-restore',
                    '--filter', $TestFilter, '--logger', 'trx;LogFileName=e2e.trx',
                    '--logger', 'console;verbosity=detailed', '--results-directory', $legDirectory,
                    '--blame-hang', '--blame-hang-timeout', '5min', '--blame-hang-dump-type', 'none'
                )
                $leg.reason = 'test-exit'
                & dotnet @testArguments *> (Join-Path $legDirectory 'test.log')
                $leg.exitCode = $LASTEXITCODE
                if ($leg.exitCode -ne 0) { throw 'test-failed' }
                $leg.reason = 'result-validation'
                $leg.results = Get-TestResult (Join-Path $legDirectory 'e2e.trx')
                $leg.reason = 'input-drift'
                if ((Get-InputHashes) -cne $baselineHashes) { throw 'built-inputs-changed' }
                $leg.reason = 'test-order-or-selection-drift'
                if ($null -ne $referenceResult -and
                    ($leg.results.orderSha256 -cne $referenceResult.orderSha256 -or
                     $leg.results.selectionSha256 -cne $referenceResult.selectionSha256))
                {
                    throw 'test-order-or-selection-changed'
                }
                if ($null -eq $referenceResult) { $referenceResult = $leg.results }
                $leg.status = 'PASS'
                $leg.reason = 'completed'
            }
            catch { $comparisonExit = 1 }
            finally
            {
                foreach ($name in $environmentNames) { Set-Item -LiteralPath "env:$name" -Value $originalEnvironment[$name] }
            }

            # A failed host may leave a JVM/worker behind. Do not reuse its port or
            # kill it; an unexpected new process also stops this conservative runner.
            $stage = 'residue-check'
            $residue = @()
            for ($attempt = 0; $attempt -lt 5; $attempt++)
            {
                $residue = @((Get-TestProcessSnapshot).Keys | Where-Object { -not $beforeProcesses.ContainsKey($_) })
                if ($residue.Count -eq 0) { break }
                Start-Sleep -Seconds 1
            }
            if ($residue.Count -ne 0)
            {
                $leg.status = 'FAIL'
                $leg.reason = 'process-residue'
                $comparisonExit = 1
            }
            $portOccupied = $false
            try { Assert-FixturePortAvailable }
            catch
            {
                $portOccupied = $true
                $leg.status = 'FAIL'
                $leg.reason = 'fixture-port-occupied'
                $comparisonExit = 1
            }
            $leg | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $legDirectory 'summary.json')
            $counts = if ($leg.Contains('results')) { "executed=$($leg.results.passed) skipped=$($leg.results.skipped)" } else { 'executed=unknown skipped=unknown' }
            Write-Output ("JDK-$($runtime.label) session=$session status=$($leg.status) reason=$($leg.reason) exit=$($leg.exitCode) $counts vendor=$($runtime.identity.vendor) runtime=$($runtime.identity.runtime) results=$legDirectory")
            if ($residue.Count -ne 0 -or $portOccupied) { throw 'unsafe-process-residue' }
        }
    }
}
catch
{
    $comparisonExit = 1
    Write-Output "JDK comparison stopped: stage=$stage. No processes were terminated."
}
finally
{
    foreach ($name in $environmentNames) { Set-Item -LiteralPath "env:$name" -Value $originalEnvironment[$name] }
    Set-Location -LiteralPath $originalLocation.Path
}
exit $comparisonExit
