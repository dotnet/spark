# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

[CmdletBinding()]
param(
    [ValidateNotNullOrEmpty()]
    [string]$Configuration = 'Debug',

    [string]$ResultsDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$testProject = Join-Path $repositoryRoot 'src/csharp/Microsoft.Spark.E2ETest/Microsoft.Spark.E2ETest.csproj'
if ([string]::IsNullOrWhiteSpace($ResultsDirectory))
{
    $ResultsDirectory = Join-Path $repositoryRoot 'artifacts/TestResults/ml-persistence'
}

# A fresh child directory prevents a stale TRX from satisfying the phase checks.
$runId = [Guid]::NewGuid().ToString('N')
$runResultsDirectory = Join-Path `
    ($ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($ResultsDirectory)) $runId
New-Item -ItemType Directory -Path $runResultsDirectory | Out-Null

$temporaryRoot = (Get-Item -LiteralPath ([IO.Path]::GetTempPath())).FullName
$modelDirectoryName = "dotnet-spark-ml-$runId"
$modelDirectory = [IO.Path]::GetFullPath((Join-Path $temporaryRoot $modelDirectoryName))
$modelDirectoryCreated = $false
$originalPhase = [Environment]::GetEnvironmentVariable('DOTNET_SPARK_ML_PERSISTENCE_PHASE', 'Process')
$originalModelPath = [Environment]::GetEnvironmentVariable('DOTNET_SPARK_ML_MODEL_PATH', 'Process')

Write-Host "ML persistence results: $runResultsDirectory"
try
{
    New-Item -ItemType Directory -Path $modelDirectory | Out-Null
    $modelDirectoryCreated = $true
    $env:DOTNET_SPARK_ML_MODEL_PATH = $modelDirectory

    # The fixture uses a fixed backend port. Finish and validate save before starting load.
    foreach ($phase in @('save', 'load'))
    {
        $env:DOTNET_SPARK_ML_PERSISTENCE_PHASE = $phase
        $trxName = "ml-persistence-$phase.trx"
        $trxPath = Join-Path $runResultsDirectory $trxName
        $logPath = Join-Path $runResultsDirectory "ml-persistence-$phase.log"
        $testArguments = @(
            'test', $testProject,
            '--configuration', $Configuration,
            '--no-build',
            '--filter', 'FullyQualifiedName~MLCompatibilityTests.TestPipelineModelPersistence',
            '--logger', "trx;LogFileName=$trxName",
            '--results-directory', $runResultsDirectory,
            '--blame-hang', '--blame-hang-timeout', '3min',
            '--blame-hang-dump-type', 'none'
        )

        Write-Host "Running ML persistence phase '$phase' with model directory '$modelDirectory'."
        & dotnet @testArguments 2>&1 | Tee-Object -FilePath $logPath
        if ($LASTEXITCODE -ne 0)
        {
            throw "ML persistence phase '$phase' failed with exit code $LASTEXITCODE. See '$logPath'."
        }

        if (-not (Test-Path -LiteralPath $trxPath -PathType Leaf))
        {
            throw "ML persistence phase '$phase' did not produce '$trxPath'."
        }

        [xml]$testResults = Get-Content -Raw -LiteralPath $trxPath
        $counters = $testResults.SelectSingleNode(
            "/*[local-name()='TestRun']/*[local-name()='ResultSummary']/*[local-name()='Counters']")
        $caseResults = @($testResults.SelectNodes(
            "/*[local-name()='TestRun']/*[local-name()='Results']/*[local-name()='UnitTestResult']"))
        $failedCases = @($caseResults | Where-Object { $_.GetAttribute('outcome') -ne 'Passed' })
        if (($null -eq $counters) -or
            ($counters.GetAttribute('total') -ne '2') -or
            ($counters.GetAttribute('executed') -ne '2') -or
            ($counters.GetAttribute('passed') -ne '2') -or
            ($counters.GetAttribute('failed') -ne '0') -or
            ($counters.GetAttribute('notExecuted') -ne '0') -or
            ($caseResults.Count -ne 2) -or
            ($failedCases.Count -ne 0))
        {
            throw "ML persistence phase '$phase' must execute and pass exactly two cases without skips. See '$trxPath'."
        }

        Write-Host "ML persistence phase '$phase': 2 executed, 2 passed."
    }
}
finally
{
    # The environment provider removes absent variables when assigned null. Calling
    # SetEnvironmentVariable through PowerShell can instead restore them as empty strings.
    $env:DOTNET_SPARK_ML_PERSISTENCE_PHASE = $originalPhase
    $env:DOTNET_SPARK_ML_MODEL_PATH = $originalModelPath

    if ($modelDirectoryCreated -and (Test-Path -LiteralPath $modelDirectory))
    {
        $modelItem = Get-Item -LiteralPath $modelDirectory -Force
        $pathComparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
        if ((-not $modelItem.PSIsContainer) -or
            ($modelItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
            (-not [string]::Equals($modelItem.FullName, $modelDirectory, $pathComparison)) -or
            (-not [string]::Equals($modelItem.Parent.FullName.TrimEnd([IO.Path]::DirectorySeparatorChar),
                $temporaryRoot.TrimEnd([IO.Path]::DirectorySeparatorChar), $pathComparison)) -or
            ($modelItem.Name -ne $modelDirectoryName))
        {
            throw "Refusing to clean up an unverified model directory: '$modelDirectory'."
        }

        Remove-Item -LiteralPath $modelItem.FullName -Recurse -Force
    }
}
