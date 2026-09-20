# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

# Tests runner boundaries with synthetic archives and launchers; no Spark or network is needed.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$runner = Join-Path (Split-Path -Parent $PSScriptRoot) 'Run-SparkReleaseSmokeTest.ps1'
$temporaryRoot = (Get-Item -LiteralPath ([IO.Path]::GetTempPath())).FullName
$testDirectoryName = 'spark release smoke-' + [Guid]::NewGuid().ToString('N')
$testDirectory = Join-Path $temporaryRoot $testDirectoryName
$packageDirectory = Join-Path $testDirectory 'packages'
$sparkDirectory = Join-Path $testDirectory 'spark'
$launcherDirectory = Join-Path $sparkDirectory 'bin'
$runtime = if ($IsWindows) { 'win-x64' } else { 'linux-x64' }
$workerName = if ($IsWindows) { 'Microsoft.Spark.Worker.exe' } else { 'Microsoft.Spark.Worker' }
$launcher = Join-Path $launcherDirectory $(if ($IsWindows) { 'spark-submit.cmd' } else { 'spark-submit' })
$version = '2.3.1-preview.1'
$previousTestState = Get-Variable -Name SparkReleaseSmokeTestState -Scope Global -ErrorAction SilentlyContinue
$global:SparkReleaseSmokeTestState = @{ RestoreCalls = 0; PublishCalls = 0; Version = $version }
$script:passed = 0
New-Item -ItemType Directory -Path $packageDirectory, $launcherDirectory | Out-Null

function New-TestArchive
{
    param([string]$Path, [hashtable]$Entries)

    $archive = [IO.Compression.ZipFile]::Open($Path, [IO.Compression.ZipArchiveMode]::Create)
    try
    {
        foreach ($name in $Entries.Keys)
        {
            $bytes = if ($Entries[$name] -is [byte[]]) { $Entries[$name] } else { [Text.Encoding]::UTF8.GetBytes($Entries[$name]) }
            $stream = $archive.CreateEntry($name).Open()
            try { $stream.Write($bytes, 0, $bytes.Length) } finally { $stream.Dispose() }
        }
    }
    finally { $archive.Dispose() }
}

function Set-TestLauncher
{
    param([string]$Message, [int]$ExitCode = 0)

    $body = if ($IsWindows)
    {
        "@echo off`r`necho $Message`r`nexit /b $ExitCode`r`n"
    }
    else
    {
        "#!/bin/sh`nprintf '%s\n' '$Message'`nexit $ExitCode`n"
    }
    [IO.File]::WriteAllText($launcher, $body, [Text.UTF8Encoding]::new($false))
    if ($IsLinux) { & chmod +x $launcher }
}

function dotnet
{
    if ($args[0] -eq 'restore')
    {
        $global:SparkReleaseSmokeTestState.RestoreCalls++
        $configPath = $args[[Array]::IndexOf($args, '--configfile') + 1]
        [xml]$config = Get-Content -Raw -LiteralPath $configPath
        if ($config.configuration.packageSourceMapping.packageSource[0].package.pattern -ne 'Microsoft.Spark')
        {
            throw 'The candidate package is not restricted to its local source.'
        }
        if ($env:NUGET_PACKAGES -ne $args[[Array]::IndexOf($args, '--packages') + 1])
        {
            throw 'Restore does not use the isolated package cache.'
        }
    }
    elseif ($args[0] -eq 'publish')
    {
        $global:SparkReleaseSmokeTestState.PublishCalls++
        if (($args -notcontains '--no-restore') -or
            ($args -notcontains "-p:SparkPackageVersion=$($global:SparkReleaseSmokeTestState.Version)"))
        {
            throw 'Publish must preserve the exact candidate restore.'
        }
    }
    else { throw 'Unexpected dotnet invocation.' }
    $global:LASTEXITCODE = 0
}

function Invoke-TestRun
{
    & $runner -PackageDirectory $packageDirectory -SparkHome $sparkDirectory `
        -WorkDirectory (Join-Path $testDirectory ([Guid]::NewGuid().ToString('N')))
}

function Assert-Rejected
{
    param([scriptblock]$Action, [string]$ExpectedMessage)

    try { & $Action } catch
    {
        if ($_.Exception.Message -notlike "*$ExpectedMessage*") { throw }
        $script:passed++
        return
    }
    throw "Expected rejection containing '$ExpectedMessage'."
}

try
{
    $jarPath = Join-Path $testDirectory 'bridge.jar'
    New-TestArchive $jarPath @{
        'org/apache/spark/deploy/dotnet/DotnetRunner.class' = [byte[]]@(0xca, 0xfe, 0xba, 0xbe)
        'org/apache/spark/api/dotnet/DotnetBackend.class' = [byte[]]@(0xca, 0xfe, 0xba, 0xbe)
        'org/apache/spark/sql/api/dotnet/SQLUtils.class' = [byte[]]@(0xca, 0xfe, 0xba, 0xbe)
        'META-INF/maven/com.microsoft.scala/microsoft-spark-4-0_2.13/pom.properties' =
            "artifactId=microsoft-spark-4-0_2.13`ngroupId=com.microsoft.scala`nversion=2.3.1`n"
    }
    $corePackage = Join-Path $packageDirectory 'arbitrary-package-name.nupkg'
    New-TestArchive $corePackage @{
        'Microsoft.Spark.nuspec' = "<package><metadata><id>Microsoft.Spark</id><version>$version</version></metadata></package>"
        'jars/microsoft-spark-4-0_2.13-2.3.1.jar' = [IO.File]::ReadAllBytes($jarPath)
    }
    New-TestArchive (Join-Path $packageDirectory 'Microsoft.Spark.0.0.0.nupkg') @{
        'Extension.nuspec' = '<package><metadata><id>Microsoft.Spark.Extension</id><version>0.0.0</version></metadata></package>'
    }
    $workerArchiveDirectory = New-Item -ItemType Directory -Path (Join-Path $packageDirectory "$runtime-archive")
    $workerArchive = Join-Path $workerArchiveDirectory.FullName "Microsoft.Spark.Worker.net8.0.$runtime-$version.zip"
    New-TestArchive $workerArchive @{ "Microsoft.Spark.Worker-$version/$workerName" = 'synthetic worker' }

    Set-TestLauncher 'SPARK_RELEASE_SMOKE_TEST_PASSED'
    $originalWorkerDirectory = $env:DOTNET_WORKER_DIR
    $originalDebugSetting = $env:DOTNET_WORKER_DEBUG
    Invoke-TestRun
    if (($global:SparkReleaseSmokeTestState.RestoreCalls -ne 1) -or
        ($global:SparkReleaseSmokeTestState.PublishCalls -ne 1) -or
        ($env:DOTNET_WORKER_DIR -ne $originalWorkerDirectory) -or ($env:DOTNET_WORKER_DEBUG -ne $originalDebugSetting))
    {
        throw 'Successful smoke execution did not restore its environment or run both build commands.'
    }
    $script:passed++

    Set-TestLauncher 'SPARK_RELEASE_SMOKE_TEST_PASSED' 7
    Assert-Rejected { Invoke-TestRun } 'exit code 7'
    Set-TestLauncher 'NO_SUCCESS_MARKER'
    Assert-Rejected { Invoke-TestRun } 'without the release smoke success marker'

    Assert-Rejected {
        & $runner -PackageDirectory $packageDirectory -SparkHome $sparkDirectory -WorkDirectory $testDirectory
    } 'must be a new directory'
    Assert-Rejected {
        & $runner -PackageDirectory $packageDirectory -SparkHome $sparkDirectory `
            -WorkDirectory (Join-Path $testDirectory 'unused') -DependencySource 'http://example.invalid/v3/index.json'
    } 'must be an HTTPS'

    Copy-Item -LiteralPath $workerArchive -Destination $packageDirectory
    Assert-Rejected { Invoke-TestRun } 'found 2'
    Copy-Item -LiteralPath $corePackage -Destination (Join-Path $packageDirectory 'duplicate.nupkg')
    Assert-Rejected { Invoke-TestRun } 'one Microsoft.Spark NuGet package, found 2'
    Write-Host "Release smoke runner tests passed: $script:passed."
}
finally
{
    if ($null -eq $previousTestState)
    {
        Remove-Variable -Name SparkReleaseSmokeTestState -Scope Global
    }
    else
    {
        $global:SparkReleaseSmokeTestState = $previousTestState.Value
    }
    $testItem = Get-Item -LiteralPath $testDirectory -Force
    if (($testItem.Parent.FullName -ne $temporaryRoot.TrimEnd([IO.Path]::DirectorySeparatorChar)) -or
        ($testItem.Name -ne $testDirectoryName) -or
        ($testItem.Attributes -band [IO.FileAttributes]::ReparsePoint))
    {
        throw "Refusing to remove unverified test directory '$testDirectory'."
    }
    Remove-Item -LiteralPath $testItem.FullName -Recurse -Force
}
