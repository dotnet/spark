# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$PackageDirectory,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$SparkHome,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$WorkDirectory,

    [ValidateSet('3.0.2', '3.1.2', '3.2.3', '3.3.4', '3.4.4', '3.5.3',
        '4.0.0', '4.0.1', '4.0.2', '4.0.3', '4.0.4')]
    [string]$SparkVersion = '4.0.4',

    [uri]$DependencySource = 'https://api.nuget.org/v3/index.json'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

if (-not ($IsWindows -or $IsLinux))
{
    throw 'Release package smoke tests support Windows and Linux x64.'
}
if ([Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne 'X64')
{
    throw 'Release package smoke tests require an x64 operating system.'
}
if ($DependencySource.Scheme -ne 'https')
{
    throw 'DependencySource must be an HTTPS NuGet v3 source.'
}

$packageItem = Get-Item -LiteralPath $PackageDirectory
$sparkItem = Get-Item -LiteralPath $SparkHome
if ((-not $packageItem.PSIsContainer) -or (-not $sparkItem.PSIsContainer))
{
    throw 'PackageDirectory and SparkHome must identify directories.'
}
$packageRoot = $packageItem.FullName
$sparkRoot = $sparkItem.FullName
$runRoot = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($WorkDirectory)
if (Test-Path -LiteralPath $runRoot)
{
    throw "WorkDirectory must be a new directory: '$runRoot'."
}
$sparkSubmit = Join-Path $sparkRoot $(if ($IsWindows) { 'bin/spark-submit.cmd' } else { 'bin/spark-submit' })
if (-not (Test-Path -LiteralPath $sparkSubmit -PathType Leaf))
{
    throw "Spark launcher not found: '$sparkSubmit'."
}

# Read package identity rather than matching extension package names or guessing a version.
$corePackages = @(foreach ($file in Get-ChildItem -LiteralPath $packageRoot -Filter '*.nupkg' -File -Recurse)
{
    $archive = [IO.Compression.ZipFile]::OpenRead($file.FullName)
    try
    {
        $nuspecs = @($archive.Entries | Where-Object { $_.FullName -match '^[^/\\]+\.nuspec$' })
        if ($nuspecs.Count -ne 1)
        {
            throw "Expected one package manifest in '$($file.FullName)'."
        }
        $reader = [IO.StreamReader]::new($nuspecs[0].Open())
        try { [xml]$manifest = $reader.ReadToEnd() } finally { $reader.Dispose() }
        $id = $manifest.SelectSingleNode("/*[local-name()='package']/*[local-name()='metadata']/*[local-name()='id']")
        if (($null -ne $id) -and ($id.InnerText -eq 'Microsoft.Spark'))
        {
            $file
        }
    }
    finally
    {
        $archive.Dispose()
    }
})
if ($corePackages.Count -ne 1)
{
    throw "Expected exactly one Microsoft.Spark NuGet package, found $($corePackages.Count)."
}
$package = $corePackages[0]
$packageInfo = & (Join-Path $PSScriptRoot 'Test-SparkReleasePackage.ps1') `
    -PackagePath $package.FullName -SparkMajorMinorVersion ([version]$SparkVersion).ToString(2)
$runtime = if ($IsWindows) { 'win-x64' } else { 'linux-x64' }
$workerArchiveName = "Microsoft.Spark.Worker.net8.0.$runtime-$($packageInfo.PackageVersion).zip"
$workerArchives = @(Get-ChildItem -LiteralPath $packageRoot -File -Recurse |
    Where-Object { $_.Name -eq $workerArchiveName })
if ($workerArchives.Count -ne 1)
{
    throw "Expected exactly one '$workerArchiveName', found $($workerArchives.Count)."
}

New-Item -ItemType Directory -Path $runRoot | Out-Null
$projectDirectory = New-Item -ItemType Directory -Path (Join-Path $runRoot 'application')
$localSource = New-Item -ItemType Directory -Path (Join-Path $runRoot 'packages')
$sparkConfDirectory = New-Item -ItemType Directory -Path (Join-Path $runRoot 'spark-conf')
Copy-Item -LiteralPath $package.FullName -Destination $localSource.FullName
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'ReleaseSmokeTest/Program.cs') -Destination $projectDirectory.FullName
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'ReleaseSmokeTest/ReleaseSmokeTest.csproj') -Destination $projectDirectory.FullName
$project = Join-Path $projectDirectory.FullName 'ReleaseSmokeTest.csproj'
$publishDirectory = Join-Path $runRoot 'published'
$packageCache = Join-Path $runRoot 'nuget-cache'
$workerDirectory = Join-Path $runRoot 'worker'
$archive = [IO.Compression.ZipFile]::OpenRead($workerArchives[0].FullName)
try
{
    if (@($archive.Entries | Where-Object { $_.FullName.Contains('\') }).Count -ne 0)
    {
        throw 'Worker ZIP entries must use forward slashes for cross-platform extraction.'
    }
}
finally
{
    $archive.Dispose()
}
[IO.Compression.ZipFile]::ExtractToDirectory($workerArchives[0].FullName, $workerDirectory)
$workerName = if ($IsWindows) { 'Microsoft.Spark.Worker.exe' } else { 'Microsoft.Spark.Worker' }
$workers = @(Get-ChildItem -LiteralPath $workerDirectory -File -Recurse |
    Where-Object { $_.Name -eq $workerName })
if ($workers.Count -ne 1)
{
    throw "Expected one extracted '$workerName', found $($workers.Count)."
}
if ($IsLinux)
{
    & chmod +x $workers[0].FullName
    if ($LASTEXITCODE -ne 0) { throw 'Failed to make the extracted Worker executable.' }
}

$archive = [IO.Compression.ZipFile]::OpenRead($package.FullName)
try
{
    $jarPath = Join-Path $runRoot ([IO.Path]::GetFileName($packageInfo.SparkJarEntry))
    [IO.Compression.ZipFileExtensions]::ExtractToFile($archive.GetEntry($packageInfo.SparkJarEntry), $jarPath)
}
finally
{
    $archive.Dispose()
}

# Exact-ID mapping wins over the wildcard. Microsoft.Spark cannot fall back to a public release.
[xml]$nugetConfig = @'
<configuration>
  <packageSources>
    <clear />
    <add key="ReleaseCandidate" value="" />
    <add key="Dependencies" value="" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="ReleaseCandidate"><package pattern="Microsoft.Spark" /></packageSource>
    <packageSource key="Dependencies"><package pattern="*" /></packageSource>
  </packageSourceMapping>
</configuration>
'@
$nugetConfig.configuration.packageSources.add[0].SetAttribute('value', $localSource.FullName)
$nugetConfig.configuration.packageSources.add[1].SetAttribute('value', $DependencySource.AbsoluteUri)
$configPath = Join-Path $runRoot 'NuGet.Config'
$nugetConfig.Save($configPath)
$buildProperties = @(
    "-p:SparkPackageVersion=$($packageInfo.PackageVersion)",
    '-p:ImportDirectoryBuildProps=false', '-p:ImportDirectoryBuildTargets=false'
)
$previousEnvironment = @{}
foreach ($name in @('SPARK_HOME', 'SPARK_CONF_DIR', 'DOTNET_WORKER_DIR',
    'DOTNET_WORKER_DEBUG', 'DOTNET_ASSEMBLY_SEARCH_PATHS', 'NUGET_PACKAGES'))
{
    $previousEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try
{
    $env:SPARK_HOME = $sparkRoot
    $env:SPARK_CONF_DIR = $sparkConfDirectory.FullName
    $env:DOTNET_WORKER_DIR = $workers[0].DirectoryName
    $env:DOTNET_WORKER_DEBUG = $null
    $env:DOTNET_ASSEMBLY_SEARCH_PATHS = $publishDirectory
    $env:NUGET_PACKAGES = $packageCache

    Write-Host "Validating Microsoft.Spark $($packageInfo.PackageVersion), Spark $SparkVersion, $runtime."
    Write-Host "Smoke test files and logs: $runRoot"
    & dotnet restore $project --configfile $configPath --packages $packageCache @buildProperties
    if ($LASTEXITCODE -ne 0) { throw "Release smoke restore failed with exit code $LASTEXITCODE." }
    & dotnet publish $project --configuration Release --no-restore --output $publishDirectory @buildProperties
    if ($LASTEXITCODE -ne 0) { throw "Release smoke publish failed with exit code $LASTEXITCODE." }

    $submitArguments = @(
        '--master', 'local[2]', '--conf', 'spark.ui.enabled=false',
        '--class', 'org.apache.spark.deploy.dotnet.DotnetRunner',
        $jarPath, 'dotnet', (Join-Path $publishDirectory 'SparkReleaseSmokeTest.dll'), $SparkVersion
    )
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.WorkingDirectory = $runRoot
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    if ($IsWindows)
    {
        # cmd.exe is necessary for Spark's Windows launcher. Reject expansion/quoting characters.
        $commandArguments = @($sparkSubmit) + $submitArguments
        if (@($commandArguments | Where-Object { $_ -match '["%\r\n]' }).Count -ne 0)
        {
            throw 'Windows smoke test paths cannot contain quotes, percent signs, or newlines.'
        }
        $startInfo.FileName = $env:ComSpec
        $startInfo.Arguments = '/d /v:off /s /c "' + (($commandArguments | ForEach-Object { '"' + $_ + '"' }) -join ' ') + '"'
    }
    else
    {
        $startInfo.FileName = $sparkSubmit
        foreach ($argument in $submitArguments) { $startInfo.ArgumentList.Add($argument) }
    }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $started = $false
    $stdout = [IO.File]::Create((Join-Path $runRoot 'spark-submit.stdout.log'))
    $stderr = [IO.File]::Create((Join-Path $runRoot 'spark-submit.stderr.log'))
    try
    {
        $started = $process.Start()
        if (-not $started) { throw 'Failed to launch spark-submit.' }
        $stdoutCopy = $process.StandardOutput.BaseStream.CopyToAsync($stdout)
        $stderrCopy = $process.StandardError.BaseStream.CopyToAsync($stderr)
        if (-not $process.WaitForExit(300000))
        {
            # Kill only the process tree started by this invocation; never search for Java/dotnet PIDs.
            $process.Kill($true)
            $process.WaitForExit(10000) | Out-Null
            throw 'Release smoke test exceeded its five-minute timeout.'
        }
        if (-not ($stdoutCopy.Wait(10000) -and $stderrCopy.Wait(10000)))
        {
            throw 'Release smoke output streams did not close after process exit.'
        }
        if ($process.ExitCode -ne 0)
        {
            throw "Release smoke test failed with exit code $($process.ExitCode)."
        }
    }
    finally
    {
        if ($started -and (-not $process.HasExited))
        {
            $process.Kill($true)
            $process.WaitForExit(10000) | Out-Null
        }
        $stdout.Dispose()
        $stderr.Dispose()
        $process.Dispose()
        Get-Content -LiteralPath (Join-Path $runRoot 'spark-submit.stdout.log') | Write-Host
        Get-Content -LiteralPath (Join-Path $runRoot 'spark-submit.stderr.log') | Write-Host
    }

    if (-not (Select-String -LiteralPath (Join-Path $runRoot 'spark-submit.stdout.log') -SimpleMatch 'SPARK_RELEASE_SMOKE_TEST_PASSED' -Quiet))
    {
        throw 'spark-submit exited without the release smoke success marker.'
    }
}
finally
{
    foreach ($name in $previousEnvironment.Keys)
    {
        Set-Item -LiteralPath "Env:$name" -Value $previousEnvironment[$name]
    }
}
