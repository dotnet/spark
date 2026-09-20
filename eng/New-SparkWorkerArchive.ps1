# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$WorkerDirectory,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$OutputDirectory,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9][A-Za-z0-9.+-]*[A-Za-z0-9]$')]
    [string]$PackageVersion,

    [Parameter(Mandatory = $true)]
    [ValidateSet('net48', 'net8.0')]
    [string]$Framework,

    [Parameter(Mandatory = $true)]
    [ValidateSet('win-x64', 'linux-x64', 'osx-x64')]
    [string]$RuntimeIdentifier
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$source = Get-Item -LiteralPath $WorkerDirectory -Force
if (-not $source.PSIsContainer)
{
    throw 'WorkerDirectory must be an already-published Worker directory.'
}
if (($Framework -eq 'net48') -and ($RuntimeIdentifier -ne 'win-x64'))
{
    throw 'The net48 Worker archive is supported only for win-x64.'
}

$outputPath = [IO.Path]::GetFullPath(
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputDirectory))
$comparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
$sourcePrefix = $source.FullName.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
if ((Test-Path -LiteralPath $outputPath) -or $outputPath.StartsWith($sourcePrefix, $comparison))
{
    throw 'OutputDirectory must be a new directory outside WorkerDirectory.'
}

# Do not follow links out of the published tree or copy a link to an external file.
$sourceItems = @($source) + @(Get-ChildItem -LiteralPath $source.FullName -Recurse -Force)
if (@($sourceItems | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }).Count -ne 0)
{
    throw 'WorkerDirectory must not contain symbolic links or reparse points.'
}
if ($RuntimeIdentifier -eq 'linux-x64')
{
    Get-Command tar -ErrorAction Stop | Out-Null
}

$workerRootName = "Microsoft.Spark.Worker-$PackageVersion"
$workerRoot = Join-Path $outputPath $workerRootName
New-Item -ItemType Directory -Path $outputPath | Out-Null
# Copy the RID directory once: moving recursively enumerated children would flatten
# nested publish files and could change the signed source used by other validators.
Copy-Item -LiteralPath $source.FullName -Destination $workerRoot -Recurse -Force

$archiveName = "Microsoft.Spark.Worker.$Framework.$RuntimeIdentifier-$PackageVersion"
$zipPath = Join-Path $outputPath "$archiveName.zip"
$archive = [IO.Compression.ZipFile]::Open($zipPath, [IO.Compression.ZipArchiveMode]::Create)
try
{
    $archive.CreateEntry("$workerRootName/") | Out-Null
    foreach ($item in (Get-ChildItem -LiteralPath $workerRoot -Recurse -Force))
    {
        # Windows PowerShell's Compress-Archive can write backslash entry names,
        # which Linux ZipFile extraction treats as literal filename characters.
        $entryName = [IO.Path]::GetRelativePath($outputPath, $item.FullName).Replace('\', '/')
        if ($item.PSIsContainer)
        {
            $archive.CreateEntry("$entryName/") | Out-Null
        }
        else
        {
            [IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $archive, $item.FullName, $entryName) | Out-Null
        }
    }
}
finally
{
    $archive.Dispose()
}

$tarGzipPath = $null
if ($RuntimeIdentifier -eq 'linux-x64')
{
    $tarGzipPath = Join-Path $outputPath "$archiveName.tar.gz"
    & tar -czf $tarGzipPath -C $outputPath $workerRootName
    if ($LASTEXITCODE -ne 0)
    {
        throw "Creating the Linux Worker tar.gz failed with exit code $LASTEXITCODE."
    }
}

[PSCustomObject]@{
    ZipPath = $zipPath
    TarGzipPath = $tarGzipPath
}
