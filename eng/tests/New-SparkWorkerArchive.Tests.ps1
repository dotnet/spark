# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

# Standalone tests: pwsh -File eng/tests/New-SparkWorkerArchive.Tests.ps1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$archiveScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'New-SparkWorkerArchive.ps1'
$temporaryRoot = (Get-Item -LiteralPath ([IO.Path]::GetTempPath())).FullName
$testDirectoryName = 'spark-worker-archive-' + [Guid]::NewGuid().ToString('N')
$testDirectory = Join-Path $temporaryRoot $testDirectoryName
New-Item -ItemType Directory -Path $testDirectory | Out-Null
$sourceDirectory = Join-Path $testDirectory 'published worker'
New-Item -ItemType Directory -Path $sourceDirectory | Out-Null
$script:passed = 0

function Assert-Rejected
{
    param([string]$Name, [scriptblock]$Action, [string]$Message)

    $failure = $null
    try
    {
        & $Action | Out-Null
    }
    catch
    {
        $failure = $_
    }
    if (($null -eq $failure) -or ($failure.Exception.Message -notlike "*$Message*"))
    {
        throw "FAIL $Name`: expected rejection containing '$Message'; actual '$failure'."
    }
    $script:passed++
    Write-Host "PASS $Name"
}

try
{
    $files = @{
        'Microsoft.Spark.Worker.dll' = 'worker binary'
        'Microsoft.Spark.Worker.runtimeconfig.json' = 'runtime configuration'
        'runtimes/linux-x64/native/library.so' = 'native binary'
        'locale/resource.dat' = 'localized resource'
        'other/resource.dat' = 'another resource with the same filename'
        '.hidden.json' = 'hidden publish file'
    }
    foreach ($name in $files.Keys)
    {
        $filePath = Join-Path $sourceDirectory $name
        New-Item -ItemType Directory -Path (Split-Path -Parent $filePath) -Force | Out-Null
        [IO.File]::WriteAllText($filePath, $files[$name])
    }
    New-Item -ItemType Directory -Path (Join-Path $sourceDirectory 'empty') | Out-Null
    if ($IsWindows)
    {
        [IO.File]::SetAttributes((Join-Path $sourceDirectory '.hidden.json'), [IO.FileAttributes]::Hidden)
    }
    $sourceItemCount = @(Get-ChildItem -LiteralPath $sourceDirectory -Recurse -Force).Count

    $targets = @(
        @{ Framework = 'net8.0'; RuntimeIdentifier = 'win-x64' },
        @{ Framework = 'net8.0'; RuntimeIdentifier = 'linux-x64' },
        @{ Framework = 'net8.0'; RuntimeIdentifier = 'osx-x64' },
        @{ Framework = 'net48'; RuntimeIdentifier = 'win-x64' }
    )
    foreach ($target in $targets)
    {
        $output = Join-Path $testDirectory "$($target.Framework)-$($target.RuntimeIdentifier)"
        $result = & $archiveScript -WorkerDirectory $sourceDirectory -OutputDirectory $output `
            -PackageVersion '2.3.1-preview.1' @target
        $expectedName = "Microsoft.Spark.Worker.$($target.Framework).$($target.RuntimeIdentifier)-2.3.1-preview.1.zip"
        if ((Split-Path -Leaf $result.ZipPath) -cne $expectedName)
        {
            throw 'FAIL archive filename is not release-compatible.'
        }

        $archive = [IO.Compression.ZipFile]::OpenRead($result.ZipPath)
        try
        {
            $prefix = 'Microsoft.Spark.Worker-2.3.1-preview.1/'
            foreach ($entry in $archive.Entries)
            {
                if ($entry.FullName.Contains('\') -or (-not $entry.FullName.StartsWith($prefix, [StringComparison]::Ordinal)))
                {
                    throw "FAIL nonportable ZIP entry: '$($entry.FullName)'."
                }
            }
            foreach ($name in $files.Keys)
            {
                $entry = $archive.GetEntry($prefix + $name)
                if ($null -eq $entry)
                {
                    throw "FAIL missing nested ZIP entry: '$name'."
                }
                $reader = [IO.StreamReader]::new($entry.Open())
                try
                {
                    if ($reader.ReadToEnd() -cne $files[$name])
                    {
                        throw "FAIL changed ZIP file contents: '$name'."
                    }
                }
                finally
                {
                    $reader.Dispose()
                }
            }
            if ($null -eq $archive.GetEntry($prefix + 'empty/'))
            {
                throw 'FAIL missing empty published directory.'
            }
        }
        finally
        {
            $archive.Dispose()
        }

        if ($target.RuntimeIdentifier -eq 'linux-x64')
        {
            $tarEntries = @(& tar -tzf $result.TarGzipPath)
            if (($LASTEXITCODE -ne 0) -or
                ($tarEntries -cnotcontains 'Microsoft.Spark.Worker-2.3.1-preview.1/runtimes/linux-x64/native/library.so'))
            {
                throw 'FAIL Linux tar.gz did not preserve the published tree.'
            }
        }
        elseif ($null -ne $result.TarGzipPath)
        {
            throw 'FAIL a non-Linux target produced a tar.gz archive.'
        }

        foreach ($name in $files.Keys)
        {
            if ([IO.File]::ReadAllText((Join-Path $sourceDirectory $name)) -cne $files[$name])
            {
                throw "FAIL source publish file was changed: '$name'."
            }
        }
        if (@(Get-ChildItem -LiteralPath $sourceDirectory -Recurse -Force).Count -ne $sourceItemCount)
        {
            throw 'FAIL source publish tree was changed.'
        }
        $script:passed++
        Write-Host "PASS $($target.Framework)/$($target.RuntimeIdentifier): portable entries, nested files, unchanged source"
    }

    $commonParameters = @{
        WorkerDirectory = $sourceDirectory
        PackageVersion = '2.3.1'
        Framework = 'net8.0'
        RuntimeIdentifier = 'win-x64'
    }
    Assert-Rejected 'existing output directory' {
        & $archiveScript @commonParameters -OutputDirectory $testDirectory
    } 'new directory outside'
    Assert-Rejected 'output inside source tree' {
        & $archiveScript @commonParameters -OutputDirectory (Join-Path $sourceDirectory 'archive')
    } 'new directory outside'
    Assert-Rejected 'net48 Linux target' {
        & $archiveScript -WorkerDirectory $sourceDirectory -OutputDirectory (Join-Path $testDirectory 'invalid-framework') `
            -PackageVersion '2.3.1' -Framework net48 -RuntimeIdentifier linux-x64
    } 'only for win-x64'
    Assert-Rejected 'source must be a directory' {
        & $archiveScript -WorkerDirectory (Join-Path $sourceDirectory 'Microsoft.Spark.Worker.dll') `
            -OutputDirectory (Join-Path $testDirectory 'invalid-source') `
            -PackageVersion '2.3.1' -Framework net8.0 -RuntimeIdentifier win-x64
    } 'already-published Worker directory'

    function Invoke-FailingTarTest
    {
        function tar { $global:LASTEXITCODE = 29 }
        Assert-Rejected 'native tar failure is propagated' {
            & $archiveScript -WorkerDirectory $sourceDirectory -OutputDirectory (Join-Path $testDirectory 'failed-tar') `
                -PackageVersion '2.3.1' -Framework net8.0 -RuntimeIdentifier linux-x64
        } 'exit code 29'
    }
    Invoke-FailingTarTest
    Write-Host "Worker archive tests: $script:passed passed."
}
finally
{
    $testItem = Get-Item -LiteralPath $testDirectory -Force
    $comparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
    if ((-not $testItem.PSIsContainer) -or
        ($testItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
        (-not [string]::Equals($testItem.FullName, [IO.Path]::GetFullPath($testDirectory), $comparison)) -or
        (-not [string]::Equals($testItem.Parent.FullName.TrimEnd([IO.Path]::DirectorySeparatorChar),
            $temporaryRoot.TrimEnd([IO.Path]::DirectorySeparatorChar), $comparison)) -or
        ($testItem.Name -ne $testDirectoryName))
    {
        throw "Refusing to clean up an unverified test directory: '$testDirectory'."
    }
    Remove-Item -LiteralPath $testItem.FullName -Recurse -Force
}
