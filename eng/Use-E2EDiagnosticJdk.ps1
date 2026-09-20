# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

<#
.SYNOPSIS
Selects a checksum-pinned, job-local Windows JDK for the opt-in CI comparison.
.DESCRIPTION
Requires Agent.TempDirectory. ArchivePath permits offline validation with an
existing ZIP; it does not bypass the pinned checksum or runtime identity checks.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$RuntimeVersion,
    [string]$ArchivePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$PSNativeCommandUseErrorActionPreference = $false

$stage = 'validate-inputs'
try
{
    if (-not $IsWindows -or
        [Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne
            [Runtime.InteropServices.Architecture]::X64)
    {
        throw 'windows-x64-required'
    }
    $release = switch -Exact ($RuntimeVersion)
    {
        '17.0.17+10' {
            @{
                url = 'https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.17%2B10/OpenJDK17U-jdk_x64_windows_hotspot_17.0.17_10.zip'
                sha256 = 'DCF0064EFEC7E515A5E3B56E7532F1A1C125510303C6C9E60AF8878E3F7347FE'
            }
        }
        '17.0.18+8' {
            @{
                url = 'https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.18%2B8/OpenJDK17U-jdk_x64_windows_hotspot_17.0.18_8.zip'
                sha256 = 'C24E6215FA75F861D2A900CEEC67C620CD2EB6DFB81541087D5981293B16F0E5'
            }
        }
        default { throw 'unsupported-diagnostic-runtime' }
    }
    if ([string]::IsNullOrWhiteSpace($env:AGENT_TEMPDIRECTORY) -or
        ($PSBoundParameters.ContainsKey('ArchivePath') -and [string]::IsNullOrWhiteSpace($ArchivePath)))
    {
        throw 'agent-temp-and-valid-archive-required'
    }

    $stage = 'agent-temp'
    $temporaryRoot = Get-Item -LiteralPath $env:AGENT_TEMPDIRECTORY
    if ($temporaryRoot -isnot [IO.DirectoryInfo]) { throw 'agent-temp-directory-required' }
    $installRoot = Join-Path $temporaryRoot.FullName ('spark-e2e-jdk-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $installRoot | Out-Null
    $archive = Join-Path $installRoot 'jdk.zip'

    $stage = 'acquire-archive'
    if ($PSBoundParameters.ContainsKey('ArchivePath'))
    {
        # Validate an isolated copy, never extract an archive that another run can replace.
        Copy-Item -LiteralPath $ArchivePath -Destination $archive
    }
    else
    {
        Invoke-WebRequest -Uri $release.url -OutFile $archive -TimeoutSec 300 -MaximumRetryCount 0
    }

    $stage = 'verify-checksum'
    $archiveHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
    if ($archiveHash -cne $release.sha256) { throw 'archive-checksum-mismatch' }

    $stage = 'extract-archive'
    Expand-Archive -LiteralPath $archive -DestinationPath $installRoot
    $javaHome = Join-Path $installRoot ('jdk-' + $RuntimeVersion)
    $javaExecutable = Join-Path $javaHome 'bin/java.exe'
    if (-not (Test-Path -LiteralPath $javaExecutable -PathType Leaf)) { throw 'java-executable-missing' }

    $stage = 'verify-runtime'
    # Keep raw properties in memory only: options, paths and other properties can contain secrets.
    $output = & $javaExecutable -XshowSettings:properties -version 2>&1
    if ($LASTEXITCODE -ne 0) { throw 'java-identity-failed' }
    $identity = @{}
    foreach ($line in $output)
    {
        if ([string]$line -match '^\s*java\.(vendor|runtime\.version) = ([A-Za-z0-9 ._()+-]{1,100})\s*$')
        {
            $identity[$Matches[1]] = $Matches[2].Trim()
        }
    }
    if ($identity['vendor'] -cne 'Eclipse Adoptium' -or
        $identity['runtime.version'] -cne $RuntimeVersion)
    {
        throw 'unexpected-java-identity'
    }
    $javaHash = (Get-FileHash -LiteralPath $javaExecutable -Algorithm SHA256).Hash
    $osVersion = [Environment]::OSVersion.Version.ToString()

    # Publish only after every validation succeeds. The agent owns temporary-directory disposal.
    $escapedHome = $javaHome.Replace('%', '%AZP25').Replace("`r", '%0D').Replace("`n", '%0A')
    Write-Host "SPARK_JDK_DIAG vendor=Eclipse_Adoptium runtime=$RuntimeVersion os_version=$osVersion archive_sha256=$archiveHash java_sha256=$javaHash"
    Write-Host "##vso[task.setvariable variable=JAVA_HOME;]$escapedHome"
    Write-Host "##vso[task.prependpath]$escapedHome\bin"
}
catch
{
    # Do not echo download exceptions: redirect URLs and process output may include secrets.
    Write-Host "##vso[task.logissue type=error;]Diagnostic JDK setup failed at $stage."
    exit 1
}
