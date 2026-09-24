# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DestinationDirectory,

    [ValidateSet('2.8.1', '3.3.5')]
    [string]$HadoopVersion = '3.3.5'
)

$ErrorActionPreference = 'Stop'
if (-not $IsWindows)
{
    throw 'Hadoop Windows tools can only be installed on Windows.'
}

$hadoopRoot = New-Item -ItemType Directory -Path $DestinationDirectory -Force
$hadoopBin = New-Item -ItemType Directory -Path (Join-Path $hadoopRoot.FullName 'bin') -Force
$archive = Join-Path $hadoopRoot.FullName "hadoop-$HadoopVersion.zip"
$archiveUrl = 'https://github.com/SparkSnail/winutils/releases/download/hadoop-3.3.5/hadoop-3.3.5.zip'
if ($HadoopVersion -eq '2.8.1')
{
    $archiveUrl = 'https://github.com/steveloughran/winutils/releases/download/tag_2017-08-29-hadoop-2.8.1-native/hadoop-2.8.1.zip'
}
Invoke-WebRequest -Uri $archiveUrl -OutFile $archive
Expand-Archive -LiteralPath $archive -DestinationPath $hadoopRoot.FullName -Force
Copy-Item -LiteralPath (Join-Path $hadoopRoot.FullName "hadoop-$HadoopVersion/winutils.exe") -Destination $hadoopBin.FullName -Force
if ($HadoopVersion -eq '3.3.5')
{
    # Match source E2E: Hadoop 3 also needs hadoop.dll on the Worker PATH.
    Copy-Item -LiteralPath (Join-Path $hadoopRoot.FullName 'hadoop-3.3.5/hadoop.dll') -Destination $hadoopBin.FullName -Force
}

# Match the Hadoop binaries used by Windows Scala/E2E tests without changing machine PATH.
$runtimeDll = Join-Path $env:SystemRoot 'System32/MSVCR100.dll'
if (-not (Test-Path -LiteralPath $runtimeDll))
{
    $installer = Join-Path $hadoopRoot.FullName 'vcredist_x64.exe'
    Invoke-WebRequest -Uri 'https://download.microsoft.com/download/1/6/5/165255E7-1014-4D0A-B094-B6A430A6BFFC/vcredist_x64.exe' -OutFile $installer
    $installation = Start-Process -FilePath $installer -ArgumentList '/q', '/norestart' -Wait -PassThru -WindowStyle Hidden
    if (($installation.ExitCode -notin @(0, 3010)) -or (-not (Test-Path -LiteralPath $runtimeDll)))
    {
        throw "VC++ 2010 x64 runtime installation failed (exit code $($installation.ExitCode))."
    }
}

& (Join-Path $hadoopBin.FullName 'winutils.exe') ls $hadoopBin.FullName
if ($LASTEXITCODE -ne 0)
{
    throw "Hadoop Windows tools failed to start (exit code $LASTEXITCODE)."
}
