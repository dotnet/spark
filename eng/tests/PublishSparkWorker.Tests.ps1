# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

# Standalone command-contract tests; the intercepted Exec task does not publish binaries.
# Run: pwsh -File eng/tests/PublishSparkWorker.Tests.ps1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$targetsPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'AfterSolutionBuild.targets'
$temporaryRoot = (Get-Item -LiteralPath ([IO.Path]::GetTempPath())).FullName
$testDirectoryName = 'spark-worker-version-' + [Guid]::NewGuid().ToString('N')
$testDirectory = Join-Path $temporaryRoot $testDirectoryName
New-Item -ItemType Directory -Path $testDirectory | Out-Null

try
{
    $projectPath = Join-Path $testDirectory 'PublishWorker.proj'
    $project = @'
<Project>
  <PropertyGroup>
    <RepoRoot>$(MSBuildProjectDirectory)\</RepoRoot>
    <PublishSparkWorker>true</PublishSparkWorker>
    <SparkWorkerPublishDir>$(MSBuildProjectDirectory)\published worker</SparkWorkerPublishDir>
    <Configuration>Release</Configuration>
    <OfficialBuildId>20260922.1</OfficialBuildId>
  </PropertyGroup>
  <Import Project="__TARGETS_PATH__" />
  <UsingTask TaskName="Exec" Override="true" TaskFactory="RoslynCodeTaskFactory"
             AssemblyFile="$(MSBuildToolsPath)/Microsoft.Build.Tasks.Core.dll">
    <ParameterGroup>
      <Command ParameterType="System.String" Required="true" />
    </ParameterGroup>
    <Task>
      <Code Type="Fragment" Language="cs"><![CDATA[
        Log.LogMessage(Microsoft.Build.Framework.MessageImportance.High,
            "WORKER_PUBLISH_ARGS:" + Command.Replace("\r", " ").Replace("\n", " "));
      ]]></Code>
    </Task>
  </UsingTask>
</Project>
'@
    $project = $project.Replace('__TARGETS_PATH__', [Security.SecurityElement]::Escape($targetsPath))
    [IO.File]::WriteAllText($projectPath, $project)

    $cases = @(
        @{ Name = 'default version is not overridden'; Version = $null },
        @{ Name = 'explicit prerelease version'; Version = '2.4.0-rc1' },
        @{ Name = 'explicit stable version'; Version = '2.4.0' }
    )
    foreach ($case in $cases)
    {
        $arguments = @('msbuild', $projectPath, '-t:PublishSparkWorker', '-nologo', '-v:minimal')
        if ($null -ne $case.Version)
        {
            $arguments += "-p:Version=$($case.Version)"
        }
        $output = @(& dotnet @arguments 2>&1)
        if ($LASTEXITCODE -ne 0)
        {
            throw "MSBuild failed: $($output -join [Environment]::NewLine)"
        }
        $commands = @($output | Where-Object { "$_" -match 'WORKER_PUBLISH_ARGS:' })
        if ($commands.Count -ne 5)
        {
            throw "Expected five Worker publish commands, found $($commands.Count)."
        }
        foreach ($command in $commands)
        {
            $versionArguments = [regex]::Matches("$command", '(?:^|\s)/p:Version=(\S*)')
            if ($null -eq $case.Version)
            {
                if ($versionArguments.Count -ne 0)
                {
                    throw "An unset parent Version must not override the child default: $command"
                }
            }
            elseif (($versionArguments.Count -ne 1) -or
                ($versionArguments[0].Groups[1].Value -cne $case.Version))
            {
                throw "Expected exactly one /p:Version=$($case.Version): $command"
            }
            if ("$command" -notmatch '/p:OfficialBuildId=20260922\.1(?:\s|$)')
            {
                throw "OfficialBuildId was not preserved: $command"
            }
        }
        Write-Host "PASS $($case.Name): all five publish targets"
    }
    Write-Host 'Worker publish version tests: 3 passed.'
}
finally
{
    $resolvedTestDirectory = [IO.Path]::GetFullPath($testDirectory)
    if (($resolvedTestDirectory -ne [IO.Path]::GetFullPath((Join-Path $temporaryRoot $testDirectoryName))) -or
        (-not $testDirectoryName.StartsWith('spark-worker-version-', [StringComparison]::Ordinal)))
    {
        throw 'Refusing to remove an unexpected test directory.'
    }
    Remove-Item -LiteralPath $resolvedTestDirectory -Recurse -Force
}
