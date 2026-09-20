# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

# Standalone tests: pwsh -File eng/tests/Test-SparkReleasePackage.Tests.ps1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$gateScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'Test-SparkReleasePackage.ps1'
$temporaryRoot = (Get-Item -LiteralPath ([IO.Path]::GetTempPath())).FullName
$testDirectoryName = 'spark-package-gate-' + [Guid]::NewGuid().ToString('N')
$testDirectory = Join-Path $temporaryRoot $testDirectoryName
New-Item -ItemType Directory -Path $testDirectory | Out-Null
$script:passed = 0

function Add-ArchiveEntry
{
    param([IO.Compression.ZipArchive]$Archive, [string]$Name, [byte[]]$Bytes)

    $stream = $Archive.CreateEntry($Name).Open()
    try
    {
        $stream.Write($Bytes, 0, $Bytes.Length)
    }
    finally
    {
        $stream.Dispose()
    }
}

function New-BridgeBytes
{
    param(
        [string]$ArtifactId = 'microsoft-spark-4-0_2.13',
        [string]$Version = '2.3.1',
        [switch]$MissingClass,
        [switch]$DuplicateClass
    )

    $memory = [IO.MemoryStream]::new()
    try
    {
        $archive = [IO.Compression.ZipArchive]::new($memory, [IO.Compression.ZipArchiveMode]::Create, $true)
        try
        {
            $classes = @(
                'org/apache/spark/deploy/dotnet/DotnetRunner.class',
                'org/apache/spark/api/dotnet/DotnetBackend.class',
                'org/apache/spark/sql/api/dotnet/SQLUtils.class'
            )
            foreach ($className in $classes)
            {
                if ($MissingClass -and ($className -eq $classes[0]))
                {
                    continue
                }
                Add-ArchiveEntry $archive $className ([byte[]]@(0xca, 0xfe, 0xba, 0xbe))
            }
            if ($DuplicateClass)
            {
                Add-ArchiveEntry $archive $classes[0] ([byte[]]@(0xca, 0xfe, 0xba, 0xbe))
            }
            $properties = "artifactId=$ArtifactId`ngroupId=com.microsoft.scala`nversion=$Version`n"
            Add-ArchiveEntry $archive "META-INF/maven/com.microsoft.scala/$ArtifactId/pom.properties" `
                ([Text.Encoding]::UTF8.GetBytes($properties))
        }
        finally
        {
            $archive.Dispose()
        }
        return ,$memory.ToArray()
    }
    finally
    {
        $memory.Dispose()
    }
}

function New-TestPackage
{
    param(
        [string[]]$JarNames = @('jars/microsoft-spark-4-0_2.13-2.3.1.jar'),
        [byte[]]$JarBytes = $validJar,
        [string]$PackageId = 'Microsoft.Spark',
        [string]$PackageVersion = '2.3.1-preview.1',
        [switch]$DuplicateNuspec,
        [switch]$MalformedNuspec,
        [switch]$DtdNuspec
    )

    $packagePath = Join-Path $testDirectory ([Guid]::NewGuid().ToString('N') + '.nupkg')
    $archive = [IO.Compression.ZipFile]::Open($packagePath, [IO.Compression.ZipArchiveMode]::Create)
    try
    {
        $nuspec = '<package xmlns="http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd"><metadata>' +
            "<id>$PackageId</id><version>$PackageVersion</version></metadata></package>"
        if ($MalformedNuspec)
        {
            $nuspec = '<package>'
        }
        if ($DtdNuspec)
        {
            $nuspec = '<!DOCTYPE package [<!ENTITY unused "value">]>' + $nuspec
        }
        Add-ArchiveEntry $archive 'Microsoft.Spark.nuspec' ([Text.Encoding]::UTF8.GetBytes($nuspec))
        if ($DuplicateNuspec)
        {
            Add-ArchiveEntry $archive 'Another.nuspec' ([Text.Encoding]::UTF8.GetBytes($nuspec))
        }
        foreach ($jarName in $JarNames)
        {
            Add-ArchiveEntry $archive $jarName $JarBytes
        }
    }
    finally
    {
        $archive.Dispose()
    }
    return $packagePath
}

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
    $validJar = New-BridgeBytes
    $packagePath = New-TestPackage
    $freshJar = Join-Path $testDirectory 'microsoft-spark-4-0_2.13-2.3.1.jar'
    [IO.File]::WriteAllBytes($freshJar, $validJar)
    $result = @(& $gateScript -PackagePath $packagePath -SparkJarPath $freshJar)
    if (($result.Count -ne 1) -or ($result[0].PackageVersion -cne '2.3.1-preview.1') -or
        ($result[0].SparkJarEntry -cne 'jars/microsoft-spark-4-0_2.13-2.3.1.jar') -or
        ($result[0].SparkJarSha256 -cne (Get-FileHash -LiteralPath $freshJar -Algorithm SHA256).Hash))
    {
        throw 'FAIL valid package: unexpected return value.'
    }
    $script:passed++
    Write-Host 'PASS valid package and fresh bridge identity'

    $resultWithoutFreshJar = & $gateScript -PackagePath $packagePath
    if ($resultWithoutFreshJar.SparkJarSha256 -cne $result[0].SparkJarSha256)
    {
        throw 'FAIL validation without SparkJarPath: unexpected hash.'
    }
    $script:passed++
    Write-Host 'PASS validation without optional fresh bridge'

    Assert-Rejected 'missing bridge' {
        & $gateScript -PackagePath (New-TestPackage -JarNames @())
    } 'exactly one Spark 4 bridge'
    Assert-Rejected 'duplicate bridge entry' {
        & $gateScript -PackagePath (New-TestPackage -JarNames @(
            'jars/microsoft-spark-4-0_2.13-2.3.1.jar', 'jars/microsoft-spark-4-0_2.13-2.3.1.jar'))
    } 'exactly one Spark 4 bridge'
    Assert-Rejected 'stale second version' {
        & $gateScript -PackagePath (New-TestPackage -JarNames @(
            'jars/microsoft-spark-4-0_2.13-2.3.1.jar', 'jars/microsoft-spark-4-0_2.13-2.3.0.jar'))
    } 'exactly one Spark 4 bridge'
    Assert-Rejected 'second Scala identity' {
        & $gateScript -PackagePath (New-TestPackage -JarNames @(
            'jars/microsoft-spark-4-0_2.13-2.3.1.jar', 'jars/microsoft-spark-4-0_2.12-2.3.1.jar'))
    } 'exactly one Spark 4 bridge'
    foreach ($invalidName in @(
        'jars/microsoft-spark-4-0_2.12-2.3.1.jar',
        'jars/microsoft-spark-4-0_2.13-2.3.1-sources.jar',
        'jars/microsoft-spark-4-0_2.13-2.3.1-javadoc.jar',
        'lib/microsoft-spark-4-0_2.13-2.3.1.jar',
        'jars\microsoft-spark-4-0_2.13-2.3.1.jar'))
    {
        Assert-Rejected "invalid identity $invalidName" {
            & $gateScript -PackagePath (New-TestPackage -JarNames @($invalidName))
        } 'Expected jars/microsoft-spark-4-0_2.13'
    }
    Assert-Rejected 'extension package' {
        & $gateScript -PackagePath (New-TestPackage -PackageId 'Microsoft.Spark.Extensions.Delta')
    } 'core Microsoft.Spark'
    Assert-Rejected 'nonexact package ID' {
        & $gateScript -PackagePath (New-TestPackage -PackageId 'microsoft.spark')
    } 'core Microsoft.Spark'
    Assert-Rejected 'empty package version' {
        & $gateScript -PackagePath (New-TestPackage -PackageVersion '')
    } 'nonempty package version'
    Assert-Rejected 'duplicate nuspec' {
        & $gateScript -PackagePath (New-TestPackage -DuplicateNuspec)
    } 'exactly one root .nuspec'
    Assert-Rejected 'malformed nuspec' {
        & $gateScript -PackagePath (New-TestPackage -MalformedNuspec)
    } 'XML'
    Assert-Rejected 'DTD in nuspec' {
        & $gateScript -PackagePath (New-TestPackage -DtdNuspec)
    } 'prohibited DTD'
    Assert-Rejected 'oversized metadata' {
        & $gateScript -PackagePath (New-TestPackage -PackageVersion ('0' * 1MB))
    } 'exceeds 1 MiB'
    Assert-Rejected 'not an executable bridge' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes (New-BridgeBytes -MissingClass))
    } 'not a runtime bridge'
    Assert-Rejected 'duplicate runtime class' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes (New-BridgeBytes -DuplicateClass))
    } 'not a runtime bridge'
    Assert-Rejected 'renamed Scala 2.12 bridge' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes (New-BridgeBytes -ArtifactId 'microsoft-spark-4-0_2.12'))
    } 'Maven identity'
    Assert-Rejected 'renamed bridge version' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes (New-BridgeBytes -Version '2.3.0'))
    } "Maven 'version'"
    Assert-Rejected 'empty bridge' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes ([byte[]]::new(0)))
    } 'empty or exceeds'
    Assert-Rejected 'oversized bridge' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes ([byte[]]::new(64MB + 1)))
    } '64 MiB inspection limit'
    Assert-Rejected 'invalid bridge archive' {
        & $gateScript -PackagePath (New-TestPackage -JarBytes ([byte[]]@(1, 2, 3)))
    } 'not a readable JAR archive'

    $otherJar = Join-Path $testDirectory 'microsoft-spark-4-0_2.13-2.3.0.jar'
    [IO.File]::WriteAllBytes($otherJar, $validJar)
    Assert-Rejected 'wrong fresh filename' {
        & $gateScript -PackagePath $packagePath -SparkJarPath $otherJar
    } 'filename does not match'
    [IO.File]::WriteAllBytes($freshJar, ([byte[]]@(1, 2, 3)))
    Assert-Rejected 'stale packaged bridge hash' {
        & $gateScript -PackagePath $packagePath -SparkJarPath $freshJar
    } 'SHA256 does not match'
    Assert-Rejected 'package directory' {
        & $gateScript -PackagePath $testDirectory
    } 'one .nupkg file'

    Write-Host "Package gate tests: $script:passed passed."
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
