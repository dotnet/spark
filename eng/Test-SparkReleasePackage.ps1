# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

<#
.SYNOPSIS
Validates the selected Spark bridge in a Microsoft.Spark NuGet package without extracting it.
.OUTPUTS
A PSCustomObject with PackageVersion, SparkJarEntry and SparkJarSha256. The optional
SparkJarPath must identify the freshly built bridge included in this package.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$PackagePath,

    [string]$SparkJarPath,

    [ValidateSet('3.0', '3.1', '3.2', '3.3', '3.4', '3.5', '4.0')]
    [string]$SparkMajorMinorVersion = '4.0'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$bridgeName = 'microsoft-spark-' + $SparkMajorMinorVersion.Replace('.', '-')
$scalaVersion = if ($SparkMajorMinorVersion -eq '4.0') { '2.13' } else { '2.12' }
$bridgeArtifactId = "${bridgeName}_$scalaVersion"
$bridgeNamePattern = [regex]::Escape($bridgeName)
$bridgeArtifactPattern = [regex]::Escape($bridgeArtifactId)

function Read-ArchiveText
{
    param([IO.Compression.ZipArchiveEntry]$Entry)

    if ($Entry.Length -gt 1MB)
    {
        throw "Package metadata '$($Entry.FullName)' exceeds 1 MiB. Check the package contents."
    }

    $reader = [IO.StreamReader]::new($Entry.Open())
    try
    {
        return $reader.ReadToEnd()
    }
    finally
    {
        $reader.Dispose()
    }
}

$packageFile = Get-Item -LiteralPath $PackagePath
if ($packageFile.PSIsContainer -or ($packageFile.Extension -ine '.nupkg'))
{
    throw 'PackagePath must identify one .nupkg file, not a directory or symbol package.'
}

$package = [IO.Compression.ZipFile]::OpenRead($packageFile.FullName)
try
{
    $nuspecEntries = @($package.Entries | Where-Object { $_.FullName -imatch '\.nuspec$' })
    if (($nuspecEntries.Count -ne 1) -or ($nuspecEntries[0].FullName -match '[/\\]'))
    {
        throw 'The release package must contain exactly one root .nuspec file.'
    }

    $nuspec = [xml]::new()
    $nuspec.XmlResolver = $null
    $xmlSettings = [Xml.XmlReaderSettings]::new()
    $xmlSettings.DtdProcessing = [Xml.DtdProcessing]::Prohibit
    $xmlSettings.MaxCharactersInDocument = 1MB
    $textReader = [IO.StringReader]::new((Read-ArchiveText $nuspecEntries[0]))
    $xmlReader = [Xml.XmlReader]::Create($textReader, $xmlSettings)
    try
    {
        $nuspec.Load($xmlReader)
    }
    catch
    {
        throw 'The release package .nuspec is not valid XML, or contains a prohibited DTD.'
    }
    finally
    {
        $xmlReader.Dispose()
        $textReader.Dispose()
    }
    $packageIds = @($nuspec.SelectNodes("/*[local-name()='package']/*[local-name()='metadata']/*[local-name()='id']"))
    $packageVersions = @($nuspec.SelectNodes("/*[local-name()='package']/*[local-name()='metadata']/*[local-name()='version']"))
    if (($packageIds.Count -ne 1) -or ($packageIds[0].InnerText -cne 'Microsoft.Spark'))
    {
        throw 'The release gate requires the core Microsoft.Spark package, not an extension package.'
    }

    if (($packageVersions.Count -ne 1) -or [string]::IsNullOrWhiteSpace($packageVersions[0].InnerText))
    {
        throw 'The Microsoft.Spark .nuspec must contain exactly one nonempty package version.'
    }

    # Count all identities for the selected bridge, including misplaced or classifier JARs.
    # Accepting the first matching file could hide a stale second bridge in the package.
    $sparkJars = @($package.Entries | Where-Object {
        $_.FullName -imatch "(^|[/\\])$bridgeNamePattern[^/\\]*\.jar$"
    })
    if ($sparkJars.Count -ne 1)
    {
        throw "The Microsoft.Spark package must contain exactly one Spark $SparkMajorMinorVersion bridge JAR; found $($sparkJars.Count). Clean and rebuild the package."
    }

    $sparkJar = $sparkJars[0]
    $jarIdentity = [regex]::Match($sparkJar.FullName,
        "^jars/$bridgeArtifactPattern-(?<version>[0-9][A-Za-z0-9.+-]*)\.jar$")
    if ((-not $jarIdentity.Success) -or
        ($sparkJar.FullName -imatch '-(sources|javadoc|tests|test-sources)\.jar$'))
    {
        throw "Expected jars/$bridgeArtifactId-<version>.jar, without a sources, javadoc or test classifier."
    }

    if (($sparkJar.Length -eq 0) -or ($sparkJar.Length -gt 64MB))
    {
        throw "The Spark $SparkMajorMinorVersion bridge JAR is empty or exceeds the 64 MiB inspection limit. Check the package contents."
    }

    $jarContent = [IO.MemoryStream]::new()
    try
    {
        $entryStream = $sparkJar.Open()
        try
        {
            $entryStream.CopyTo($jarContent)
        }
        finally
        {
            $entryStream.Dispose()
        }

        $jarContent.Position = 0
        $hashAlgorithm = [Security.Cryptography.SHA256]::Create()
        try
        {
            $jarHash = [BitConverter]::ToString($hashAlgorithm.ComputeHash($jarContent)).Replace('-', '')
        }
        finally
        {
            $hashAlgorithm.Dispose()
        }

        if (-not [string]::IsNullOrWhiteSpace($SparkJarPath))
        {
            $freshJar = Get-Item -LiteralPath $SparkJarPath
            if ($freshJar.PSIsContainer -or ($sparkJar.FullName -cne "jars/$($freshJar.Name)"))
            {
                throw 'The packaged bridge filename does not match SparkJarPath. Pack the freshly built bridge.'
            }

            if ($jarHash -cne (Get-FileHash -LiteralPath $freshJar.FullName -Algorithm SHA256).Hash)
            {
                throw 'The packaged bridge SHA256 does not match SparkJarPath. Remove stale outputs and repack the freshly built bridge.'
            }
        }

        $jarContent.Position = 0
        try
        {
            $bridge = [IO.Compression.ZipArchive]::new($jarContent, [IO.Compression.ZipArchiveMode]::Read, $true)
        }
        catch
        {
            throw 'The packaged bridge is not a readable JAR archive. Clean and rebuild the JAR.'
        }
        try
        {
            foreach ($className in @(
                'org/apache/spark/deploy/dotnet/DotnetRunner.class',
                'org/apache/spark/api/dotnet/DotnetBackend.class',
                'org/apache/spark/sql/api/dotnet/SQLUtils.class'))
            {
                $classes = @($bridge.Entries | Where-Object { $_.FullName -ceq $className })
                if (($classes.Count -ne 1) -or ($classes[0].Length -eq 0))
                {
                    throw "The Spark $SparkMajorMinorVersion JAR must contain one compiled '$className'. It is not a runtime bridge JAR."
                }
            }

            $mavenEntries = @($bridge.Entries | Where-Object {
                $_.FullName -imatch "^META-INF/maven/com\.microsoft\.scala/$bridgeNamePattern[^/]*/pom\.properties$"
            })
            $expectedMetadata = "META-INF/maven/com.microsoft.scala/$bridgeArtifactId/pom.properties"
            if (($mavenEntries.Count -ne 1) -or ($mavenEntries[0].FullName -cne $expectedMetadata))
            {
                throw "The Spark $SparkMajorMinorVersion JAR must contain exactly one Maven identity for $bridgeArtifactId."
            }

            $properties = Read-ArchiveText $mavenEntries[0]
            $expectedProperties = @{
                artifactId = $bridgeArtifactId
                groupId = 'com.microsoft.scala'
                version = $jarIdentity.Groups['version'].Value
            }
            foreach ($name in $expectedProperties.Keys)
            {
                $values = [regex]::Matches($properties, "(?m)^$name=(?<value>[^\r\n]*)\r?$")
                if (($values.Count -ne 1) -or ($values[0].Groups['value'].Value -cne $expectedProperties[$name]))
                {
                    throw "The Spark $SparkMajorMinorVersion JAR Maven '$name' does not match its expected bridge identity. Clean and rebuild the JAR."
                }
            }
        }
        finally
        {
            $bridge.Dispose()
        }
    }
    finally
    {
        $jarContent.Dispose()
    }

    [PSCustomObject]@{
        PackageVersion = $packageVersions[0].InnerText
        SparkJarEntry = $sparkJar.FullName
        SparkJarSha256 = $jarHash
    }
}
finally
{
    $package.Dispose()
}
