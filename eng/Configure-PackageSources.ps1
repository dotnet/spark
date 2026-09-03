# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$FeedName,

    [Parameter(Mandatory = $true)]
    [ValidateNotNull()]
    [uri]$FeedUrl,

    [ValidateNotNullOrEmpty()]
    [string]$MavenSettingsPath = (Join-Path `
        ([Environment]::GetFolderPath('UserProfile')) '.m2/settings.xml'),

    [string]$IvySettingsPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($FeedUrl.Scheme -ne [Uri]::UriSchemeHttps)
{
    throw "Package feed must use HTTPS: '$FeedUrl'."
}

if (-not (Test-Path -LiteralPath $MavenSettingsPath -PathType Leaf))
{
    throw "Maven settings file '$MavenSettingsPath' does not exist. Run MavenAuthenticate first."
}

[xml]$mavenSettings = Get-Content -Raw -LiteralPath $MavenSettingsPath
$settingsRoot = $mavenSettings.DocumentElement
if (($null -eq $settingsRoot) -or ($settingsRoot.LocalName -ne 'settings'))
{
    throw "Maven settings file '$MavenSettingsPath' has an invalid root element."
}

$servers = $settingsRoot.SelectSingleNode("*[local-name()='servers']")
if ($null -eq $servers)
{
    throw "MavenAuthenticate did not add a servers element to '$MavenSettingsPath'."
}

$server = $servers.ChildNodes |
    Where-Object {
        $id = $_.SelectSingleNode("*[local-name()='id']")
        ($_.LocalName -eq 'server') -and
        ($null -ne $id) -and
        ($id.InnerText -eq $FeedName)
    } |
    Select-Object -First 1

if ($null -eq $server)
{
    throw "MavenAuthenticate did not add credentials for feed '$FeedName'."
}

$usernameElement = $server.SelectSingleNode("*[local-name()='username']")
$passwordElement = $server.SelectSingleNode("*[local-name()='password']")
if (($null -eq $usernameElement) -or ($null -eq $passwordElement))
{
    throw "Maven credentials for feed '$FeedName' are incomplete."
}

$username = $usernameElement.InnerText
$password = $passwordElement.InnerText
if ([string]::IsNullOrWhiteSpace($username) -or [string]::IsNullOrWhiteSpace($password))
{
    throw "Maven credentials for feed '$FeedName' are incomplete."
}

$namespace = $settingsRoot.NamespaceURI
$mirrors = $settingsRoot.SelectSingleNode("*[local-name()='mirrors']")
if ($null -eq $mirrors)
{
    $mirrors = $mavenSettings.CreateElement('mirrors', $namespace)
    $settingsRoot.InsertAfter($mirrors, $servers) | Out-Null
}
else
{
    $mirrors.RemoveAll()
}

$mirror = $mavenSettings.CreateElement('mirror', $namespace)
foreach ($property in ([ordered]@{
    id = $FeedName
    name = 'ManagedOSS authenticated Maven mirror'
    url = $FeedUrl.AbsoluteUri.TrimEnd('/')
    mirrorOf = '*'
}).GetEnumerator())
{
    $element = $mavenSettings.CreateElement($property.Key, $namespace)
    $element.InnerText = $property.Value
    $mirror.AppendChild($element) | Out-Null
}
$mirrors.AppendChild($mirror) | Out-Null
$mavenSettings.Save($MavenSettingsPath)

if ([string]::IsNullOrWhiteSpace($IvySettingsPath))
{
    Write-Host "Configured Maven to use authenticated feed '$FeedName'."
    return
}

$ivyDirectory = Split-Path -Parent $IvySettingsPath
if (-not (Test-Path -LiteralPath $ivyDirectory -PathType Container))
{
    throw "Ivy settings directory '$ivyDirectory' does not exist."
}

$ivySettings = [Xml.XmlDocument]::new()
$ivyRoot = $ivySettings.CreateElement('ivysettings')
$ivySettings.AppendChild($ivyRoot) | Out-Null

$realmResponse = Invoke-WebRequest `
    -Uri $FeedUrl.AbsoluteUri `
    -Method Head `
    -SkipHttpErrorCheck
$credentialRealm = $null
foreach ($challenge in @($realmResponse.Headers['WWW-Authenticate']))
{
    $realmMatch = [regex]::Match(
        [string]$challenge,
        'Basic\s+realm="(?<realm>[^"]+)"',
        [Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($realmMatch.Success)
    {
        $credentialRealm = $realmMatch.Groups['realm'].Value
        break
    }
}
if ([string]::IsNullOrWhiteSpace($credentialRealm))
{
    throw "Package feed '$FeedUrl' did not advertise a Basic authentication realm."
}

$defaultSettings = $ivySettings.CreateElement('settings')
$defaultSettings.SetAttribute('defaultResolver', $FeedName)
$ivyRoot.AppendChild($defaultSettings) | Out-Null

$credentials = $ivySettings.CreateElement('credentials')
$credentials.SetAttribute('realm', $credentialRealm)
$credentials.SetAttribute('host', $FeedUrl.Host)
$credentials.SetAttribute('username', $username)
$credentials.SetAttribute('passwd', $password)
$ivyRoot.AppendChild($credentials) | Out-Null

$resolvers = $ivySettings.CreateElement('resolvers')
$resolver = $ivySettings.CreateElement('ibiblio')
$resolver.SetAttribute('name', $FeedName)
$resolver.SetAttribute('root', $FeedUrl.AbsoluteUri.TrimEnd('/'))
$resolver.SetAttribute('m2compatible', 'true')
$resolver.SetAttribute('usepoms', 'true')
$resolvers.AppendChild($resolver) | Out-Null
$ivyRoot.AppendChild($resolvers) | Out-Null
$ivySettings.Save($IvySettingsPath)

if (-not $IsWindows)
{
    chmod 600 $IvySettingsPath
    if ($LASTEXITCODE -ne 0)
    {
        throw "Unable to restrict permissions on '$IvySettingsPath'."
    }
}

Write-Host "Configured Maven and Ivy to use authenticated feed '$FeedName'."
